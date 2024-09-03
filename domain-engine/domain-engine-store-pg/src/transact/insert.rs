use std::{collections::hash_map::Entry, ops::Deref};

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def},
    query::select::Select,
    value::Value,
    DefId, PackageId, PropId,
};
use pin_utils::pin_mut;
use tokio_postgres::{Row, ToStatement};
use tracing::{debug, trace, warn};

use crate::{
    pg_error::{map_row_error, PgError, PgInputError},
    pg_model::{EdgeId, InDomain, PgDataKey, PgDomainTable, PgRepr},
    sql::{self},
    sql_record::SqlColumnStream,
    sql_value::SqlScalar,
    statement::{Prepare, PreparedStatement},
    transact::query::IncludeJoinedAttrs,
};

use super::{
    cache::PgCache,
    data::{Data, RowValue},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    query::{QueryBuildCtx, QuerySelect},
    InsertMode, MutationMode, TransactCtx,
};

#[derive(Clone)]
pub struct PreparedInsert {
    inherent_stmt: PreparedStatement,
    edge_select_stmt: Option<PreparedStatement>,
}

struct AnalyzedInput<'a> {
    pub sql_params: Vec<SqlScalar>,
    pub update_tentative: Option<PgDataKey>,
    pub edges: EdgePatches,
    pub abstract_values: Vec<(PropId, Value)>,
    pub query_select: QuerySelect<'a>,
}

struct ParentProp {
    prop_id: PropId,
    key: PgDataKey,
}

impl<'a> TransactCtx<'a> {
    /// Returns BoxFuture because of potential recursion
    pub fn insert_vertex<'s>(
        &'s self,
        value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
        cache: &'s mut PgCache,
    ) -> BoxFuture<'_, DomainResult<RowValue>> {
        Box::pin(async move {
            let _def_id = value.value.type_def_id();
            self.insert_vertex_impl(value, mode, select, cache)
                // .instrument(debug_span!("ins", id = ?def_id))
                .await
        })
    }

    async fn insert_vertex_impl<'s>(
        &'s self,
        mut value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
        cache: &'s mut PgCache,
    ) -> DomainResult<RowValue> {
        let pkg_id = value.pkg_id;
        let def_id = value.value.type_def_id();

        let prepared = if let Some(prepared) = cache.insert.get(&(pkg_id, def_id)).cloned() {
            prepared
        } else {
            let prepared = self.prepare_insert(pkg_id, def_id, select, cache).await?;
            cache.insert.insert((pkg_id, def_id), prepared.clone());
            prepared
        };

        self.preprocess_insert_value(mode, &mut value.value)?;

        let def = self.ontology.def(value.value.type_def_id());
        let (analyzed, pg) = self.analyze_input(None, def, value, select, cache)?;

        let mut row_value = if let Some(data_key) = analyzed.update_tentative {
            self.update_tentative_vertex(def, pg, &analyzed, data_key)
                .await?
        } else {
            self.insert_row(
                &prepared.inherent_stmt,
                &analyzed.sql_params,
                analyzed.query_select,
            )
            .await?
        };

        self.patch_edges(pg.table, row_value.data_key, analyzed.edges, cache)
            .await?;

        for (prop_id, value) in analyzed.abstract_values {
            self.insert_abstract_sub_value(
                ParentProp {
                    prop_id,
                    key: row_value.data_key,
                },
                value,
                cache,
            )
            .await?;
        }

        if let (Some(edge_select_stmt), QuerySelect::Struct(properties)) =
            (prepared.edge_select_stmt, analyzed.query_select)
        {
            debug!("{edge_select_stmt}");

            let row = self
                .client()
                .query_one(
                    edge_select_stmt.deref(),
                    &[&SqlScalar::I64(row_value.data_key)],
                )
                .await
                .map_err(PgError::InsertEdgeFetch)?;

            let mut column_stream = SqlColumnStream::new(&row);

            let Value::Struct(attrs, _) = &mut row_value.value else {
                unreachable!();
            };

            self.read_edge_attributes(&mut column_stream, def, properties, attrs.as_mut())?;
        }

        Ok(row_value)
    }

    /// Instead of doing an INSERT, do an UPDATE against a vertex with a "tentative key".
    /// A tentative vertex has been inserted into the database upon seing a foreign key referencing that vertice.
    /// It has to exist in the DB because its _data key_ need to exist.
    /// At the end of the transaction, all the tentative keys will be checked to have been updated with proper vertex data.
    async fn update_tentative_vertex(
        &self,
        def: &Def,
        pg: PgDomainTable<'a>,
        analyzed: &AnalyzedInput<'a>,
        data_key: PgDataKey,
    ) -> DomainResult<RowValue> {
        let mut sql_update = sql::Update {
            with: None,
            table_name: pg.table_name(),
            set: vec![],
            where_: Some(sql::Expr::eq(
                sql::Expr::path1("_key"),
                sql::Expr::param(analyzed.sql_params.len()),
            )),
            returning: vec![sql::Expr::LiteralInt(0)],
        };

        let mut param_idx = 0;

        let mut attrs: FnvHashMap<PropId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        for (prop_id, rel_info) in &def.data_relationships {
            if let Some(pg_column) = pg.table.find_column(prop_id) {
                let input_param = analyzed.sql_params.get(param_idx).unwrap();

                let unit_val =
                    self.deserialize_sql(rel_info.target.def_id(), input_param.clone().into())?;
                attrs.insert(*prop_id, Attr::Unit(unit_val));

                sql_update.set.push(sql::UpdateColumn(
                    &pg_column.col_name,
                    sql::Expr::param(param_idx),
                ));
                param_idx += 1;
            }
        }

        assert_eq!(param_idx, analyzed.sql_params.len());

        let mut update_params = analyzed.sql_params.clone();
        update_params.push(SqlScalar::I64(data_key));

        let sql = sql_update.to_string();

        debug!("{sql}");

        self.query_one_raw(&sql, &update_params).await?;

        Ok(RowValue {
            value: Value::Struct(Box::new(attrs), def.id.into()),
            def_key: pg.table.key,
            data_key,
            op: DataOperation::Inserted,
        })
    }

    fn insert_abstract_sub_value<'s>(
        &'s self,
        parent: ParentProp,
        value: Value,
        cache: &'s mut PgCache,
    ) -> BoxFuture<'_, DomainResult<()>> {
        Box::pin(self.insert_abstract_sub_value_impl(parent, value, cache))
    }

    async fn insert_abstract_sub_value_impl(
        &self,
        parent: ParentProp,
        value: Value,
        cache: &mut PgCache,
    ) -> DomainResult<()> {
        let value_def_id = value.type_def_id();
        let def = self.ontology.def(value_def_id);

        if let Some(PgRepr::Scalar(_pg_type, ontol_def_tag)) =
            PgRepr::classify_opt_def_repr(def.repr(), self.ontology)
        {
            let pkg_id = parent.prop_id.0.package_id();
            let def_id = ontol_def_tag.def_id();

            let prepared = if let Some(prepared) = cache.insert.get(&(pkg_id, def_id)).cloned() {
                prepared
            } else {
                let prepared = self
                    .prepare_insert(pkg_id, def_id, &Select::Unit, cache)
                    .await?;
                cache.insert.insert((pkg_id, def_id), prepared.clone());
                prepared
            };

            let parent_prop_key = self
                .pg_model
                .datatable(parent.prop_id.0.package_id(), parent.prop_id.0)?
                .abstract_property(&parent.prop_id)?;

            let Data::Sql(value) = self.data_from_value(value)? else {
                panic!("value must be a scalar here");
            };

            let sql_params = vec![
                SqlScalar::I32(parent_prop_key),
                SqlScalar::I64(parent.key),
                value,
            ];

            self.insert_row(&prepared.inherent_stmt, &sql_params, QuerySelect::Unit)
                .await?;

            Ok(())
        } else {
            let pkg_id = value_def_id.0;
            let def_id = value_def_id;

            let prepared = if let Some(prepared) = cache.insert.get(&(pkg_id, def_id)).cloned() {
                prepared
            } else {
                let prepared = self
                    .prepare_insert(pkg_id, def_id, &Select::Unit, cache)
                    .await?;
                cache.insert.insert((pkg_id, def_id), prepared.clone());
                prepared
            };

            let def = self.ontology.def(def_id);
            let (analyzed, pg) = self.analyze_input(
                Some(parent),
                def,
                InDomain { pkg_id, value },
                &Select::Unit,
                cache,
            )?;

            let row_value = self
                .insert_row(
                    &prepared.inherent_stmt,
                    &analyzed.sql_params,
                    analyzed.query_select,
                )
                .await?;

            self.patch_edges(pg.table, row_value.data_key, analyzed.edges, cache)
                .await?;

            for (prop_id, value) in analyzed.abstract_values {
                self.insert_abstract_sub_value(
                    ParentProp {
                        prop_id,
                        key: row_value.data_key,
                    },
                    value,
                    cache,
                )
                .await?;
            }

            Ok(())
        }
    }

    fn preprocess_insert_value(&self, mode: InsertMode, value: &mut Value) -> DomainResult<()> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        if let Value::Struct(map, _) = value {
            if let Entry::Vacant(vacant) = map.entry(entity.id_prop) {
                match mode {
                    InsertMode::Insert => {
                        let value_generator = entity
                            .id_value_generator
                            .ok_or(PgInputError::MissingValueWithoutGenerator)?;

                        let (generated_id, _container) = try_generate_entity_id(
                            entity.id_operator_addr,
                            value_generator,
                            self.ontology,
                            self.system,
                        )?;
                        if let GeneratedId::Generated(value) = generated_id {
                            vacant.insert(Attr::Unit(value));
                        }
                    }
                    InsertMode::Upsert => {
                        return Err(DomainErrorKind::InherentIdNotFound.into_error())
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn prepare_insert(
        &self,
        pkg_id: PackageId,
        def_id: DefId,
        select: &'a Select,
        cache: &mut PgCache,
    ) -> DomainResult<PreparedInsert> {
        let def = self.ontology.def(def_id);
        let pg = self.pg_model.pg_domain_datatable(pkg_id, def_id)?;

        let mut query_ctx = QueryBuildCtx::default();
        let root_alias = query_ctx.alias;
        query_ctx.with_def_aliases.insert(def_id, root_alias);

        let mut edge_select_stmt: Option<PreparedStatement> = None;

        let mut column_names = vec!["_key"];
        let mut values = vec![sql::Expr::Default];
        let mut insert_returning = vec![];

        let mut param_idx = 0;

        if pg.table.has_fkey {
            column_names.extend(["_fprop", "_fkey"]);
            values.extend([sql::Expr::param(0), sql::Expr::param(1)]);
            param_idx += 2;
        }

        match PgRepr::classify_opt_def_repr(def.repr(), self.ontology) {
            Some(PgRepr::Scalar(_, _)) => {
                column_names.push("value");
                values.push(sql::Expr::param(param_idx));
            }
            _ => {
                // insert columns in the order of data relationships
                for (prop_id, rel_info) in &def.data_relationships {
                    if matches!(
                        &rel_info.kind,
                        DataRelationshipKind::Id | DataRelationshipKind::Tree
                    ) {
                        if let Some(pg_column) = pg.table.find_column(prop_id) {
                            column_names.push(pg_column.col_name.as_ref());
                            if pg_column.pg_type.insert_default() {
                                values.push(sql::Expr::Default);
                            } else {
                                values.push(sql::Expr::param(param_idx));
                                param_idx += 1;
                            }
                        }
                    }
                }
            }
        }

        // RETURNING is based on select, etc
        match select {
            Select::EntityId => {
                let entity = def.entity().ok_or_else(|| {
                    warn!("not an entity");
                    DomainErrorKind::NotAnEntity(def_id).into_error()
                })?;

                if let Some(field) = pg.table.find_column(&entity.id_prop) {
                    insert_returning.extend(self.initial_standard_data_fields(pg));
                    insert_returning.push(sql::Expr::path1(field.col_name.as_ref()));
                }
            }
            Select::Struct(sel) => {
                insert_returning.extend(self.initial_standard_data_fields(pg));
                let select_stats =
                    self.select_inherent_struct_fields(def, pg.table, &mut insert_returning, None)?;

                if select_stats.edge_count > 0 {
                    let mut ctx = QueryBuildCtx::default();
                    let root_alias = ctx.alias;
                    let mut expressions = sql::Expressions {
                        items: vec![],
                        multiline: true,
                    };

                    self.sql_select_edge_properties(
                        (def, pg),
                        root_alias,
                        &sel.properties,
                        &mut ctx,
                        &mut expressions.items,
                        cache,
                    )?;

                    if !expressions.items.is_empty() {
                        edge_select_stmt = Some(
                            sql::Select {
                                with: ctx.with(),
                                expressions,
                                from: vec![sql::FromItem::TableNameAs(
                                    pg.table_name(),
                                    sql::Name::Alias(root_alias),
                                )],
                                where_: Some(sql::Expr::eq(
                                    sql::Expr::path1("_key"),
                                    sql::Expr::param(0),
                                )),
                                limit: sql::Limit {
                                    limit: Some(1),
                                    offset: None,
                                },
                                ..Default::default()
                            }
                            .to_string()
                            .prepare(self.client())
                            .await?,
                        );
                    }
                }
            }
            Select::Unit => {
                insert_returning.extend(self.initial_standard_data_fields(pg));
            }
            select => {
                todo!("{select:?}")
            }
        };

        let insert = sql::Insert {
            with: query_ctx.with(),
            into: pg.table_name(),
            as_: None,
            column_names,
            values,
            on_conflict: None,
            returning: insert_returning,
        };

        Ok(PreparedInsert {
            inherent_stmt: insert.to_string().prepare(self.client()).await?,
            edge_select_stmt,
        })
    }

    fn analyze_input(
        &self,
        parent: Option<ParentProp>,
        def: &Def,
        value: InDomain<Value>,
        select: &'a Select,
        cache: &mut PgCache,
    ) -> DomainResult<(AnalyzedInput<'a>, PgDomainTable<'_>)> {
        let pg = self
            .pg_model
            .pg_domain_datatable(value.pkg_id, value.type_def_id())?;
        let pg_table = pg.table;

        let Value::Struct(mut attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut sql_params: Vec<SqlScalar> = vec![];
        let mut update_tentative: Option<PgDataKey> = None;
        let mut edge_patches = EdgePatches::default();
        let mut abstract_values: Vec<(PropId, Value)> = vec![];

        if pg_table.has_fkey {
            let Some(parent) = parent else {
                panic!("missing parent property for fkey");
            };

            let parent_prop_key = self
                .pg_model
                .datatable(parent.prop_id.0.package_id(), parent.prop_id.0)?
                .abstract_property(&parent.prop_id)?;

            sql_params.extend([SqlScalar::I32(parent_prop_key), SqlScalar::I64(parent.key)]);
        }

        for (prop_id, rel_info) in &def.data_relationships {
            let attr = attrs.remove(prop_id);

            match (rel_info.kind, pg_table.find_column(prop_id)) {
                (DataRelationshipKind::Edge(proj), _) => {
                    let patch = edge_patches.patch(EdgeId(proj.edge_id), proj.subject);

                    match attr {
                        Some(Attr::Unit(value)) => {
                            if patch.tuples.is_empty() {
                                patch.tuples.push(EdgeEndoTuplePatch { elements: vec![] });
                            }
                            patch.tuples[0].insert_element(
                                proj.object,
                                value,
                                MutationMode::insert(),
                            )?;
                        }
                        Some(Attr::Tuple(tuple)) => {
                            patch.tuples.push(EdgeEndoTuplePatch::from_tuple(
                                tuple
                                    .elements
                                    .into_iter()
                                    .map(|val| (val, MutationMode::insert())),
                            ));
                        }
                        Some(Attr::Matrix(matrix)) => {
                            patch.tuples.extend(matrix.into_rows().map(|tuple| {
                                EdgeEndoTuplePatch::from_tuple(
                                    tuple
                                        .elements
                                        .into_iter()
                                        .map(|val| (val, MutationMode::insert())),
                                )
                            }))
                        }
                        None => {}
                    }
                }
                (kind, Some(pg_column)) => {
                    if pg_column.pg_type.insert_default() {
                        continue;
                    }

                    match attr {
                        Some(Attr::Unit(value)) => {
                            let type_def_id = value.type_def_id();

                            match self.data_from_value(value)? {
                                Data::Sql(scalar) => {
                                    // register inserted/known ID
                                    if matches!(kind, DataRelationshipKind::Id) {
                                        if let Some(tentative_foreign_ids) = cache
                                            .tentative_foreign_keys
                                            .get_mut(&(*prop_id, type_def_id))
                                        {
                                            if let Some((_, data_key)) =
                                                tentative_foreign_ids.remove(&scalar)
                                            {
                                                trace!("pop tentative foreign key: {scalar:?}");
                                                update_tentative = Some(data_key);
                                            }
                                        }
                                    }

                                    sql_params.push(scalar);
                                }
                                Data::Compound(comp) => {
                                    todo!("compound: {comp:?}");
                                }
                            }
                        }
                        None => {
                            sql_params.push(SqlScalar::Null);
                        }
                        Some(_) => {
                            debug!("edge ignored");
                        }
                    }
                }
                _ => {
                    match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => {
                            if matches!(PgRepr::classify(*def_id, self.ontology), PgRepr::Unit) {
                                continue;
                            }
                        }
                        DataRelationshipTarget::Union(_) => {}
                    };

                    match attr {
                        Some(Attr::Unit(value)) => {
                            abstract_values.push((*prop_id, value));
                        }
                        Some(Attr::Tuple(_tuple)) => {
                            return Err(PgInputError::MultivaluedSubValue(*prop_id).into());
                        }
                        Some(Attr::Matrix(matrix)) => {
                            if matrix.columns.len() == 1 {
                                for row in matrix.into_rows() {
                                    let value = row.elements.into_iter().next().unwrap();
                                    abstract_values.push((*prop_id, value));
                                }
                            } else {
                                return Err(PgInputError::MultivaluedSubValue(*prop_id).into());
                            }
                        }
                        None => {}
                    }
                }
            }
        }

        let query_select = match select {
            Select::EntityId => {
                let entity = def.entity().ok_or_else(|| {
                    warn!("not an entity");
                    DomainErrorKind::NotAnEntity(def.id).into_error()
                })?;

                QuerySelect::Field(entity.id_prop)
            }
            Select::Struct(sel) => QuerySelect::Struct(&sel.properties),
            Select::Unit => QuerySelect::Unit,
            select => todo!("{select:?}"),
        };

        Ok((
            AnalyzedInput {
                sql_params,
                update_tentative,
                edges: edge_patches,
                abstract_values,
                query_select,
            },
            pg,
        ))
    }

    async fn insert_row(
        &self,
        stmt: &PreparedStatement,
        sql_params: &[SqlScalar],
        query_select: QuerySelect<'a>,
    ) -> DomainResult<RowValue> {
        debug!("{}", stmt);
        let row = self.query_one_raw(stmt.deref(), sql_params).await?;
        self.read_row_value_as_vertex(
            SqlColumnStream::new(&row),
            Some(query_select),
            IncludeJoinedAttrs::No,
            DataOperation::Inserted,
        )
    }

    async fn query_one_raw<S: ToStatement>(
        &self,
        stmt: &S,
        sql_params: &[SqlScalar],
    ) -> DomainResult<Row> {
        trace!("{sql_params:?}");

        let stream = self
            .client()
            .query_raw(stmt, sql_params)
            .await
            .map_err(PgError::InsertQuery)?;
        pin_mut!(stream);

        let row = stream
            .try_next()
            .await
            .map_err(map_row_error)?
            .ok_or(PgError::NothingInserted)?;

        stream
            .try_next()
            .await
            .map_err(PgError::InsertRowStreamNotClosed)?;

        match stream.rows_affected() {
            Some(affected) => {
                if affected != 1 {
                    return Err(PgError::InsertIncorrectAffectCount.into());
                }
            }
            None => {
                return Err(PgError::InsertNoRowsAffected.into());
            }
        }

        Ok(row)
    }
}

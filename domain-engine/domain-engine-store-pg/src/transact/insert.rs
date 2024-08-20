use std::{collections::hash_map::Entry, ops::Deref};

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def, DefRepr},
    query::select::Select,
    value::Value,
    DefId, PackageId, PropId,
};
use pin_utils::pin_mut;
use tracing::{debug, warn};

use crate::{
    pg_error::{map_row_error, PgError, PgInputError},
    pg_model::{InDomain, PgDataKey, PgTable},
    sql::{self},
    sql_record::SqlColumnStream,
    sql_value::SqlVal,
    statement::{Prepare, PreparedStatement},
    transact::query::IncludeEdgeAttrs,
};

use super::{
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

struct AnalyzedInput<'a, 'b> {
    pub field_buf: Vec<SqlVal<'b>>,
    pub edges: EdgePatches,
    pub sub_values: Vec<(PropId, Value)>,
    pub query_select: QuerySelect<'a>,
}

impl<'a> TransactCtx<'a> {
    /// Returns BoxFuture because of potential recursion
    pub fn insert_vertex(
        &self,
        value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
    ) -> BoxFuture<'_, DomainResult<RowValue>> {
        Box::pin(async move {
            let _def_id = value.value.type_def_id();
            self.insert_vertex_impl(value, mode, select)
                // .instrument(debug_span!("ins", id = ?def_id))
                .await
        })
    }

    async fn insert_vertex_impl(
        &self,
        mut value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
    ) -> DomainResult<RowValue> {
        let pkg_id = value.pkg_id;
        let def_id = value.value.type_def_id();

        let prepared = if let Some(prepared) =
            self.stmt_cache_locked(|c| c.insert.get(&(pkg_id, def_id)).cloned())
        {
            prepared
        } else {
            let prepared = self.prepare_insert(pkg_id, def_id, select).await?;
            self.stmt_cache_locked(|c| c.insert.insert((pkg_id, def_id), prepared.clone()));
            prepared
        };

        self.preprocess_insert_value(mode, &mut value.value)?;

        let def = self.ontology.def(value.value.type_def_id());
        let (analyzed, pg_table) = self.analyze_input(None, def, value, select)?;

        let mut row_value = self
            .insert_row(
                &prepared.inherent_stmt,
                &analyzed.field_buf,
                analyzed.query_select,
            )
            .await?;

        self.patch_edges(pg_table, row_value.data_key, analyzed.edges)
            .await?;

        for (prop_id, value) in analyzed.sub_values {
            self.insert_sub_value((prop_id, row_value.data_key), value)
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
                    &[&SqlVal::I64(row_value.data_key)],
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

    fn insert_sub_value(
        &self,
        parent: (PropId, PgDataKey),
        value: Value,
    ) -> BoxFuture<'_, DomainResult<()>> {
        Box::pin(self.insert_sub_value_impl(parent, value))
    }

    async fn insert_sub_value_impl(
        &self,
        parent: (PropId, PgDataKey),
        value: Value,
    ) -> DomainResult<()> {
        let pkg_id = value.type_def_id().0;
        let def_id = value.type_def_id();

        let prepared = if let Some(prepared) =
            self.stmt_cache_locked(|c| c.insert.get(&(pkg_id, def_id)).cloned())
        {
            prepared
        } else {
            let prepared = self.prepare_insert(pkg_id, def_id, &Select::Unit).await?;
            self.stmt_cache_locked(|c| c.insert.insert((pkg_id, def_id), prepared.clone()));
            prepared
        };

        let def = self.ontology.def(def_id);
        let (analyzed, pg_table) =
            self.analyze_input(Some(parent), def, InDomain { pkg_id, value }, &Select::Unit)?;

        let row_value = self
            .insert_row(
                &prepared.inherent_stmt,
                &analyzed.field_buf,
                analyzed.query_select,
            )
            .await?;

        self.patch_edges(pg_table, row_value.data_key, analyzed.edges)
            .await?;

        for (prop_id, value) in analyzed.sub_values {
            self.insert_sub_value((prop_id, row_value.data_key), value)
                .await?;
        }

        Ok(())
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
    ) -> DomainResult<PreparedInsert> {
        let def = self.ontology.def(def_id);
        let pg = self.pg_model.pg_domain_datatable(pkg_id, def_id)?;

        let mut ctx = QueryBuildCtx::default();
        let root_alias = ctx.alias;
        ctx.with_def_aliases.insert(def_id, root_alias);

        let mut edge_select_stmt: Option<PreparedStatement> = None;

        let mut column_names = vec!["_key"];
        let mut values = vec![sql::Expr::Default];
        let mut insert_returning = vec![];

        let mut param_idx = 0;

        if pg.table.has_fkey {
            column_names.extend(["_fdef", "_fkey"]);
            values.extend([sql::Expr::param(0), sql::Expr::param(1)]);
            param_idx += 2;
        }

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
                        def,
                        root_alias,
                        &sel.properties,
                        pg,
                        &mut ctx,
                        &mut expressions.items,
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
            with: ctx.with(),
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
        parent: Option<(PropId, PgDataKey)>,
        def: &Def,
        value: InDomain<Value>,
        select: &'a Select,
    ) -> DomainResult<(AnalyzedInput<'a, '_>, &PgTable)> {
        let pg_table = self.pg_model.datatable(value.pkg_id, value.type_def_id())?;

        let Value::Struct(mut attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut field_buf: Vec<SqlVal> = vec![];
        let mut edge_patches = EdgePatches::default();
        let mut sub_values: Vec<(PropId, Value)> = vec![];

        if pg_table.has_fkey {
            let Some((parent_prop_id, parent_key)) = parent else {
                panic!();
            };

            let parent_pg_table = self
                .pg_model
                .datatable(parent_prop_id.0.package_id(), parent_prop_id.0)?;

            field_buf.extend([SqlVal::I32(parent_pg_table.key), SqlVal::I64(parent_key)]);
        }

        for (prop_id, rel_info) in &def.data_relationships {
            let attr = attrs.remove(prop_id);

            match (rel_info.kind, pg_table.find_column(prop_id)) {
                (DataRelationshipKind::Edge(proj), _) => {
                    let patch = edge_patches.patch(proj.id, proj.subject);

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
                (_, Some(pg_column)) => {
                    if pg_column.pg_type.insert_default() {
                        continue;
                    }

                    match attr {
                        Some(Attr::Unit(value)) => match self.data_from_value(value)? {
                            Data::Sql(scalar) => {
                                field_buf.push(scalar);
                            }
                            Data::Compound(comp) => {
                                todo!("compound: {comp:?}");
                            }
                        },
                        None => {
                            field_buf.push(SqlVal::Null);
                        }
                        Some(_) => {
                            debug!("edge ignored");
                        }
                    }
                }
                _ => {
                    match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => {
                            match self.ontology.def(*def_id).repr() {
                                Some(DefRepr::Unit) | None => continue,
                                _ => {}
                            }
                        }
                        DataRelationshipTarget::Union(_) => {}
                    };

                    match attr {
                        Some(Attr::Unit(value)) => {
                            sub_values.push((*prop_id, value));
                        }
                        Some(Attr::Tuple(_tuple)) => {
                            return Err(PgInputError::MultivaluedSubValue(*prop_id).into());
                        }
                        Some(Attr::Matrix(matrix)) => {
                            if matrix.columns.len() == 1 {
                                for row in matrix.into_rows() {
                                    let value = row.elements.into_iter().next().unwrap();
                                    sub_values.push((*prop_id, value));
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
                field_buf,
                edges: edge_patches,
                sub_values,
                query_select,
            },
            pg_table,
        ))
    }

    async fn insert_row(
        &self,
        prepared_inherent_insert: &PreparedStatement,
        field_buf: &[SqlVal<'a>],
        query_select: QuerySelect<'a>,
    ) -> DomainResult<RowValue> {
        let row = {
            debug!("{}", prepared_inherent_insert);

            let stream = self
                .client()
                .query_raw(prepared_inherent_insert.deref(), field_buf)
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

            row
        };
        self.read_row_value_as_vertex(
            SqlColumnStream::new(&row),
            Some(query_select),
            IncludeEdgeAttrs::No,
            DataOperation::Inserted,
        )
    }
}

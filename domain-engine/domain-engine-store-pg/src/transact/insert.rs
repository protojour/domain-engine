use std::{borrow::Cow, collections::hash_map::Entry, ops::Deref};

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
    ontology::{
        domain::{DataRelationshipKind, DataRelationshipTarget, Def},
        ontol::ValueGenerator,
    },
    query::select::Select,
    value::Value,
    DefId, DomainIndex, PropId,
};
use pin_utils::pin_mut;
use tokio_postgres::{Row, ToStatement};
use tracing::{debug, trace, warn};

use crate::{
    pg_error::{map_row_error, PgError, PgInputError},
    pg_model::{
        EdgeId, InDomain, PgColumnRef, PgDataKey, PgDomainTable, PgDomainTableType, PgRepr, PgType,
    },
    sql::{self},
    sql_record::{SqlColumnStream, SqlRecordIterator},
    sql_value::{Layout, PgTimestamp, SqlScalar},
    statement::{Prepare, PreparedStatement, ToArcStr},
    transact::query::IncludeJoinedAttrs,
};

use super::{
    data::{Data, RowValue},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    mut_ctx::PgMutCtx,
    query::{DefAliasKey, QueryBuildCtx},
    query_select::{QuerySelect, QuerySelectRef},
    InsertMode, MutationMode, TransactCtx,
};

#[derive(Clone)]
pub struct PreparedInsert {
    inherent_stmt: PreparedStatement,
    edge_select_stmt: Option<PreparedStatement>,
}

struct AnalyzedInput {
    sql_params: Vec<SqlScalar>,
    query_kind: InsertQueryKind,
    edges: EdgePatches,
    abstract_values: Vec<(PropId, Value)>,
    query_select: QuerySelect,
    timestamp: PgTimestamp,
}

enum InsertQueryKind {
    Insert,
    // Upsert,
    UpdateTentative(PgDataKey),
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
        insert_mode: InsertMode,
        select: &'a Select,
        timestamp: PgTimestamp,
        mut_ctx: &'s mut PgMutCtx,
    ) -> BoxFuture<'_, DomainResult<RowValue>> {
        Box::pin(async move {
            self.insert_vertex_impl(value, insert_mode, select, timestamp, mut_ctx)
                // .instrument(debug_span!("ins", id = ?def_id))
                .await
        })
    }

    async fn insert_vertex_impl<'s>(
        &'s self,
        mut value: InDomain<Value>,
        insert_mode: InsertMode,
        select: &'a Select,
        timestamp: PgTimestamp,
        mut_ctx: &'s mut PgMutCtx,
    ) -> DomainResult<RowValue> {
        let domain_index = value.domain_index;
        let def_id = value.value.type_def_id();

        mut_ctx.write_stats.mark_mutated(def_id);

        let cache_key = (insert_mode, domain_index, def_id);

        let prepared = if let Some(prepared) = mut_ctx.cache.insert.get(&cache_key).cloned() {
            prepared
        } else {
            let prepared = self
                .prepare_insert(domain_index, def_id, insert_mode, select, mut_ctx)
                .await?;
            mut_ctx.cache.insert.insert(cache_key, prepared.clone());
            prepared
        };

        self.preprocess_insert_value(insert_mode, &mut value.value)?;

        let def = self.ontology.def(value.value.type_def_id());
        let (mut analyzed, pg) =
            self.analyze_input(None, def, value, select, timestamp, mut_ctx)?;
        let sql_params = analyzed.sql_params;

        let mut row_value = match analyzed.query_kind {
            InsertQueryKind::Insert => {
                self.insert_row(
                    &prepared.inherent_stmt,
                    &sql_params,
                    analyzed.query_select.as_ref(),
                )
                .await?
            }
            InsertQueryKind::UpdateTentative(data_key) => {
                self.update_tentative_vertex(
                    def,
                    pg,
                    sql_params,
                    data_key,
                    analyzed.timestamp,
                    mut_ctx,
                )
                .await?
            }
        };

        // patch edges
        {
            if matches!(row_value.op, DataOperation::Updated) {
                for edge_path in analyzed.edges.patches.values_mut() {
                    for tuple in &mut edge_path.tuples {
                        for (_, _, mutation_mode) in &mut tuple.elements {
                            if let MutationMode::Create(InsertMode::Insert) = mutation_mode {
                                *mutation_mode = MutationMode::Create(InsertMode::Upsert);
                            }
                        }
                    }
                }
            }

            self.patch_edges(
                pg.table,
                row_value.data_key,
                analyzed.edges,
                analyzed.timestamp,
                mut_ctx,
            )
            .await?;
        }

        for (prop_id, value) in analyzed.abstract_values {
            self.insert_abstract_sub_value(
                ParentProp {
                    prop_id,
                    key: row_value.data_key,
                },
                value,
                row_value.updated_at,
                mut_ctx,
            )
            .await?;
        }

        if let (Some(edge_select_stmt), QuerySelect::Vertex(vertex_select)) =
            (prepared.edge_select_stmt, analyzed.query_select)
        {
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

            self.read_edge_attributes(&mut column_stream, def, &vertex_select, attrs.as_mut())?;
        }

        Ok(row_value)
    }

    /// Instead of doing an INSERT, do an UPDATE against a vertex with a "tentative key".
    /// A tentative vertex has been inserted into the database upon seing a foreign key referencing that vertice.
    /// It has to exist in the DB because its _data key_ need to exist.
    /// At the end of the transaction, all the tentative keys will be checked to have been updated with proper vertex data.
    async fn update_tentative_vertex<'s>(
        &'s self,
        def: &Def,
        pg: PgDomainTable<'a>,
        mut sql_params: Vec<SqlScalar>,
        data_key: PgDataKey,
        timestamp: PgTimestamp,
        mut_ctx: &'s mut PgMutCtx,
    ) -> DomainResult<RowValue> {
        let cache_key = (def.id.domain_index(), def.id);

        let prepared =
            if let Some(prepared) = mut_ctx.cache.update_tentative.get(&cache_key).cloned() {
                prepared
            } else {
                let mut sql_update = sql::Update {
                    with: None,
                    table_name: pg.table_name(),
                    set: vec![
                        sql::UpdateColumn("_created", sql::Expr::param(0)),
                        sql::UpdateColumn("_updated", sql::Expr::param(0)),
                    ],
                    where_: Some(sql::Expr::eq(
                        sql::Expr::path1("_key"),
                        sql::Expr::param(sql_params.len()),
                    )),
                    returning: vec![sql::Expr::LiteralInt(0)],
                };
                let mut param_idx = 1;

                for prop_id in def.data_relationships.keys() {
                    if let Some(pg_column) = pg.table.find_column(prop_id) {
                        sql_update.set.push(sql::UpdateColumn(
                            pg_column.col_name,
                            sql::Expr::param(param_idx),
                        ));
                        param_idx += 1;
                    }
                }

                let prepared = sql_update.to_arcstr().prepare(self.client()).await?;
                mut_ctx
                    .cache
                    .update_tentative
                    .insert(cache_key, prepared.clone());
                prepared
            };

        let mut param_idx = 1;

        let mut attrs: FnvHashMap<PropId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        for (prop_id, rel_info) in &def.data_relationships {
            if pg.table.find_column(prop_id).is_some() {
                let input_param = sql_params.get(param_idx).unwrap();

                let unit_val =
                    self.deserialize_sql(rel_info.target.def_id(), input_param.clone().into())?;
                attrs.insert(*prop_id, Attr::Unit(unit_val));
                param_idx += 1;
            } else if let Some(ValueGenerator::CreatedAtTime | ValueGenerator::UpdatedAtTime) =
                rel_info.generator
            {
                let input_param = sql_params.first().unwrap();

                let unit_val =
                    self.deserialize_sql(rel_info.target.def_id(), input_param.clone().into())?;
                attrs.insert(*prop_id, Attr::Unit(unit_val));
            }
        }

        assert_eq!(param_idx, sql_params.len());

        sql_params.push(SqlScalar::I64(data_key));

        debug!("{prepared}");
        trace!("{sql_params:?}");

        self.query_one_raw(prepared.deref(), &sql_params).await?;

        Ok(RowValue {
            value: Value::Struct(Box::new(attrs), def.id.into()),
            def_key: pg.table.key,
            data_key,
            updated_at: timestamp,
            op: DataOperation::Inserted,
        })
    }

    fn insert_abstract_sub_value<'s>(
        &'s self,
        parent: ParentProp,
        value: Value,
        timestamp: PgTimestamp,
        mut_ctx: &'s mut PgMutCtx,
    ) -> BoxFuture<'_, DomainResult<()>> {
        Box::pin(self.insert_abstract_sub_value_impl(parent, value, timestamp, mut_ctx))
    }

    async fn insert_abstract_sub_value_impl(
        &self,
        parent: ParentProp,
        value: Value,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let value_def_id = value.type_def_id();
        let def = self.ontology.def(value_def_id);

        if let Some(PgRepr::Scalar(_pg_type, ontol_def_tag)) =
            PgRepr::classify_opt_def_repr(def.repr(), self.ontology)
        {
            let domain_index = parent.prop_id.0.domain_index();
            let def_id = ontol_def_tag.def_id();

            let prepared = if let Some(prepared) = mut_ctx
                .cache
                .insert
                .get(&(InsertMode::Insert, domain_index, def_id))
                .cloned()
            {
                prepared
            } else {
                let prepared = self
                    .prepare_insert(
                        domain_index,
                        def_id,
                        InsertMode::Insert,
                        &Select::Unit,
                        mut_ctx,
                    )
                    .await?;
                mut_ctx
                    .cache
                    .insert
                    .insert((InsertMode::Insert, domain_index, def_id), prepared.clone());
                prepared
            };

            let parent_prop_key = self
                .pg_model
                .datatable(parent.prop_id.0.domain_index(), parent.prop_id.0)?
                .abstract_property(&parent.prop_id)?;

            let Data::Sql(value) = self.data_from_value(value)? else {
                panic!("value must be a scalar here");
            };

            let sql_params = vec![
                SqlScalar::Timestamp(timestamp),
                SqlScalar::I32(parent_prop_key),
                SqlScalar::I64(parent.key),
                value,
            ];

            self.insert_row(&prepared.inherent_stmt, &sql_params, QuerySelectRef::Unit)
                .await?;

            Ok(())
        } else {
            let domain_index = value_def_id.0;
            let def_id = value_def_id;

            let cache_key = (InsertMode::Insert, domain_index, def_id);

            let prepared = if let Some(prepared) = mut_ctx.cache.insert.get(&cache_key).cloned() {
                prepared
            } else {
                let prepared = self
                    .prepare_insert(
                        domain_index,
                        def_id,
                        InsertMode::Insert,
                        &Select::Unit,
                        mut_ctx,
                    )
                    .await?;
                mut_ctx.cache.insert.insert(cache_key, prepared.clone());
                prepared
            };

            let def = self.ontology.def(def_id);
            let (analyzed, pg) = self.analyze_input(
                Some(parent),
                def,
                InDomain {
                    domain_index,
                    value,
                },
                &Select::Unit,
                timestamp,
                mut_ctx,
            )?;

            let row_value = self
                .insert_row(
                    &prepared.inherent_stmt,
                    &analyzed.sql_params,
                    analyzed.query_select.as_ref(),
                )
                .await?;

            self.patch_edges(
                pg.table,
                row_value.data_key,
                analyzed.edges,
                analyzed.timestamp,
                mut_ctx,
            )
            .await?;

            for (prop_id, value) in analyzed.abstract_values {
                self.insert_abstract_sub_value(
                    ParentProp {
                        prop_id,
                        key: row_value.data_key,
                    },
                    value,
                    row_value.updated_at,
                    mut_ctx,
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
        domain_index: DomainIndex,
        def_id: DefId,
        insert_mode: InsertMode,
        select: &'a Select,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<PreparedInsert> {
        let def = self.ontology.def(def_id);
        let pg = self.pg_model.pg_domain_datatable(domain_index, def_id)?;

        let mut query_ctx = QueryBuildCtx::default();
        let root_alias = query_ctx.alias;

        let vertex_select = self.analyze_vertex_select(def, pg.table, select)?;

        query_ctx.with_def_aliases.insert(
            DefAliasKey {
                def_id,
                inherent_set: Cow::Owned(vertex_select.inherent_set.clone()),
            },
            root_alias,
        );

        let mut edge_select_stmt: Option<PreparedStatement> = None;

        let mut column_names = vec!["_key", "_created", "_updated"];
        let mut values = vec![sql::Expr::Default, sql::Expr::param(0), sql::Expr::param(0)];
        let mut insert_returning = vec![
            // The first returned column is `xmax = 0` which is used to determine whether created or updated for UPSERTs
            sql::Expr::eq(sql::Expr::path1("xmax"), sql::Expr::LiteralInt(0)),
        ];
        let mut primary_id_column: Option<PgColumnRef> = None;
        let mut on_conflict: Option<sql::OnConflict> = None;

        let mut param_idx = 1;

        if pg.table.has_fkey {
            column_names.extend(["_fprop", "_fkey"]);
            values.extend([sql::Expr::param(1), sql::Expr::param(2)]);
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
                    let pg_column = pg.table.find_column(prop_id);

                    match &rel_info.kind {
                        DataRelationshipKind::Id => {
                            primary_id_column = pg_column;
                        }
                        DataRelationshipKind::Tree => {}
                        DataRelationshipKind::Edge(_) => continue,
                    }

                    if let Some(pg_column) = pg_column {
                        column_names.push(pg_column.col_name);
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

        // UPSERT / ON CONFLICT clause handling
        if let (InsertMode::Upsert, Some(primary_id_column)) = (insert_mode, primary_id_column) {
            let mut update_columns: Vec<sql::UpdateColumn> =
                vec![sql::UpdateColumn("_updated", sql::Expr::param(0))];
            let mut param_idx = 1;

            for (prop_id, rel_info) in &def.data_relationships {
                match rel_info.kind {
                    DataRelationshipKind::Id => {
                        if pg.table.find_column(prop_id).is_some() {
                            param_idx += 1;
                        }
                    }
                    DataRelationshipKind::Tree => {
                        // TODO: Make sure not to write "write-once" columns like Created time
                        if let Some(pg_column) = pg.table.find_column(prop_id) {
                            update_columns.push(sql::UpdateColumn(
                                pg_column.col_name,
                                sql::Expr::param(param_idx),
                            ));
                            param_idx += 1;
                        }
                    }
                    DataRelationshipKind::Edge(_) => {}
                }
            }

            on_conflict = Some(sql::OnConflict {
                target: Some(sql::ConflictTarget::Columns(vec![
                    &primary_id_column.col_name,
                ])),
                action: sql::ConflictAction::DoUpdateSet(update_columns),
            });
        }

        // returning
        {
            insert_returning.extend(self.initial_standard_data_fields(pg));
            self.select_inherent_vertex_fields(
                pg.table,
                &vertex_select,
                &mut insert_returning,
                None,
            );

            if !vertex_select.edge_set.is_empty() {
                let mut ctx = QueryBuildCtx::default();
                let root_alias = ctx.alias;
                let mut expressions = sql::Expressions {
                    items: vec![],
                    multiline: true,
                };

                self.sql_select_edge_properties(
                    (def, pg),
                    root_alias,
                    &vertex_select,
                    &mut ctx,
                    &mut expressions.items,
                    mut_ctx,
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
                        .to_arcstr()
                        .prepare(self.client())
                        .await?,
                    );
                }
            }
        }

        let insert = sql::Insert {
            with: query_ctx.with(),
            into: pg.table_name(),
            as_: None,
            column_names,
            values,
            on_conflict,
            returning: insert_returning,
        };

        Ok(PreparedInsert {
            inherent_stmt: insert.to_arcstr().prepare(self.client()).await?,
            edge_select_stmt,
        })
    }

    fn analyze_input(
        &self,
        parent: Option<ParentProp>,
        def: &Def,
        value: InDomain<Value>,
        select: &'a Select,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<(AnalyzedInput, PgDomainTable<'_>)> {
        let pg = self
            .pg_model
            .pg_domain_datatable(value.domain_index, value.type_def_id())?;
        let pg_table = pg.table;

        let Value::Struct(mut attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut sql_params: Vec<SqlScalar> = vec![SqlScalar::Timestamp(timestamp)];
        let mut update_tentative: Option<PgDataKey> = None;
        let mut edge_patches = EdgePatches::default();
        let mut abstract_values: Vec<(PropId, Value)> = vec![];

        if pg_table.has_fkey {
            let Some(parent) = parent else {
                panic!("missing parent property for fkey");
            };

            let parent_prop_key = self
                .pg_model
                .datatable(parent.prop_id.0.domain_index(), parent.prop_id.0)?
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
                                        if let Some(tentative_foreign_ids) = mut_ctx
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
                            if matches!(
                                PgRepr::classify_property(rel_info, *def_id, self.ontology),
                                PgRepr::Unit
                            ) {
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
            Select::EntityId => QuerySelect::EntityId,
            select @ Select::Struct(_) => {
                QuerySelect::Vertex(self.analyze_vertex_select(def, pg.table, select)?)
            }
            Select::Unit => QuerySelect::Unit,
            select => todo!("{select:?}"),
        };

        Ok((
            AnalyzedInput {
                sql_params,
                query_kind: if let Some(data_key) = update_tentative {
                    InsertQueryKind::UpdateTentative(data_key)
                } else {
                    InsertQueryKind::Insert
                },
                edges: edge_patches,
                abstract_values,
                query_select,
                timestamp,
            },
            pg,
        ))
    }

    async fn insert_row(
        &self,
        stmt: &PreparedStatement,
        sql_params: &[SqlScalar],
        query_select: QuerySelectRef<'a>,
    ) -> DomainResult<RowValue> {
        debug!("{}", stmt);
        let row = self.query_one_raw(stmt.deref(), sql_params).await?;
        let mut column_stream = SqlColumnStream::new(&row);

        // read the initial `xmax = 0` column
        let inserted = column_stream
            .next_field(&Layout::Scalar(PgType::Boolean))?
            .into_bool()?;

        self.read_vertex_row_value(
            column_stream,
            query_select,
            IncludeJoinedAttrs::No,
            if inserted {
                DataOperation::Inserted
            } else {
                DataOperation::Updated
            },
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
            .map_err(|e| map_row_error(e, PgDomainTableType::Vertex))?
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

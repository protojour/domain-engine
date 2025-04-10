use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use arcstr::ArcStr;
use domain_engine_core::{DomainError, DomainResult, domain_error::DomainErrorKind};
use futures_util::{TryFutureExt, TryStreamExt};
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt, PropId,
    attr::Attr,
    format_utils::format_value,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget},
    query::select::Select,
    tuple::CardinalIdx,
    value::Value,
};
use postgres_types::ToSql;
use tracing::{debug, trace};

use crate::{
    CountRows,
    address::deserialize_ontol_vertex_address,
    pg_error::{PgError, PgInputError, PgModelError, ds_bad_req, map_row_error},
    pg_model::{
        EdgeId, InDomain, PgColumnRef, PgDataKey, PgDomainTable, PgDomainTableType, PgEdgeCardinal,
        PgEdgeCardinalKind, PgIndexType, PgRegKey, PgTable,
    },
    sql::{self, WhereExt},
    sql_value::{PgTimestamp, SqlScalar},
    statement::{Prepare, PreparedStatement, ToArcStr},
    transact::{InsertMode, data::Data, edge_query::edge_join_condition},
};

use super::{MutationMode, TransactCtx, mut_ctx::PgMutCtx};

#[derive(Default, Debug)]
pub struct EdgePatches {
    pub patches: BTreeMap<EdgeId, ProjectedEdgePatch>,
}

impl EdgePatches {
    pub fn patch(&mut self, edge_id: EdgeId, subject: CardinalIdx) -> &mut ProjectedEdgePatch {
        self.patches
            .entry(edge_id)
            .or_insert_with(|| ProjectedEdgePatch {
                subject,
                tuples: vec![],
                deletes: Default::default(),
            })
    }
}

#[derive(Debug)]
pub struct ProjectedEdgePatch {
    pub subject: CardinalIdx,
    pub tuples: Vec<EdgeEndoTuplePatch>,
    pub deletes: BTreeMap<CardinalIdx, Vec<Value>>,
}

impl ProjectedEdgePatch {
    pub fn delete(&mut self, object_idx: CardinalIdx, foreign_id: Value) {
        self.deletes.entry(object_idx).or_default().push(foreign_id);
    }
}

#[derive(Debug)]
pub struct EdgeEndoTuplePatch {
    /// The length of the vector is the edge cardinality - 1
    pub elements: Vec<(CardinalIdx, Value, MutationMode)>,
}

impl EdgeEndoTuplePatch {
    pub fn into_element_ops(self) -> impl Iterator<Item = (Value, MutationMode)> {
        self.elements.into_iter().map(|(_, value, op)| (value, op))
    }

    pub fn from_tuple(it: impl Iterator<Item = (Value, MutationMode)>) -> Self {
        Self {
            elements: it
                .enumerate()
                .map(|(idx, (value, mode))| (CardinalIdx(idx as u8), value, mode))
                .collect(),
        }
    }

    /// build the tuple one-by-one
    pub fn insert_element(
        &mut self,
        idx: CardinalIdx,
        value: Value,
        mode: MutationMode,
    ) -> DomainResult<()> {
        match self.elements.binary_search_by_key(&idx, |(idx, ..)| *idx) {
            Ok(pos) => Err(PgModelError::DuplicateEdgeCardinal(pos).into()),
            Err(pos) => {
                self.elements.insert(pos, (idx, value, mode));
                Ok(())
            }
        }
    }
}

enum ProjectedEdgeCardinal<'a> {
    Subject(&'a PgEdgeCardinal, Dynamic<'a>),
    Object(&'a PgEdgeCardinal, Dynamic<'a>),
    Parameters,
}

enum Dynamic<'a> {
    Yes { def_col_name: &'a str },
    No,
}

#[derive(Default)]
struct EdgeAnalysis<'a> {
    param_order: BTreeSet<(PropId, DefId)>,
    projected_cardinals: Vec<ProjectedEdgeCardinal<'a>>,
}

impl<'a> TransactCtx<'a> {
    pub async fn patch_edges(
        &self,
        subject_datatable: &PgTable,
        subject_data_key: PgDataKey,
        patches: EdgePatches,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        // debug!(?patches, "patch edges");

        for (edge_id, patch) in patches.patches {
            self.patch_edge(
                edge_id,
                subject_datatable,
                subject_data_key,
                patch,
                timestamp,
                mut_ctx,
            )
            .await?;
        }

        Ok(())
    }

    async fn patch_edge(
        &self,
        edge_id: EdgeId,
        subject_datatable: &PgTable,
        subject_data_key: PgDataKey,
        patch: ProjectedEdgePatch,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let subject_index = patch.subject;
        let pg_edge = self.pg_model.pg_domain_edgetable(&edge_id)?;

        let mut unique_cardinal: Option<CardinalIdx> = None;
        let mut conflict_actions: Vec<sql::UpdateColumn> = vec![];

        for (index, pg_cardinal) in &pg_edge.table.edge_cardinals {
            if matches!(pg_cardinal.index_type, Some(PgIndexType::Unique)) {
                unique_cardinal = Some(*index);
            }
        }

        let mut sql_insert = sql::Insert {
            with: None,
            into: pg_edge.table_name(),
            as_: None,
            column_names: vec!["_created", "_updated"],
            values: vec![sql::Expr::param(0), sql::Expr::param(0)],
            on_conflict: Some(sql::OnConflict {
                target: None,
                action: sql::ConflictAction::DoNothing,
            }),
            returning: vec![sql::Expr::LiteralInt(0)],
        };

        let mut analysis = EdgeAnalysis::default();

        let mut param_index: usize = 1;

        for (index, pg_cardinal) in &pg_edge.table.edge_cardinals {
            match &pg_cardinal.kind {
                PgEdgeCardinalKind::Dynamic {
                    def_col_name,
                    key_col_name,
                } => {
                    sql_insert
                        .column_names
                        .extend([def_col_name.as_ref(), key_col_name]);
                    sql_insert.values.extend([
                        sql::Expr::param(param_index),
                        sql::Expr::param(param_index + 1),
                    ]);
                    let dynamic = Dynamic::Yes { def_col_name };
                    analysis
                        .projected_cardinals
                        .push(if *index == subject_index {
                            ProjectedEdgeCardinal::Subject(pg_cardinal, dynamic)
                        } else {
                            ProjectedEdgeCardinal::Object(pg_cardinal, dynamic)
                        });

                    if unique_cardinal.is_some() && Some(*index) != unique_cardinal {
                        conflict_actions.push(sql::UpdateColumn(
                            def_col_name,
                            sql::Expr::param(param_index),
                        ));
                        conflict_actions.push(sql::UpdateColumn(
                            key_col_name,
                            sql::Expr::param(param_index + 1),
                        ));
                    }

                    param_index += 2;
                }
                PgEdgeCardinalKind::PinnedDef { key_col_name, .. } => {
                    sql_insert.column_names.push(key_col_name);
                    sql_insert.values.push(sql::Expr::param(param_index));
                    analysis
                        .projected_cardinals
                        .push(if *index == subject_index {
                            ProjectedEdgeCardinal::Subject(pg_cardinal, Dynamic::No)
                        } else {
                            ProjectedEdgeCardinal::Object(pg_cardinal, Dynamic::No)
                        });

                    if unique_cardinal.is_some() && Some(*index) != unique_cardinal {
                        conflict_actions.push(sql::UpdateColumn(
                            key_col_name,
                            sql::Expr::param(param_index),
                        ));
                    }

                    param_index += 1;
                }
                PgEdgeCardinalKind::Parameters(params_def_id) => {
                    let def = self.ontology_defs.def(*params_def_id);
                    for (prop_id, rel_info) in &def.data_relationships {
                        if let DataRelationshipKind::Tree(_) = &rel_info.kind {
                            match &rel_info.target {
                                DataRelationshipTarget::Unambiguous(def_id) => {
                                    let pg_column = pg_edge.table.column(prop_id)?;
                                    sql_insert.column_names.push(pg_column.col_name);
                                    sql_insert.values.push(sql::Expr::param(param_index));
                                    analysis.param_order.insert((*prop_id, *def_id));
                                    param_index += 1;
                                }
                                DataRelationshipTarget::Union(_) => {
                                    todo!("union in params");
                                }
                            }
                        }
                    }
                    analysis
                        .projected_cardinals
                        .push(ProjectedEdgeCardinal::Parameters);
                }
            }
        }

        if let Some(unique_cardinal) = unique_cardinal {
            if !conflict_actions.is_empty() {
                let pg_unique_cardinal = pg_edge.table.edge_cardinal(unique_cardinal)?;

                let mut unique_columns = vec![];

                match &pg_unique_cardinal.kind {
                    PgEdgeCardinalKind::Dynamic {
                        def_col_name,
                        key_col_name,
                    } => {
                        unique_columns.extend([def_col_name, key_col_name.as_ref()]);
                    }
                    _ => return Err(PgModelError::InvalidUniqueCardinal.into()),
                }

                sql_insert.on_conflict = Some(sql::OnConflict {
                    target: Some(sql::ConflictTarget::Columns(unique_columns)),
                    action: sql::ConflictAction::DoUpdateSet(conflict_actions),
                });
            }
        }

        let mut insert_sql: Option<ArcStr> = None;

        let mut sql_params: Vec<SqlScalar> = vec![];

        for tuple in patch.tuples {
            sql_params.clear();

            if tuple.elements.iter().any(|(_, _, mutation_mode)| {
                matches!(
                    mutation_mode,
                    MutationMode::Update | MutationMode::UpdateEdgeCardinal
                )
            }) {
                self.update_edge(
                    pg_edge,
                    (subject_datatable, subject_data_key),
                    &analysis,
                    tuple,
                    &mut sql_params,
                    timestamp,
                    mut_ctx,
                )
                .await?;
            } else {
                self.insert_edge(
                    (subject_datatable, subject_data_key),
                    &analysis,
                    tuple,
                    &mut sql_params,
                    insert_sql.get_or_insert_with(|| arcstr::format!("{}", sql_insert)),
                    timestamp,
                    mut_ctx,
                )
                .await?;
            }
        }

        for (cardinal_idx, foreign_ids) in patch.deletes {
            let pg_cardinal = pg_edge.table.edge_cardinal(cardinal_idx)?;

            let mut foreigns_per_vertex: BTreeMap<DefId, Vec<Value>> = Default::default();

            for id in foreign_ids {
                let vertex_def_id = self
                    .pg_model
                    .entity_id_to_entity
                    .get(&id.type_def_id())
                    .ok_or_else(|| DomainErrorKind::NotAnEntity(id.type_def_id()).into_error())?;

                foreigns_per_vertex
                    .entry(*vertex_def_id)
                    .or_default()
                    .push(id);
            }

            for (vertex_def_id, foreign_ids) in foreigns_per_vertex {
                let mut sql_ids_param: Vec<SqlScalar> = Vec::with_capacity(foreign_ids.len());
                let pg_foreign = self
                    .pg_model
                    .pg_domain_datatable(vertex_def_id.domain_index(), vertex_def_id)?;
                let foreign_def = self.ontology_defs.def(vertex_def_id);
                let Some(foreign_entity) = foreign_def.entity() else {
                    return Err(PgInputError::NotAnEntity.into());
                };

                for id in foreign_ids {
                    let Data::Sql(sql_val) = self.data_from_value(id)? else {
                        return Err(PgInputError::IdMustBeScalar)?;
                    };
                    sql_ids_param.push(sql_val);
                }

                let mut sql_delete = sql::Delete {
                    from: pg_edge.table_name(),
                    where_: None,
                    returning: vec![],
                };

                let key_col_name = match &pg_cardinal.kind {
                    PgEdgeCardinalKind::Dynamic {
                        def_col_name,
                        key_col_name,
                    } => {
                        sql_delete.where_and(sql::Expr::eq(
                            sql::Expr::path1(def_col_name.as_ref()),
                            sql::Expr::LiteralInt(pg_foreign.table.key),
                        ));

                        key_col_name
                    }
                    PgEdgeCardinalKind::PinnedDef { key_col_name, .. } => key_col_name,
                    PgEdgeCardinalKind::Parameters(_) => {
                        return Err(DomainError::data_store_bad_request(
                            "cannot delete edge based on parameters (yet)",
                        ));
                    }
                };

                let pg_foreign_id = pg_foreign.table.column(&foreign_entity.id_prop)?;

                sql_delete.where_and(sql::Expr::in_(
                    sql::Expr::path1(key_col_name.as_ref()),
                    sql::Expr::Select(Box::new(sql::Select {
                        expressions: vec![sql::Expr::path1("_key")].into(),
                        from: vec![pg_foreign.table_name().into()],
                        where_: Some(sql::Expr::eq(
                            sql::Expr::path1(pg_foreign_id.col_name),
                            sql::Expr::Any(Box::new(sql::Expr::param(0))),
                        )),
                        ..Default::default()
                    })),
                ));

                let sql = sql_delete.to_string();
                debug!("{sql}");

                self.client()
                    .query(&sql, &[&sql_ids_param])
                    .await
                    .map_err(PgError::EdgeDeletion)?;
            }
        }

        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    async fn insert_edge<'s>(
        &'s self,
        (subject_datatable, subject_data_key): (&PgTable, PgDataKey),
        analysis: &EdgeAnalysis<'s>,
        tuple: EdgeEndoTuplePatch,
        sql_params: &mut Vec<SqlScalar>,
        insert_sql: &ArcStr,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let is_upsert = tuple.elements.iter().any(|(.., mutation_mode)| {
            matches!(mutation_mode, MutationMode::Create(InsertMode::Upsert))
        });

        sql_params.push(SqlScalar::Timestamp(timestamp));

        let mut element_iter = tuple.into_element_ops();

        for projected_cardinal in &analysis.projected_cardinals {
            match projected_cardinal {
                ProjectedEdgeCardinal::Subject(_, Dynamic::Yes { .. }) => {
                    sql_params.extend([
                        SqlScalar::I32(subject_datatable.key),
                        SqlScalar::I64(subject_data_key),
                    ]);
                }
                ProjectedEdgeCardinal::Subject(_, Dynamic::No) => {
                    sql_params.push(SqlScalar::I64(subject_data_key));
                }
                ProjectedEdgeCardinal::Object(.., dynamic) => {
                    let (value, mode) = element_iter.next().unwrap();
                    let (foreign_def_id, _, foreign_key) = self
                        .resolve_linked_vertex(value, mode, Select::EntityId, timestamp, mut_ctx)
                        .await?;

                    if matches!(dynamic, Dynamic::Yes { .. }) {
                        let datatable = self
                            .pg_model
                            .datatable(foreign_def_id.domain_index(), foreign_def_id)?;
                        sql_params.push(SqlScalar::I32(datatable.key));
                    }

                    sql_params.push(SqlScalar::I64(foreign_key));
                }
                ProjectedEdgeCardinal::Parameters => {
                    let Some((Value::Struct(mut map, _), _op)) = element_iter.next() else {
                        return Err(ds_bad_req("edge params must be a struct"));
                    };

                    for (prop_id, _def_id) in &analysis.param_order {
                        let data = match map.remove(prop_id) {
                            Some(Attr::Unit(value)) => self.data_from_value(value)?,
                            Some(_) => {
                                return Err(ds_bad_req("non-scalar attribute in edge parameter"));
                            }
                            None => Data::Sql(SqlScalar::Null),
                        };

                        match data {
                            Data::Sql(sql_val) => {
                                sql_params.push(sql_val);
                            }
                            Data::Compound(_) => {
                                return Err(ds_bad_req("non-scalar value in edge parameter"));
                            }
                        }
                    }
                }
            }
        }

        let stmt = self.edge_patch_stmt_cached(insert_sql, mut_ctx).await?;

        debug!("{stmt}");
        trace!("{sql_params:?}");

        let row_count = self
            .client()
            .query_raw(
                stmt.deref(),
                sql_params.iter().map(|param| param as &dyn ToSql),
            )
            .await
            .map_err(PgError::EdgeInsertion)?
            .try_collect::<CountRows>()
            .map_err(|e| map_row_error(e, PgDomainTableType::Edge))
            .await?;

        if row_count.0 == 1 || is_upsert {
            Ok(())
        } else {
            Err(DomainErrorKind::EdgeAlreadyExists.into_error())
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn update_edge<'s>(
        &'s self,
        pg_edge: PgDomainTable<'_>,
        (subject_datatable, subject_data_key): (&PgTable, PgDataKey),
        analysis: &EdgeAnalysis<'s>,
        tuple: EdgeEndoTuplePatch,
        sql_params: &mut Vec<SqlScalar>,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let mut element_iter = tuple.into_element_ops();

        let mut sql_update = sql::Update {
            with: None,
            table_name: pg_edge.table_name(),
            set: vec![],
            where_: None,
            returning: vec![sql::Expr::LiteralInt(0)],
        };

        sql_update
            .set
            .push(sql::UpdateColumn("_updated", sql::Expr::param(0)));
        sql_params.push(SqlScalar::Timestamp(timestamp));

        for projected_cardinal in &analysis.projected_cardinals {
            match projected_cardinal {
                ProjectedEdgeCardinal::Subject(pg_cardinal, _) => {
                    sql_update.where_and(edge_join_condition(
                        sql::Path::empty(),
                        pg_cardinal,
                        subject_datatable,
                        sql::Expr::param(sql_params.len()),
                    ));
                    sql_params.push(SqlScalar::I64(subject_data_key));
                }
                ProjectedEdgeCardinal::Object(pg_cardinal, dynamic) => {
                    // for objects, the edge itself is not updated
                    let (value, mode) = element_iter.next().unwrap();
                    let (foreign_def_id, foreign_def_key, foreign_data_key) = self
                        .resolve_linked_vertex(value, mode, Select::EntityId, timestamp, mut_ctx)
                        .await?;

                    match mode {
                        MutationMode::UpdateEdgeCardinal => {
                            if let Dynamic::Yes { def_col_name } = dynamic {
                                sql_update.set.push(sql::UpdateColumn(
                                    def_col_name,
                                    sql::Expr::param(sql_params.len()),
                                ));
                                sql_params.push(SqlScalar::I32(foreign_def_key));
                            }

                            sql_update.set.push(sql::UpdateColumn(
                                pg_cardinal.key_col_name().unwrap(),
                                sql::Expr::param(sql_params.len()),
                            ));
                            sql_params.push(SqlScalar::I64(foreign_data_key));
                        }
                        MutationMode::Create(_) | MutationMode::Update => {
                            let object_datatable = self
                                .pg_model
                                .datatable(foreign_def_id.domain_index(), foreign_def_id)?;

                            sql_update.where_and(edge_join_condition(
                                sql::Path::empty(),
                                pg_cardinal,
                                object_datatable,
                                sql::Expr::param(sql_params.len()),
                            ));
                            sql_params.push(SqlScalar::I64(foreign_data_key));
                        }
                    }
                }
                ProjectedEdgeCardinal::Parameters => {
                    let Some((Value::Struct(mut map, _), _op)) = element_iter.next() else {
                        return Err(ds_bad_req("edge params must be a struct"));
                    };

                    for (prop_id, _def_id) in &analysis.param_order {
                        let data = match map.remove(prop_id) {
                            Some(Attr::Unit(value)) => self.data_from_value(value)?,
                            Some(_) => {
                                return Err(ds_bad_req("non-scalar attribute in edge parameter"));
                            }
                            None => Data::Sql(SqlScalar::Null),
                        };

                        match data {
                            Data::Sql(sql_val) => {
                                let field = pg_edge.table.column(prop_id)?;
                                sql_update.set.push(sql::UpdateColumn(
                                    field.col_name,
                                    sql::Expr::param(sql_params.len()),
                                ));
                                sql_params.push(sql_val);
                            }
                            Data::Compound(_) => {
                                return Err(ds_bad_req("non-scalar value in edge parameter"));
                            }
                        }
                    }
                }
            }
        }

        let stmt = self
            .edge_patch_stmt_cached(
                &if !sql_update.set.is_empty() {
                    ArcStr::from(sql_update.to_string())
                } else {
                    // nothing to update, but it should be proven that the edge exists.
                    debug!("nothing to update");
                    let sql_select = sql::Select {
                        expressions: vec![sql::Expr::LiteralInt(0)].into(),
                        from: vec![pg_edge.table_name().into()],
                        where_: sql_update.where_,
                        limit: sql::Limit {
                            limit: Some(1),
                            offset: None,
                        },
                        ..Default::default()
                    };
                    ArcStr::from(sql_select.to_string())
                },
                mut_ctx,
            )
            .await?;

        debug!("{stmt}");
        trace!("{sql_params:?}");

        let row_count = self
            .client()
            .query_raw(
                stmt.deref(),
                sql_params.iter().map(|param| param as &dyn ToSql),
            )
            .await
            .map_err(PgError::EdgeUpdate)?
            .try_collect::<CountRows>()
            .map_err(|e| map_row_error(e, PgDomainTableType::Edge))
            .await?;

        if row_count.0 == 1 {
            Ok(())
        } else {
            Err(DomainErrorKind::EdgeNotFound.into_error())
        }
    }

    async fn edge_patch_stmt_cached(
        &self,
        sql: &ArcStr,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<PreparedStatement> {
        if let Some(stmt) = mut_ctx.cache.edge_patch.get(sql).cloned() {
            Ok(stmt)
        } else {
            let stmt = sql.clone().prepare(self.client()).await?;
            mut_ctx
                .cache
                .edge_patch
                .insert(stmt.src().clone(), stmt.clone());
            Ok(stmt)
        }
    }

    pub async fn resolve_linked_vertex(
        &self,
        value: Value,
        mode: MutationMode,
        select: Select,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<(DefId, PgRegKey, PgDataKey)> {
        let def_id = value.type_def_id();

        enum ResolveMode {
            VertexData,
            Id(PropId, IdResolveMode),
        }

        enum IdResolveMode {
            Plain,
            SelfIdentifying,
        }

        let (resolve_mode, vertex_def_id, value) = if self
            .pg_model
            .find_datatable(def_id.domain_index(), def_id)
            .is_some()
        {
            let entity = self.ontology_defs.def(def_id).entity().unwrap();

            if entity.is_self_identifying {
                let Value::Struct(mut map, _) = value else {
                    return Err(ds_bad_req("must be a struct"));
                };
                let Some(Attr::Unit(id)) = map.remove(&entity.id_prop) else {
                    return Err(ds_bad_req("self-identifying vertex without id"));
                };

                (
                    ResolveMode::Id(entity.id_prop, IdResolveMode::SelfIdentifying),
                    def_id,
                    id,
                )
            } else {
                (ResolveMode::VertexData, def_id, value)
            }
        } else if let Some(vertex_def_id) = self.pg_model.entity_id_to_entity.get(&def_id) {
            let entity = self.ontology_defs.def(*vertex_def_id).entity().unwrap();
            let id_prop_id = entity.id_prop;
            let id_resolve_mode = if entity.is_self_identifying {
                IdResolveMode::SelfIdentifying
            } else {
                IdResolveMode::Plain
            };

            (
                ResolveMode::Id(id_prop_id, id_resolve_mode),
                *vertex_def_id,
                value,
            )
        } else if def_id == OntolDefTag::Vertex.def_id() {
            let (def_key, data_key) = deserialize_ontol_vertex_address(value)?;
            let (_domain_index, def_id) = self.pg_model.datatable_key_by_def_key(def_key)?;

            return Ok((def_id, def_key, data_key));
        } else {
            return Err(ds_bad_req("bad foreign key"));
        };

        match resolve_mode {
            ResolveMode::VertexData => {
                let row_value = match mode {
                    MutationMode::Create(insert_mode) => {
                        self.insert_vertex(
                            InDomain {
                                domain_index: def_id.domain_index(),
                                value,
                            },
                            insert_mode,
                            &select,
                            timestamp,
                            mut_ctx,
                        )
                        .await?
                    }
                    MutationMode::Update | MutationMode::UpdateEdgeCardinal => todo!(),
                };

                Ok((def_id, row_value.def_key, row_value.data_key))
            }
            ResolveMode::Id(id_prop_id, IdResolveMode::Plain) => {
                let (pg, pg_column, id_param) =
                    self.prepare_id_check(vertex_def_id, id_prop_id, value)?;

                let stmt_key = (vertex_def_id, id_prop_id);
                let select_stmt =
                    if let Some(stmt) = mut_ctx.cache.key_by_id.get(&stmt_key).cloned() {
                        stmt
                    } else {
                        let stmt = sql::Select {
                            expressions: sql::Expressions {
                                items: vec![sql::Expr::path1("_key")],
                                multiline: false,
                            },
                            from: vec![pg.table_name().into()],
                            where_: Some(sql::Expr::eq(
                                sql::Expr::path1(pg_column.col_name),
                                sql::Expr::param(0),
                            )),
                            ..Default::default()
                        }
                        .to_arcstr()
                        .prepare(self.client())
                        .await?;

                        mut_ctx.cache.key_by_id.insert(stmt_key, stmt.clone());
                        stmt
                    };

                debug!("{select_stmt}");
                trace!("resolve linked vertex {:?}", [&id_param]);

                let opt_row = self
                    .client()
                    .query_opt(select_stmt.deref(), &[&id_param])
                    .await
                    .map_err(PgError::ForeignKeyLookup)?;

                if let Some(row) = opt_row {
                    return Ok((vertex_def_id, pg.table.key, row.get(0)));
                } else if !self.txn_mode.is_atomic() {
                    return Err(self.unresolved_foreign_error(def_id, id_param));
                }

                // Key does not exist based on the ID, and the transaction is atomic.
                // Now, create a row that just maps a key to the ID,
                // and then assert before the transaction is committed that it's actually written.
                let insert_stmt =
                    if let Some(stmt) = mut_ctx.cache.insert_tmp_id.get(&stmt_key).cloned() {
                        stmt
                    } else {
                        let stmt = sql::Insert {
                            with: None,
                            into: pg.table_name(),
                            as_: None,
                            column_names: vec!["_created", "_updated", &pg_column.col_name],
                            values: vec![
                                sql::Expr::param(0),
                                sql::Expr::param(0),
                                sql::Expr::param(1),
                            ],
                            on_conflict: None,
                            returning: vec![sql::Expr::path1("_key")],
                        }
                        .to_arcstr()
                        .prepare(self.client())
                        .await?;

                        mut_ctx.cache.insert_tmp_id.insert(stmt_key, stmt.clone());
                        stmt
                    };

                debug!("{insert_stmt}");
                let sql_params: [&(dyn ToSql + Sync); 2] = [&timestamp, &id_param];

                let row = self
                    .client()
                    .query_one(insert_stmt.deref(), &sql_params)
                    .await
                    .map_err(PgError::TentativeForeignKey)?;

                let data_key = row.get(0);

                trace!(
                    "register tentative foreign key: {id_param:?}: ({}, {})",
                    pg.table.key, data_key
                );

                mut_ctx
                    .tentative_foreign_keys
                    .entry((id_prop_id, def_id))
                    .or_default()
                    .insert(id_param, (pg.table.key, data_key));

                Ok((vertex_def_id, pg.table.key, data_key))
            }
            ResolveMode::Id(id_prop_id, IdResolveMode::SelfIdentifying) => {
                let (pg, pg_column, id_param) =
                    self.prepare_id_check(vertex_def_id, id_prop_id, value)?;

                let stmt = if let Some(stmt) = mut_ctx
                    .cache
                    .upsert_self_identifying
                    .get(&vertex_def_id)
                    .cloned()
                {
                    stmt
                } else {
                    // upsert
                    // TODO: It might actually be better to do SELECT + optional INSERT.
                    // this approach (DO UPDATE SET) always re-appends the row.
                    let stmt = sql::Insert {
                        with: None,
                        into: pg.table_name(),
                        as_: None,
                        column_names: vec!["_created", "_updated", &pg_column.col_name],
                        values: vec![
                            sql::Expr::param(0),
                            sql::Expr::param(0),
                            sql::Expr::param(1),
                        ],
                        on_conflict: Some(sql::OnConflict {
                            target: Some(sql::ConflictTarget::Columns(vec![&pg_column.col_name])),
                            action: sql::ConflictAction::DoUpdateSet(vec![sql::UpdateColumn(
                                pg_column.col_name,
                                sql::Expr::param(1),
                            )]),
                        }),
                        returning: vec![sql::Expr::path1("_key")],
                    }
                    .to_arcstr()
                    .prepare(self.client())
                    .await?;

                    mut_ctx
                        .cache
                        .upsert_self_identifying
                        .insert(vertex_def_id, stmt.clone());
                    stmt
                };

                debug!("{stmt}");

                let sql_params: [&(dyn ToSql + Sync); 2] = [&timestamp, &id_param];
                trace!("resolve linked vertex {:?}", sql_params);

                let row = self
                    .client()
                    .query_opt(stmt.deref(), &sql_params)
                    .await
                    .map_err(PgError::ForeignKeyLookup)?
                    .ok_or_else(|| self.unresolved_foreign_error(def_id, id_param))?;

                Ok((vertex_def_id, pg.table.key, row.get(0)))
            }
        }
    }

    pub fn check_unresolved_foreign_keys(&self, mut_ctx: &PgMutCtx) -> DomainResult<()> {
        if !self.txn_mode.is_atomic() {
            return Ok(());
        }

        const MAX_REPORTED: usize = 2;
        let mut values: Vec<Value> = vec![];

        'outer: for ((_, def_id), scalars) in &mut_ctx.tentative_foreign_keys {
            for scalar in scalars.keys() {
                values.push(self.deserialize_sql(*def_id, scalar.clone())?);

                if values.len() >= MAX_REPORTED {
                    break 'outer;
                }
            }
        }

        match values.len() {
            0 => Ok(()),
            1 => {
                let value = values.into_iter().next().unwrap();
                Err(
                    DomainErrorKind::UnresolvedForeignKey(format_value(&value, self).to_string())
                        .into_error(),
                )
            }
            _ => {
                let value = values.into_iter().next().unwrap();
                Err(
                    DomainErrorKind::UnresolvedForeignKeys(format_value(&value, self).to_string())
                        .into_error(),
                )
            }
        }
    }

    pub fn unresolved_foreign_error(&self, def_id: DefId, id: SqlScalar) -> DomainError {
        match self.deserialize_sql(def_id, id) {
            Ok(value) => {
                DomainErrorKind::UnresolvedForeignKey(format_value(&value, self).to_string())
                    .into_error()
            }
            Err(error) => error,
        }
    }

    fn prepare_id_check(
        &self,
        vertex_def_id: DefId,
        id_prop_id: PropId,
        id: Value,
    ) -> DomainResult<(PgDomainTable<'a>, PgColumnRef<'a>, SqlScalar)> {
        let pg = self
            .pg_model
            .pg_domain_datatable(vertex_def_id.domain_index(), vertex_def_id)?;
        let pg_column = pg.table.column(&id_prop_id)?;

        let Data::Sql(id_param) = self.data_from_value(id)? else {
            return Err(ds_bad_req("compound foreign key"));
        };

        Ok((pg, pg_column, id_param))
    }
}

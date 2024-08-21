use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult};
use futures_util::{TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget},
    query::select::Select,
    tuple::CardinalIdx,
    value::Value,
    DefId, EdgeId, PropId,
};
use postgres_types::ToSql;
use tracing::{debug, trace};

use crate::{
    pg_error::{ds_bad_req, map_row_error, PgError, PgInputError, PgModelError},
    pg_model::{
        InDomain, PgDataKey, PgDomainTable, PgEdgeCardinal, PgEdgeCardinalKind, PgIndexType,
        PgRegKey, PgTable,
    },
    sql::{self, WhereExt},
    sql_value::SqlVal,
    statement::{Prepare, PreparedStatement},
    transact::{data::Data, edge_query::edge_join_condition},
    CountRows, IgnoreRows,
};

use super::{cache::PgCache, MutationMode, TransactCtx};

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
    #[allow(unused)]
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
        cache: &mut PgCache,
    ) -> DomainResult<()> {
        // debug!("patch edges: {patches:#?}");

        for (edge_id, patch) in patches.patches {
            self.patch_edge(edge_id, subject_datatable, subject_data_key, patch, cache)
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
        cache: &mut PgCache,
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
            column_names: vec![],
            values: vec![],
            on_conflict: None,
            returning: vec![],
        };

        let mut analysis = EdgeAnalysis::default();

        let mut param_index: usize = 0;

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
                    let def = self.ontology.def(*params_def_id);
                    for (prop_id, rel_info) in &def.data_relationships {
                        if let DataRelationshipKind::Tree = &rel_info.kind {
                            match &rel_info.target {
                                DataRelationshipTarget::Unambiguous(def_id) => {
                                    let pg_column = pg_edge.table.column(prop_id)?;
                                    sql_insert.column_names.push(&pg_column.col_name);
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
                        unique_columns.extend([def_col_name.as_ref(), key_col_name.as_ref()]);
                    }
                    _ => return Err(PgModelError::InvalidUniqueCardinal.into()),
                }

                sql_insert.on_conflict = Some(sql::OnConflict {
                    target: Some(sql::ConflictTarget::Columns(unique_columns)),
                    action: sql::ConflictAction::DoUpdateSet(conflict_actions),
                });
            }
        }

        let mut insert_sql: Option<String> = None;

        let mut param_buf: Vec<SqlVal> = vec![];

        for tuple in patch.tuples {
            param_buf.clear();

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
                    &mut param_buf,
                    cache,
                )
                .await?;
            } else {
                self.insert_edge(
                    (subject_datatable, subject_data_key),
                    &analysis,
                    tuple,
                    &mut param_buf,
                    insert_sql.get_or_insert_with(|| sql_insert.to_string()),
                    cache,
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
                let mut sql_ids_param: Vec<SqlVal> = Vec::with_capacity(foreign_ids.len());
                let pg_foreign = self
                    .pg_model
                    .pg_domain_datatable(vertex_def_id.package_id(), vertex_def_id)?;
                let foreign_def = self.ontology.def(vertex_def_id);
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
                        ))
                    }
                };

                let pg_foreign_id = pg_foreign.table.column(&foreign_entity.id_prop)?;

                sql_delete.where_and(sql::Expr::in_(
                    sql::Expr::path1(key_col_name.as_ref()),
                    sql::Expr::paren(sql::Expr::Select(Box::new(sql::Select {
                        expressions: vec![sql::Expr::path1("_key")].into(),
                        from: vec![pg_foreign.table_name().into()],
                        where_: Some(sql::Expr::eq(
                            sql::Expr::path1(pg_foreign_id.col_name.as_ref()),
                            sql::Expr::Any(Box::new(sql::Expr::param(0))),
                        )),
                        ..Default::default()
                    }))),
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

    async fn insert_edge<'s>(
        &'s self,
        (subject_datatable, subject_data_key): (&PgTable, PgDataKey),
        analysis: &EdgeAnalysis<'s>,
        tuple: EdgeEndoTuplePatch,
        param_buf: &mut Vec<SqlVal<'s>>,
        insert_sql: &str,
        cache: &mut PgCache,
    ) -> DomainResult<()> {
        let mut element_iter = tuple.into_element_ops();

        for projected_cardinal in &analysis.projected_cardinals {
            match projected_cardinal {
                ProjectedEdgeCardinal::Subject(_, Dynamic::Yes { .. }) => {
                    param_buf.extend([
                        SqlVal::I32(subject_datatable.key),
                        SqlVal::I64(subject_data_key),
                    ]);
                }
                ProjectedEdgeCardinal::Subject(_, Dynamic::No) => {
                    param_buf.push(SqlVal::I64(subject_data_key));
                }
                ProjectedEdgeCardinal::Object(.., dynamic) => {
                    let (value, mode) = element_iter.next().unwrap();
                    let (foreign_def_id, _, foreign_key) = self
                        .resolve_linked_vertex(value, mode, Select::EntityId, cache)
                        .await?;

                    if matches!(dynamic, Dynamic::Yes { .. }) {
                        let datatable = self
                            .pg_model
                            .datatable(foreign_def_id.package_id(), foreign_def_id)?;
                        param_buf.push(SqlVal::I32(datatable.key));
                    }

                    param_buf.push(SqlVal::I64(foreign_key));
                }
                ProjectedEdgeCardinal::Parameters => {
                    let Some((Value::Struct(mut map, _), _op)) = element_iter.next() else {
                        return Err(ds_bad_req("edge params must be a struct"));
                    };

                    for (prop_id, _def_id) in &analysis.param_order {
                        let data = match map.remove(prop_id) {
                            Some(Attr::Unit(value)) => self.data_from_value(value)?,
                            Some(_) => {
                                return Err(ds_bad_req("non-scalar attribute in edge parameter"))
                            }
                            None => Data::Sql(SqlVal::Null),
                        };

                        match data {
                            Data::Sql(sql_val) => {
                                param_buf.push(sql_val);
                            }
                            Data::Compound(_) => {
                                return Err(ds_bad_req("non-scalar value in edge parameter"));
                            }
                        }
                    }
                }
            }
        }

        debug!("{insert_sql}");
        trace!("{param_buf:?}");

        self.client()
            .query_raw(
                insert_sql,
                param_buf.iter().map(|param| param as &dyn ToSql),
            )
            .await
            .map_err(PgError::EdgeInsertion)?
            .try_collect::<IgnoreRows>()
            .map_err(map_row_error)
            .await?;

        Ok(())
    }

    async fn update_edge<'s>(
        &'s self,
        pg_edge: PgDomainTable<'_>,
        (subject_datatable, subject_data_key): (&PgTable, PgDataKey),
        analysis: &EdgeAnalysis<'s>,
        tuple: EdgeEndoTuplePatch,
        param_buf: &mut Vec<SqlVal<'s>>,
        cache: &mut PgCache,
    ) -> DomainResult<()> {
        let mut element_iter = tuple.into_element_ops();

        let mut sql_update = sql::Update {
            with: None,
            table_name: pg_edge.table_name(),
            set: vec![],
            where_: None,
            returning: vec![sql::Expr::LiteralInt(0)],
        };

        for projected_cardinal in &analysis.projected_cardinals {
            match projected_cardinal {
                ProjectedEdgeCardinal::Subject(pg_cardinal, _) => {
                    sql_update.where_and(edge_join_condition(
                        sql::Path::empty(),
                        pg_cardinal,
                        subject_datatable,
                        sql::Expr::param(param_buf.len()),
                    ));
                    param_buf.push(SqlVal::I64(subject_data_key));
                }
                ProjectedEdgeCardinal::Object(pg_cardinal, dynamic) => {
                    // for objects, the edge itself is not updated
                    let (value, mode) = element_iter.next().unwrap();
                    let (foreign_def_id, foreign_def_key, foreign_data_key) = self
                        .resolve_linked_vertex(value, mode, Select::EntityId, cache)
                        .await?;

                    match mode {
                        MutationMode::UpdateEdgeCardinal => {
                            if let Dynamic::Yes { def_col_name } = dynamic {
                                sql_update.set.push(sql::UpdateColumn(
                                    def_col_name,
                                    sql::Expr::param(param_buf.len()),
                                ));
                                param_buf.push(SqlVal::I32(foreign_def_key));
                            }

                            sql_update.set.push(sql::UpdateColumn(
                                pg_cardinal.key_col_name().unwrap(),
                                sql::Expr::param(param_buf.len()),
                            ));
                            param_buf.push(SqlVal::I64(foreign_data_key));
                        }
                        MutationMode::Create(_) | MutationMode::Update => {
                            let object_datatable = self
                                .pg_model
                                .datatable(foreign_def_id.package_id(), foreign_def_id)?;

                            sql_update.where_and(edge_join_condition(
                                sql::Path::empty(),
                                pg_cardinal,
                                object_datatable,
                                sql::Expr::param(param_buf.len()),
                            ));
                            param_buf.push(SqlVal::I64(foreign_data_key));
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
                                return Err(ds_bad_req("non-scalar attribute in edge parameter"))
                            }
                            None => Data::Sql(SqlVal::Null),
                        };

                        match data {
                            Data::Sql(sql_val) => {
                                let field = pg_edge.table.column(prop_id)?;
                                sql_update.set.push(sql::UpdateColumn(
                                    &field.col_name,
                                    sql::Expr::param(param_buf.len()),
                                ));
                                param_buf.push(sql_val);
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
            .edge_update_stmt_cached(
                if !sql_update.set.is_empty() {
                    sql_update.to_string()
                } else {
                    // nothing to update, but it should be proven that the edge exists.
                    debug!("nothing to update");
                    let sql_select = sql::Select {
                        with: None,
                        expressions: vec![sql::Expr::LiteralInt(0)].into(),
                        from: vec![pg_edge.table_name().into()],
                        where_: sql_update.where_,
                        limit: sql::Limit {
                            limit: Some(1),
                            offset: None,
                        },
                    };
                    sql_select.to_string()
                },
                cache,
            )
            .await?;

        debug!("{stmt}");
        trace!("{param_buf:?}");

        let count = self
            .client()
            .query_raw(
                stmt.deref(),
                param_buf.iter().map(|param| param as &dyn ToSql),
            )
            .await
            .map_err(PgError::EdgeUpdate)?
            .try_collect::<CountRows>()
            .map_err(map_row_error)
            .await?;

        if count.0 == 1 {
            Ok(())
        } else {
            Err(DomainErrorKind::EdgeNotFound.into_error())
        }
    }

    async fn edge_update_stmt_cached(
        &self,
        sql: String,
        cache: &mut PgCache,
    ) -> DomainResult<PreparedStatement> {
        if let Some(stmt) = cache.edge_update.get(&sql).cloned() {
            Ok(stmt)
        } else {
            let stmt = sql.prepare(self.client()).await?;
            cache.edge_update.insert(stmt.src().clone(), stmt.clone());
            Ok(stmt)
        }
    }

    pub async fn resolve_linked_vertex<'s>(
        &'s self,
        value: Value,
        mode: MutationMode,
        select: Select,
        cache: &mut PgCache,
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
            .find_datatable(def_id.package_id(), def_id)
            .is_some()
        {
            let entity = self.ontology.def(def_id).entity().unwrap();

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
            let entity = self.ontology.def(*vertex_def_id).entity().unwrap();
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
        } else {
            return Err(ds_bad_req("bad foreign key"));
        };

        match resolve_mode {
            ResolveMode::VertexData => {
                let row_value = match mode {
                    MutationMode::Create(insert_mode) => {
                        self.insert_vertex(
                            InDomain {
                                pkg_id: def_id.package_id(),
                                value,
                            },
                            insert_mode,
                            &select,
                            cache,
                        )
                        .await?
                    }
                    MutationMode::Update | MutationMode::UpdateEdgeCardinal => todo!(),
                };

                Ok((def_id, row_value.def_key, row_value.data_key))
            }
            ResolveMode::Id(id_prop_id, id_resolve_mode) => {
                let pg = self
                    .pg_model
                    .pg_domain_datatable(vertex_def_id.package_id(), vertex_def_id)?;

                let pg_id_field = pg.table.column(&id_prop_id)?;

                let Data::Sql(id_param) = self.data_from_value(value)? else {
                    return Err(ds_bad_req("compound foreign key"));
                };

                let stmt = match id_resolve_mode {
                    IdResolveMode::Plain => {
                        let stmt_key = (vertex_def_id, id_prop_id);
                        if let Some(stmt) = cache.key_by_id.get(&stmt_key).cloned() {
                            stmt
                        } else {
                            let stmt = sql::Select {
                                expressions: sql::Expressions {
                                    items: vec![sql::Expr::path1("_key")],
                                    multiline: false,
                                },
                                from: vec![pg.table_name().into()],
                                where_: Some(sql::Expr::eq(
                                    sql::Expr::path1(pg_id_field.col_name.as_ref()),
                                    sql::Expr::param(0),
                                )),
                                ..Default::default()
                            }
                            .to_string()
                            .prepare(self.client())
                            .await?;

                            cache.key_by_id.insert(stmt_key, stmt.clone());
                            stmt
                        }
                    }
                    IdResolveMode::SelfIdentifying => {
                        let stmt_key = vertex_def_id;
                        if let Some(stmt) = cache.upsert_self_identifying.get(&stmt_key).cloned() {
                            stmt
                        } else {
                            // upsert
                            // TODO: It might actually be better to do SELECT + optional INSERT.
                            // this approach (DO UPDATE SET) always re-appends the row.
                            let stmt = sql::Insert {
                                with: None,
                                into: pg.table_name(),
                                as_: None,
                                column_names: vec![&pg_id_field.col_name],
                                values: vec![sql::Expr::param(0)],
                                on_conflict: Some(sql::OnConflict {
                                    target: Some(sql::ConflictTarget::Columns(vec![
                                        &pg_id_field.col_name,
                                    ])),
                                    action: sql::ConflictAction::DoUpdateSet(vec![
                                        sql::UpdateColumn(
                                            &pg_id_field.col_name,
                                            sql::Expr::param(0),
                                        ),
                                    ]),
                                }),
                                returning: vec![sql::Expr::path1("_key")],
                            }
                            .to_string()
                            .prepare(self.client())
                            .await?;

                            cache.upsert_self_identifying.insert(stmt_key, stmt.clone());
                            stmt
                        }
                    }
                };

                debug!("{stmt}");
                trace!("resolve linked vertex {:?}", [&id_param]);

                let row = self
                    .client()
                    .query_opt(stmt.deref(), &[&id_param])
                    .await
                    .map_err(PgError::ForeignKeyLookup)?
                    .ok_or_else(|| {
                        let value = match self.deserialize_sql(def_id, id_param) {
                            Ok(value) => value,
                            Err(error) => return error,
                        };
                        DomainErrorKind::UnresolvedForeignKey(self.ontology.format_value(&value))
                            .into_error()
                    })?;

                Ok((vertex_def_id, pg.table.key, row.get(0)))
            }
        }
    }
}

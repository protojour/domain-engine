use std::collections::BTreeMap;

use domain_engine_core::{
    domain_error::DomainErrorKind, entity_id_utils::find_inherent_entity_id, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr, ontology::domain::DataRelationshipKind, property::ValueCardinality,
    query::select::Select, tuple::CardinalIdx, value::Value, DefId, EdgeId, RelId,
};
use postgres_types::ToSql;
use tracing::{debug, warn};

use crate::{
    ds_bad_req, ds_err,
    pg_model::{InDomain, PgDataKey, PgDomainTable, PgEdgeCardinalKind, PgRegKey},
    sql::{self, UpdateColumn},
    sql_value::SqlVal,
    transact::query::{QueryFrame, QuerySelect},
};

use super::{
    data::{Data, RowValue},
    edge_query::edge_join_condition,
    TransactCtx,
};

enum UpdateCondition {
    FieldEq(RelId, Value),
    #[allow(unused)]
    PgKey(PgDataKey),
}

impl<'a> TransactCtx<'a> {
    pub async fn update_vertex_with_select<'s>(
        &'s self,
        value: InDomain<Value>,
        select: &'s Select,
    ) -> DomainResult<Value> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        let entity_id = find_inherent_entity_id(&value, self.ontology)?
            .ok_or_else(|| DomainErrorKind::EntityNotFound.into_error())?;

        let key = self
            .update_datatable(
                value,
                UpdateCondition::FieldEq(entity.id_relationship_id, entity_id),
            )
            .await?;

        let query_select = match select {
            Select::Struct(sel) => QuerySelect::Struct(&sel.properties),
            Select::Leaf | Select::EntityId => QuerySelect::Field(entity.id_relationship_id),
            _ => todo!(),
        };

        let row = collect_one_row_value(
            self.query(def_id, false, 1, None, Some(key), Some(query_select))
                .await?,
        )
        .await?;

        Ok(row.value)
    }

    async fn update_datatable<'s>(
        &'s self,
        value: InDomain<Value>,
        update_condition: UpdateCondition,
    ) -> DomainResult<PgDataKey> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);

        let pg = self
            .pg_model
            .pg_domain_datatable(value.pkg_id, value.type_def_id())?;

        let mut where_: Option<sql::Expr> = None;
        let mut set: Vec<UpdateColumn> = vec![];

        let mut update_params: Vec<SqlVal> = vec![];

        match update_condition {
            UpdateCondition::FieldEq(rel_id, value) => {
                let pg_field = pg.table.field(&rel_id)?;
                where_ = Some(sql::Expr::eq(
                    sql::Expr::path1(pg_field.col_name.as_ref()),
                    sql::Expr::param(0),
                ));
                let Data::Sql(sql_value) = self.data_from_value(value)? else {
                    return Err(ds_bad_req("id must be a scalar"));
                };
                update_params.push(sql_value);
            }
            UpdateCondition::PgKey(key) => {
                where_ = Some(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
                update_params.push(SqlVal::I64(key));
            }
        }

        let Value::Struct(data_struct, _) = value.value else {
            return Err(
                DomainErrorKind::BadInputData("expected a struct".to_string()).into_error(),
            );
        };

        let mut edge_updates: FnvHashMap<RelId, Attr> = Default::default();

        for (rel_id, attr) in *data_struct {
            let rel_info = def
                .data_relationships
                .get(&rel_id)
                .ok_or_else(|| ds_bad_req("update: invalid data relationship"))?;

            match rel_info.kind {
                DataRelationshipKind::Id => {
                    warn!("ID should not be updated");
                }
                DataRelationshipKind::Tree => {
                    let pg_field = pg.table.field(&rel_id)?;
                    let param_index = update_params.len();

                    set.push(sql::UpdateColumn(
                        &pg_field.col_name,
                        sql::Expr::path1(sql::Param(param_index)),
                    ));
                    match attr {
                        Attr::Unit(val) => {
                            let Data::Sql(sql_val) = self.data_from_value(val)? else {
                                return Err(ds_err("TODO: compound update value"));
                            };
                            update_params.push(sql_val);
                        }
                        _ => todo!(),
                    }
                }
                DataRelationshipKind::Edge(_) => {
                    edge_updates.insert(rel_id, attr);
                }
            }
        }

        let key_rows = if !set.is_empty() {
            let sql = sql::Update {
                with: None,
                table_name: pg.table_name(),
                set,
                where_,
                returning: vec![sql::Expr::path1("_key")],
            }
            .to_string();
            debug!("{sql}");

            self.client()
                .query_raw(&sql, update_params.iter().map(|param| param as &dyn ToSql))
                .await
                .map_err(|e| ds_err(format!("unable to update data(1): {e:?}")))?
                .try_collect::<Vec<_>>()
                .map_err(|e| ds_err(format!("update problem: {e:?}")))
                .await?
        } else {
            // nothing inherent to update. Just fetch key.
            let sql = sql::Select {
                with: None,
                expressions: sql::Expressions {
                    items: vec![sql::Expr::path1("_key")],
                    multiline: false,
                },
                from: vec![pg.table_name().into()],
                where_,
                limit: sql::Limit {
                    limit: Some(1),
                    offset: None,
                },
            }
            .to_string();
            debug!("{sql}");

            self.client()
                .query_raw(&sql, update_params.iter().map(|param| param as &dyn ToSql))
                .await
                .map_err(|e| ds_err(format!("unable to select data for update: {e:?}")))?
                .try_collect::<Vec<_>>()
                .map_err(|e| ds_err(format!("update problem: {e:?}")))
                .await?
        };

        if key_rows.len() == 1 {
            let key = key_rows[0].get(0);
            self.update_edges(def_id, pg, key, edge_updates).await?;
            Ok(key)
        } else {
            Err(DomainErrorKind::EntityNotFound.into_error())
        }
    }

    async fn update_edges(
        &self,
        def_id: DefId,
        subject_pg: PgDomainTable<'a>,
        subject_key: PgDataKey,
        edge_updates: FnvHashMap<RelId, Attr>,
    ) -> DomainResult<()> {
        let def = self.ontology.def(def_id);

        let mut unit_edges: BTreeMap<EdgeId, BTreeMap<CardinalIdx, EdgeCardinalPatch>> =
            Default::default();
        let mut patches: BTreeMap<EdgeId, Vec<BTreeMap<CardinalIdx, EdgeCardinalPatch>>> =
            Default::default();
        let mut deletions: BTreeMap<(EdgeId, CardinalIdx), Vec<Value>> = Default::default();

        for (rel_id, attr) in edge_updates {
            let rel_info = def
                .data_relationships
                .get(&rel_id)
                .ok_or_else(|| ds_bad_req("update: invalid data relationship"))?;
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                unreachable!("already filtered for edges");
            };

            match (attr, rel_info.cardinality.1) {
                (Attr::Unit(unit), ValueCardinality::Unit) => {
                    unit_edges
                        .entry(proj.id)
                        .or_insert_with(|| {
                            BTreeMap::from_iter([(
                                proj.subject,
                                EdgeCardinalPatch::MatchKey(subject_pg.table.key, subject_key),
                            )])
                        })
                        .insert(proj.object, EdgeCardinalPatch::Update(unit));
                }
                (Attr::Matrix(matrix), ValueCardinality::IndexSet | ValueCardinality::List) => {
                    for tuple in matrix.into_rows() {
                        if tuple.elements.iter().any(|value| value.tag().is_delete()) {
                            let mut iter = tuple.elements.into_iter();
                            let foreign_id = iter.next().unwrap();

                            deletions
                                .entry((proj.id, proj.object))
                                .or_default()
                                .push(foreign_id);
                        } else {
                            let mut edge_tuple: BTreeMap<CardinalIdx, EdgeCardinalPatch> =
                                Default::default();

                            let mut cardinal_idx = CardinalIdx(0);

                            for value in tuple.elements {
                                if cardinal_idx == proj.subject {
                                    edge_tuple.insert(
                                        cardinal_idx,
                                        EdgeCardinalPatch::MatchKey(
                                            subject_pg.table.key,
                                            subject_key,
                                        ),
                                    );

                                    cardinal_idx.0 += 1;
                                }

                                edge_tuple.insert(
                                    cardinal_idx,
                                    if value.tag().is_update() {
                                        EdgeCardinalPatch::Update(value)
                                    } else {
                                        EdgeCardinalPatch::Insert(value)
                                    },
                                );

                                cardinal_idx.0 += 1;
                            }
                        }
                    }
                }
                (Attr::Unit(_unit), ValueCardinality::IndexSet | ValueCardinality::List) => {
                    return Err(ds_bad_req("invalid input for multi-relation write"));
                }
                _ => {
                    return Err(ds_bad_req("invalid combination for edge update"));
                }
            }
        }

        for (edge_id, tuple) in unit_edges {
            self.patch_edge(edge_id, subject_pg, subject_key, tuple)
                .await?;
        }

        for (edge_id, patches) in patches {
            for tuple in patches {
                self.patch_edge(edge_id, subject_pg, subject_key, tuple)
                    .await?;
            }
        }

        Ok(())
    }

    async fn patch_edge(
        &self,
        edge_id: EdgeId,
        subject_pg: PgDomainTable<'a>,
        subject_key: PgDataKey,
        tuple: BTreeMap<CardinalIdx, EdgeCardinalPatch>,
    ) -> DomainResult<()> {
        let pg_edge = self.pg_model.pg_domain_edgetable(&edge_id)?;
        let mut objects: Vec<EdgeCardinalPatch> = vec![];

        let mut edge_exprs: Vec<sql::Expr> = vec![];
        let mut params: Vec<SqlVal> = vec![];
        let mut subject_condition: Option<sql::Expr> = None;

        for (idx, patch) in tuple {
            let pg_cardinal = pg_edge.table.edge_cardinal(idx)?;

            match patch {
                EdgeCardinalPatch::MatchKey(def, key) => {
                    subject_condition = Some(edge_join_condition(
                        sql::Alias(0),
                        pg_cardinal,
                        subject_pg.table,
                        sql::Expr::param(0),
                    ));
                    params.push(SqlVal::I64(subject_key));
                }
                patch @ (EdgeCardinalPatch::Update(_) | EdgeCardinalPatch::Insert(_)) => {
                    match &pg_cardinal.kind {
                        PgEdgeCardinalKind::Dynamic {
                            def_col_name,
                            key_col_name,
                        } => {
                            edge_exprs.extend([
                                sql::Expr::path1(def_col_name.as_ref()),
                                sql::Expr::path1(key_col_name.as_ref()),
                            ]);
                        }
                        PgEdgeCardinalKind::Unique {
                            def_id,
                            key_col_name,
                        } => {
                            edge_exprs.push(sql::Expr::path1(key_col_name.as_ref()));
                        }
                        PgEdgeCardinalKind::Parameters(_) => todo!(),
                    }
                }
            }
        }

        let mut sql_select = sql::Select {
            with: None,
            expressions: sql::Expressions {
                items: edge_exprs,
                multiline: false,
            },
            from: vec![sql::FromItem::TableNameAs(
                pg_edge.table_name(),
                sql::Name::Alias(sql::Alias(0)),
            )],
            where_: subject_condition,
            limit: sql::Limit {
                limit: Some(1),
                offset: None,
            },
        };

        Ok(())
    }
}

enum EdgeCardinalPatch {
    MatchKey(PgRegKey, PgDataKey),
    Update(Value),
    Insert(Value),
}

async fn collect_one_row_value(
    frame_stream: impl Stream<Item = DomainResult<QueryFrame>>,
) -> DomainResult<RowValue> {
    let frames: Vec<_> = frame_stream.collect().await;
    let frame = frames
        .into_iter()
        .next()
        .ok_or_else(|| ds_err("expected frame from post-update query"))??;

    let QueryFrame::Row(row) = frame else {
        return Err(ds_err("expected Row"));
    };

    Ok(row)
}

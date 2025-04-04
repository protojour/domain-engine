use domain_engine_core::{
    DomainError, DomainResult, domain_error::DomainErrorKind,
    entity_id_utils::find_inherent_entity_id,
};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ontol_runtime::{
    PropId, attr::Attr, ontology::domain::DataRelationshipKind, query::select::Select,
    tuple::EndoTuple, value::Value,
};
use postgres_types::ToSql;
use tracing::{debug, trace, warn};

use crate::{
    pg_error::{PgError, PgInputError},
    pg_model::{EdgeId, InDomain, PgDataKey},
    sql::{self, UpdateColumn},
    sql_value::{PgTimestamp, SqlScalar},
    transact::query::QueryFrame,
};

use super::{
    MutationMode, TransactCtx,
    data::{Data, RowValue},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    mut_ctx::PgMutCtx,
    query::Query,
    query_select::QuerySelect,
};

pub enum UpdateCondition {
    FieldEq(PropId, Value),
    PgKey(PgDataKey),
}

impl TransactCtx<'_> {
    /// Note: updates are not cached statements currently.
    /// Unsure if it's possible to cache a generic update statement, since every update
    /// can touch different fields.
    pub async fn update_vertex_with_select<'s>(
        &'s self,
        value: InDomain<Value>,
        select: &'s Select,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<Value> {
        let domain_index = value.domain_index;
        let def_id = value.type_def_id();
        let def = self.ontology_defs.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        let entity_id = find_inherent_entity_id(&value, self.ontology_defs)?
            .ok_or_else(|| DomainErrorKind::EntityNotFound.into_error())?;

        mut_ctx.write_stats.mark_mutated(def_id);

        let key = self
            .update_datatable(
                value,
                UpdateCondition::FieldEq(entity.id_prop, entity_id),
                timestamp,
                mut_ctx,
            )
            .await?;

        let query_select = match select {
            select @ Select::Struct(_) => {
                let pg = self.pg_model.pg_domain_datatable(domain_index, def_id)?;
                let vertex_select = self.analyze_vertex_select(def, pg.table, select)?;
                QuerySelect::Vertex(vertex_select)
            }
            Select::Unit | Select::EntityId => QuerySelect::EntityId,
            _ => todo!(),
        };

        let row = collect_one_row_value(
            self.query(
                def_id,
                Query {
                    include_total_len: false,
                    limit: Some(1),
                    after_cursor: None,
                    native_id_condition: Some(key),
                },
                None,
                query_select,
                mut_ctx,
            )
            .await?,
        )
        .await?;

        Ok(row.value)
    }

    pub async fn update_datatable(
        &self,
        value: InDomain<Value>,
        update_condition: UpdateCondition,
        timestamp: PgTimestamp,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<PgDataKey> {
        let def_id = value.type_def_id();
        let def = self.ontology_defs.def(def_id);

        let pg = self
            .pg_model
            .pg_domain_datatable(value.domain_index, value.type_def_id())?;

        let where_: Option<sql::Expr>;
        let mut set: Vec<UpdateColumn> = vec![];

        let mut update_params: Vec<SqlScalar> = vec![];

        match update_condition {
            UpdateCondition::FieldEq(prop_id, value) => {
                let pg_column = pg.table.column(&prop_id)?;
                where_ = Some(sql::Expr::eq(
                    sql::Expr::path1(pg_column.col_name),
                    sql::Expr::param(0),
                ));
                let Data::Sql(sql_value) = self.data_from_value(value)? else {
                    return Err(PgInputError::IdMustBeScalar.into());
                };
                update_params.push(sql_value);
            }
            UpdateCondition::PgKey(key) => {
                where_ = Some(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
                update_params.push(SqlScalar::I64(key));
            }
        }

        set.push(sql::UpdateColumn("_updated", sql::Expr::param(1)));
        update_params.push(SqlScalar::Timestamp(timestamp));

        let Value::Struct(data_struct, _) = value.value else {
            return Err(PgInputError::VertexMustBeStruct.into());
        };

        let mut edge_patches = EdgePatches::default();

        for (prop_id, attr) in *data_struct {
            let rel_info = def
                .data_relationships
                .get(&prop_id)
                .ok_or(PgInputError::DataRelationshipNotFound(prop_id))?;

            match rel_info.kind {
                DataRelationshipKind::Id => {
                    trace!("skipping ID for update");
                }
                DataRelationshipKind::Tree(_) => {
                    let pg_column = pg.table.column(&prop_id)?;
                    let param_index = update_params.len();

                    set.push(sql::UpdateColumn(
                        pg_column.col_name,
                        sql::Expr::path1(sql::Param(param_index)),
                    ));
                    match attr {
                        Attr::Unit(val) => {
                            let Data::Sql(sql_val) = self.data_from_value(val)? else {
                                return Err(DomainError::data_store("TODO: compound update value"));
                            };
                            update_params.push(sql_val);
                        }
                        _ => todo!(),
                    }
                }
                DataRelationshipKind::Edge(proj) => {
                    let edge_patch = edge_patches.patch(EdgeId(proj.edge_id), proj.subject);

                    match attr {
                        Attr::Unit(value) => {
                            if edge_patch.tuples.is_empty() {
                                edge_patch
                                    .tuples
                                    .push(EdgeEndoTuplePatch { elements: vec![] });
                            }
                            edge_patch.tuples[0].insert_element(
                                proj.object,
                                value,
                                MutationMode::UpdateEdgeCardinal,
                            )?;
                        }
                        Attr::Tuple(tuple) => match analyze_update_tuple(*tuple, false) {
                            AnalyzedTuple::Patch(patch) => {
                                edge_patch.tuples.push(patch);
                            }
                            AnalyzedTuple::Delete(foreign_id) => {
                                edge_patch.delete(proj.object, foreign_id);
                            }
                        },
                        Attr::Matrix(matrix) => {
                            for tuple in matrix.into_rows() {
                                match analyze_update_tuple(tuple, true) {
                                    AnalyzedTuple::Patch(patch) => {
                                        edge_patch.tuples.push(patch);
                                    }
                                    AnalyzedTuple::Delete(foreign_id) => {
                                        edge_patch.delete(proj.object, foreign_id);
                                    }
                                }
                            }
                        }
                    }
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
            trace!("{update_params:?}");

            self.client()
                .query_raw(&sql, update_params.iter().map(|param| param as &dyn ToSql))
                .await
                .map_err(PgError::UpdateQuery)?
                .try_collect::<Vec<_>>()
                .map_err(PgError::UpdateRow)
                .await?
        } else {
            // nothing inherent to update. Just fetch key.
            let sql = sql::Select {
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
                ..Default::default()
            }
            .to_string();
            debug!("{sql}");

            self.client()
                .query_raw(&sql, update_params.iter().map(|param| param as &dyn ToSql))
                .await
                .map_err(PgError::UpdateQueryKeyFetch)?
                .try_collect::<Vec<_>>()
                .map_err(PgError::UpdateRow)
                .await?
        };

        if key_rows.len() == 1 {
            let key = key_rows[0].get(0);
            self.patch_edges(pg.table, key, edge_patches, timestamp, mut_ctx)
                .await?;

            Ok(key)
        } else {
            Err(DomainErrorKind::EntityNotFound.into_error())
        }
    }
}

enum AnalyzedTuple {
    Patch(EdgeEndoTuplePatch),
    Delete(Value),
}

fn analyze_update_tuple(tuple: EndoTuple<Value>, from_matrix: bool) -> AnalyzedTuple {
    if tuple.elements.iter().any(|value| value.tag().is_delete()) {
        let mut iter = tuple.elements.into_iter();
        let foreign_id = iter.next().unwrap();

        AnalyzedTuple::Delete(foreign_id)
    } else {
        AnalyzedTuple::Patch(EdgeEndoTuplePatch::from_tuple(
            tuple.elements.into_iter().map(|val| {
                let mutation_mode = if val.tag().is_update() {
                    if from_matrix {
                        MutationMode::Update
                    } else {
                        MutationMode::UpdateEdgeCardinal
                    }
                } else {
                    MutationMode::insert()
                };

                (val, mutation_mode)
            }),
        ))
    }
}

async fn collect_one_row_value(
    frame_stream: impl Stream<Item = DomainResult<QueryFrame>>,
) -> DomainResult<RowValue> {
    let frames: Vec<_> = frame_stream.collect().await;
    let frame = frames.into_iter().next().ok_or(PgError::NothingUpdated)??;

    let QueryFrame::Row(row) = frame else {
        return Err(PgError::InvalidQueryFrame.into());
    };

    Ok(row)
}

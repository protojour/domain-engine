use domain_engine_core::{
    domain_error::DomainErrorKind, entity_id_utils::find_inherent_entity_id, DomainError,
    DomainResult,
};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr, ontology::domain::DataRelationshipKind, query::select::Select, tuple::EndoTuple,
    value::Value, RelId,
};
use postgres_types::ToSql;
use tracing::{debug, trace, warn};

use crate::{
    pg_error::{PgError, PgInputError},
    pg_model::{InDomain, PgDataKey},
    sql::{self, UpdateColumn},
    sql_value::SqlVal,
    transact::query::{QueryFrame, QuerySelect},
};

use super::{
    data::{Data, RowValue},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    MutationMode, TransactCtx,
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

        let where_: Option<sql::Expr>;
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
                    return Err(PgInputError::IdMustBeScalar.into());
                };
                update_params.push(sql_value);
            }
            UpdateCondition::PgKey(key) => {
                where_ = Some(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
                update_params.push(SqlVal::I64(key));
            }
        }

        let Value::Struct(data_struct, _) = value.value else {
            return Err(PgInputError::VertexMustBeStruct.into());
        };

        let mut edge_patches = EdgePatches::default();

        for (rel_id, attr) in *data_struct {
            let rel_info = def
                .data_relationships
                .get(&rel_id)
                .ok_or(PgInputError::DataRelationshipNotFound(rel_id))?;

            match rel_info.kind {
                DataRelationshipKind::Id => {
                    trace!("skipping ID for update");
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
                                return Err(DomainError::data_store("TODO: compound update value"));
                            };
                            update_params.push(sql_val);
                        }
                        _ => todo!(),
                    }
                }
                DataRelationshipKind::Edge(proj) => {
                    let edge_patch = edge_patches.patch(proj.id, proj.subject);

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
                .map_err(PgError::UpdateQueryKeyFetch)?
                .try_collect::<Vec<_>>()
                .map_err(PgError::UpdateRow)
                .await?
        };

        if key_rows.len() == 1 {
            let key = key_rows[0].get(0);
            self.patch_edges(pg.table, key, edge_patches).await?;

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

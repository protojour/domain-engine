use domain_engine_core::{
    domain_error::DomainErrorKind, entity_id_utils::find_inherent_entity_id, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr, ontology::domain::DataRelationshipKind, property::ValueCardinality,
    query::select::Select, value::Value, DefId, RelId,
};
use postgres_types::ToSql;
use tracing::{debug, warn};

use crate::{
    ds_bad_req, ds_err,
    pg_model::{InDomain, PgDataKey},
    sql::{self, UpdateColumn},
    sql_value::SqlVal,
    transact::query::{QueryFrame, QuerySelect},
};

use super::{
    data::{Data, RowValue},
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
            self.update_edges(def_id, key, edge_updates).await?;
            Ok(key)
        } else {
            Err(DomainErrorKind::EntityNotFound.into_error())
        }
    }

    async fn update_edges(
        &self,
        def_id: DefId,
        subject_key: PgDataKey,
        edge_updates: FnvHashMap<RelId, Attr>,
    ) -> DomainResult<()> {
        let def = self.ontology.def(def_id);

        for (rel_id, attr) in edge_updates {
            let rel_info = def
                .data_relationships
                .get(&rel_id)
                .ok_or_else(|| ds_bad_req("update: invalid data relationship"))?;
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                unreachable!("already filtered for edges");
            };

            match (attr, rel_info.cardinality.1) {
                (Attr::Unit(unit), ValueCardinality::Unit) => {}
                (Attr::Matrix(matrix), ValueCardinality::IndexSet | ValueCardinality::List) => {}
                (Attr::Unit(_unit), ValueCardinality::IndexSet | ValueCardinality::List) => {
                    return Err(ds_bad_req("invalid input for multi-relation write"));
                }
                _ => {
                    return Err(ds_bad_req("invalid combination for edge update"));
                }
            }
        }

        Ok(())
    }
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

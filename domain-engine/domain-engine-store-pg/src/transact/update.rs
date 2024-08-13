use domain_engine_core::{
    domain_error::DomainErrorKind, entity_id_utils::find_inherent_entity_id, DomainResult,
};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr, ontology::domain::DataRelationshipKind, property::ValueCardinality,
    query::select::Select, value::Value, RelId,
};
use postgres_types::ToSql;
use tracing::{debug, warn};

use crate::{
    ds_bad_req, ds_err,
    pg_model::{InDomain, PgDataKey},
    sql,
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
            self.query(def_id, false, 1, None, Some(key), query_select)
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

        let mut sql_update = sql::Update {
            with: None,
            table_name: pg.table_name(),
            set: vec![],
            where_: None,
            returning: vec![sql::Expr::path1("_key")],
        };

        let mut update_params: Vec<SqlVal> = vec![];

        match update_condition {
            UpdateCondition::FieldEq(rel_id, value) => {
                let pg_field = pg.table.field(&rel_id)?;
                sql_update.where_ = Some(sql::Expr::eq(
                    sql::Expr::path1(pg_field.col_name.as_ref()),
                    sql::Expr::param(0),
                ));
                let Data::Sql(sql_value) = self.data_from_value(value)? else {
                    return Err(ds_bad_req("id must be a scalar"));
                };
                update_params.push(sql_value);
            }
            UpdateCondition::PgKey(key) => {
                sql_update.where_ =
                    Some(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
                update_params.push(SqlVal::I64(key));
            }
        }

        let Value::Struct(data_struct, _) = value.value else {
            return Err(
                DomainErrorKind::BadInputData("expected a struct".to_string()).into_error(),
            );
        };

        for (rel_id, attr) in *data_struct {
            let rel_info = def
                .data_relationships
                .get(&rel_id)
                .ok_or_else(|| ds_bad_req("update: invalid data relationship"))?;

            match (rel_info.kind, attr, rel_info.cardinality.1) {
                (DataRelationshipKind::Id, ..) => {
                    warn!("ID should not be updated");
                }
                (DataRelationshipKind::Tree, attr, _) => {
                    let pg_field = pg.table.field(&rel_id)?;
                    let param_index = update_params.len();

                    sql_update.set.push(sql::UpdateColumn(
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
                (
                    DataRelationshipKind::Edge(_projection),
                    Attr::Unit(_unit),
                    ValueCardinality::Unit,
                ) => {
                    todo!();
                }
                (
                    DataRelationshipKind::Edge(_projection),
                    Attr::Matrix(_matrix),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    todo!();
                }
                (
                    DataRelationshipKind::Edge(_projection),
                    Attr::Unit(_unit),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    return Err(ds_bad_req("invalid input for multi-relation write"));
                }
                _ => {
                    return Err(ds_bad_req("invalid combination for edge update"));
                }
            }
        }

        let sql = sql_update.to_string();
        debug!("{sql}");

        let rows = self
            .client()
            .query_raw(&sql, update_params.iter().map(|param| param as &dyn ToSql))
            .await
            .map_err(|e| ds_err(format!("unable to insert edge(1): {e:?}")))?
            .try_collect::<Vec<_>>()
            .map_err(|e| ds_err(format!("update problem: {e:?}")))
            .await?;

        if rows.len() == 1 {
            Ok(rows[0].get(0))
        } else {
            Err(DomainErrorKind::EntityNotFound.into_error())
        }
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

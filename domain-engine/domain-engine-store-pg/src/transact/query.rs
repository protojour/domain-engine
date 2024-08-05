use domain_engine_core::{transact::DataOperation, DomainError, DomainResult};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget},
    query::select::{EntitySelect, StructOrUnionSelect},
    value::Value,
    RelId,
};
use pin_utils::pin_mut;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{pg_model::PgSerial, sql, transact::data::Scalar};

use super::{data::RowValue, TransactCtx};

impl<'d, 't> TransactCtx<'d, 't> {
    pub async fn query(
        &self,
        entity_select: EntitySelect,
    ) -> DomainResult<impl Stream<Item = DomainResult<RowValue>> + '_> {
        debug!("query {entity_select:?}");

        let struct_select = match entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => struct_select,
            StructOrUnionSelect::Union(..) => {
                return Err(DomainError::data_store_bad_request(
                    "union top-level query not supported (yet)",
                ))
            }
        };

        let def_id = struct_select.def_id;

        let def = self.ontology.def(def_id);
        let pg_domain = self.pg_model.pg_domain(struct_select.def_id.package_id())?;
        let datatable = self.pg_model.datatable(def_id.package_id(), def_id)?;

        let mut sql = sql::Select {
            expressions: vec![sql::Expression::Column("_key")],
            from: vec![sql::TableName(&pg_domain.schema_name, &datatable.table_name).into()],
            limit: Some(entity_select.limit),
        };

        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let data_field = datatable.data_fields.get(&rel_id.tag()).unwrap();

                    sql.expressions
                        .push(sql::Expression::Column(&data_field.column_name));
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        let sql = sql.to_string();
        debug!("{sql}");

        let row_stream = self
            .txn
            .query_raw(&sql, slice_iter(&[]))
            .await
            .map_err(|err| DomainError::data_store(format!("{err}")))?;

        Ok(async_stream::try_stream! {
            pin_mut!(row_stream);

            for await row_result in row_stream {
                let row = row_result.map_err(|_| DomainError::data_store("unable to fetch row"))?;

                let key: PgSerial = row.get(0);

                let mut attrs: FnvHashMap<RelId, Attr> =
                    FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());
                let mut col_idx = 1;

                for (rel_id, rel) in &def.data_relationships {
                    match &rel.kind {
                        DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                            let scalar: Option<Scalar> = row.get(col_idx);
                            col_idx += 1;

                            if let Some(scalar) = scalar {
                                match rel.target {
                                    DataRelationshipTarget::Unambiguous(def_id) => {
                                        attrs.insert(
                                            *rel_id,
                                            Attr::Unit(self.deserialize_scalar(def_id, scalar)?),
                                        );
                                    }
                                    DataRelationshipTarget::Union(_) => {}
                                }
                            }
                        }
                        DataRelationshipKind::Edge(_) => {}
                    }
                }

                yield RowValue {
                    value: Value::Struct(Box::new(attrs), def.id.into()),
                    key,
                    op: DataOperation::Inserted,
                }
            }
        })
    }
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}

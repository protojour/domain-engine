use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{attr::Attr, query::select::Select, value::Value, RelId};
use pin_utils::pin_mut;
use tracing::{debug, trace, warn};

use crate::{
    ds_bad_req, ds_err, map_row_error,
    pg_model::{InDomain, PgType},
    sql::{self, TableName},
    sql_record::{SqlColumnStream, SqlRecordIterator},
    sql_value::Layout,
};

use super::{data::RowValue, TransactCtx};

pub enum InsertMode {
    Insert,
    Upsert,
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
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        if let Value::Struct(map, _) = &mut value.value {
            if let Entry::Vacant(vacant) = map.entry(entity.id_relationship_id) {
                match mode {
                    InsertMode::Insert => {
                        let value_generator = entity
                            .id_value_generator
                            .ok_or_else(|| ds_bad_req("no id provided and no ID generator"))?;

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

        let pkg_id = value.pkg_id;
        let pg_domain = self.pg_model.pg_domain(pkg_id)?;
        let analyzed = self.analyze_struct(value, def)?;

        // TODO: prepared statement for each entity type/select
        let mut insert = sql::Insert {
            into: TableName(
                &pg_domain.schema_name,
                &analyzed.root_attrs.datatable.table_name,
            ),
            column_names: analyzed.root_attrs.column_selection()?,
            on_conflict: None,
            returning: vec![sql::Expr::path1("_key")],
        };
        let mut layout: Vec<Layout> = vec![Layout::Scalar(PgType::BigInt)];

        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    insert
                        .returning
                        .push(sql::Expr::path1(field.col_name.as_ref()));
                    layout.push(Layout::Scalar(field.pg_type));
                }
            }
            Select::Struct(_sel) => {
                self.select_inherent_struct_fields(
                    def,
                    analyzed.root_attrs.datatable,
                    &mut insert.returning,
                    None,
                )?;
            }
            _ => {
                todo!()
            }
        }

        let row = {
            let sql = insert.to_string();
            debug!("{sql}");

            let stream = self
                .client()
                .query_raw(&sql, analyzed.root_attrs.as_params())
                .await
                .map_err(|err| ds_err(format!("{err}")))?;
            pin_mut!(stream);

            let row = stream
                .try_next()
                .await
                .map_err(map_row_error)?
                .ok_or_else(|| ds_err("no rows returned"))?;

            stream
                .try_next()
                .await
                .map_err(|_| ds_err("row stream not closed"))?;

            match stream.rows_affected() {
                Some(affected) => {
                    if affected != 1 {
                        return Err(ds_err("expected 1 affected row"));
                    }
                }
                None => {
                    return Err(ds_err("unknown problem"));
                }
            }

            row
        };

        let mut row = SqlColumnStream::new(&row);
        let data_key = row
            .next_field(&Layout::Scalar(PgType::BigInt))?
            .into_i64()?;

        self.patch_edges(
            analyzed.root_attrs.datatable,
            data_key,
            analyzed.edge_projections,
        )
        .await?;

        match select {
            Select::EntityId => {
                let sql_val = row.next_field(&layout[1])?.non_null()?;

                trace!("deserialized entity ID: {sql_val:?}");
                Ok(RowValue {
                    value: self.deserialize_sql(entity.id_value_def_id, sql_val)?,
                    data_key,
                    op: DataOperation::Inserted,
                })
            }
            Select::Struct(sel) => {
                let mut attrs: FnvHashMap<RelId, Attr> =
                    FnvHashMap::with_capacity_and_hasher(sel.properties.len(), Default::default());

                self.read_inherent_struct_fields(def, &mut row, &mut attrs)?;

                Ok(RowValue {
                    value: Value::Struct(Box::new(attrs), def.id.into()),
                    data_key,
                    op: DataOperation::Inserted,
                })
            }
            _ => Ok(RowValue {
                value: Value::unit(),
                data_key,
                op: DataOperation::Inserted,
            }),
        }
    }
}

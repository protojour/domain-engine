use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    object_generator::ObjectGenerator,
    transact::DataOperation,
    DomainError, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr, interface::serde::processor::ProcessorMode,
    ontology::domain::DataRelationshipTarget, query::select::Select, value::Value, DefId, RelId,
};
use pin_utils::pin_mut;
use tracing::{debug, trace, warn};

use crate::{
    pg_model::{InDomain, PgSerial},
    sql::{self, TableName},
    transact::data::Scalar,
};

use super::{data::RowValue, TransactCtx};

pub enum InsertMode {
    Insert,
    Upsert,
}

impl<'d, 't> TransactCtx<'d, 't> {
    /// Returns BoxFuture because of potential recursion
    pub fn insert_vertex<'a>(
        &'a self,
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

    async fn insert_vertex_impl<'a>(
        &'a self,
        mut value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
    ) -> DomainResult<RowValue> {
        ObjectGenerator::new(ProcessorMode::Create, self.ontology, self.system)
            .generate_objects(&mut value.value);

        let def = self.ontology.def(value.type_def_id());
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        if let Value::Struct(map, _) = &mut value.value {
            if let Entry::Vacant(vacant) = map.entry(entity.id_relationship_id) {
                match mode {
                    InsertMode::Insert => {
                        let value_generator = entity.id_value_generator.ok_or_else(|| {
                            DomainError::data_store_bad_request(
                                "no id provided and no ID generator",
                            )
                        })?;

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
            table_name: TableName(
                &pg_domain.schema_name,
                &analyzed.root_attrs.datatable.table_name,
            ),
            column_names: analyzed.root_attrs.column_selection()?,
            returning: vec!["_key"],
        };

        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    insert.returning.push(&field.column_name);
                }
            }
            Select::Struct(sel) => {
                for rel_id in sel.properties.keys() {
                    if let Some(field) =
                        analyzed.root_attrs.datatable.data_fields.get(&rel_id.tag())
                    {
                        insert.returning.push(&field.column_name);
                    }
                }
            }
            _ => {
                todo!()
            }
        }

        let row = {
            let sql = insert.to_string();
            debug!("{sql}");

            let stream = self
                .txn
                .query_raw(&sql, analyzed.root_attrs.as_params())
                .await
                .map_err(|err| DomainError::data_store(format!("{err}")))?;
            pin_mut!(stream);

            stream
                .try_next()
                .await
                .map_err(|_| DomainError::data_store("could not fetch row"))?
                .ok_or_else(|| DomainError::data_store("no rows returned"))?
        };

        let key: PgSerial = row.get(0);

        // write edges
        for (_edge_id, projection) in analyzed.edge_projections {
            for tuple in projection.tuples {
                for value in tuple {
                    self.resolve_linked_vertex(value, InsertMode::Insert, Select::EntityId)
                        .await?;
                }
            }
        }

        match select {
            Select::EntityId => {
                let scalar: Scalar = row.get(1);
                trace!("deserialized entity ID: {scalar:?}");
                Ok(RowValue {
                    value: self.deserialize_scalar(entity.id_value_def_id, scalar)?,
                    key,
                    op: DataOperation::Inserted,
                })
            }
            Select::Struct(sel) => {
                let mut attrs: FnvHashMap<RelId, Attr> =
                    FnvHashMap::with_capacity_and_hasher(sel.properties.len(), Default::default());

                for (idx, rel_id) in sel.properties.keys().enumerate() {
                    let scalar: Scalar = row.get(idx + 1);
                    let data_relationship = def.data_relationships.get(rel_id).unwrap();

                    match data_relationship.target {
                        DataRelationshipTarget::Unambiguous(def_id) => {
                            attrs.insert(
                                *rel_id,
                                Attr::Unit(self.deserialize_scalar(def_id, scalar)?),
                            );
                        }
                        DataRelationshipTarget::Union(_) => {}
                    }
                }

                Ok(RowValue {
                    value: Value::Struct(Box::new(attrs), def.id.into()),
                    key,
                    op: DataOperation::Inserted,
                })
            }
            _ => Ok(RowValue {
                value: Value::unit(),
                key,
                op: DataOperation::Inserted,
            }),
        }
    }

    async fn resolve_linked_vertex(
        &self,
        value: Value,
        mode: InsertMode,
        select: Select,
    ) -> DomainResult<(DefId, PgSerial)> {
        let def_id = value.type_def_id();

        if self
            .pg_model
            .find_datatable(def_id.package_id(), def_id)
            .is_some()
        {
            let row_value = self
                .insert_vertex(
                    InDomain {
                        pkg_id: def_id.package_id(),
                        value,
                    },
                    mode,
                    &select,
                )
                .await?;

            Ok((def_id, row_value.key))
        } else {
            // TODO: find reference
            Ok((def_id, 1337))
        }
    }
}

use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    object_generator::ObjectGenerator,
    transact::DataOperation,
    DomainError, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::TryStreamExt;
use ontol_runtime::{
    attr::Attr,
    interface::serde::processor::ProcessorMode,
    ontology::domain::{DataRelationshipTarget, DefKind, DefRepr},
    query::select::Select,
    value::Value,
    DefId, RelId,
};
use pin_utils::pin_mut;
use tracing::debug;

use crate::{
    pg_model::{InDomain, PgSerial},
    sql::Insert,
    transact::data::Scalar,
};

use super::TransactCtx;

pub enum InsertMode {
    Insert,
    Upsert,
}

impl<'d, 't> TransactCtx<'d, 't> {
    pub async fn insert_entity(
        &self,
        mut value: InDomain<Value>,
        mode: InsertMode,
        select: &Select,
    ) -> DomainResult<(Value, DataOperation)> {
        ObjectGenerator::new(ProcessorMode::Create, self.ontology, self.system)
            .generate_objects(&mut value.value);

        let def = self.ontology.def(value.type_def_id());
        let entity = def
            .entity()
            .ok_or(DomainErrorKind::NotAnEntity(value.type_def_id()).into_error())?;

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
        let analyzed = self.analyze_struct(value, def)?;
        let pg_domain = self.pg_model.find_pg_domain(pkg_id)?;

        // TODO: prepared statement for each entity type/select
        let mut sql = Insert {
            schema: &pg_domain.schema_name,
            table: &analyzed.root_attrs.datatable.table_name,
            columns: analyzed.root_attrs.column_selection()?,
            returning: vec!["_key"],
        };

        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    sql.returning.push(&field.column_name);
                }
            }
            Select::Struct(sel) => {
                for rel_id in sel.properties.keys() {
                    if let Some(field) =
                        analyzed.root_attrs.datatable.data_fields.get(&rel_id.tag())
                    {
                        sql.returning.push(&field.column_name);
                    }
                }
            }
            _ => {
                todo!()
            }
        }

        let sql = sql.to_string();
        debug!("{sql}");

        let stream = self
            .txn
            .query_raw(&sql, analyzed.root_attrs.as_params())
            .await
            .map_err(|err| DomainError::data_store(format!("{err}")))?;
        pin_mut!(stream);

        let row = stream
            .try_next()
            .await
            .map_err(|_| DomainError::data_store("could not fetch row"))?
            .ok_or_else(|| DomainError::data_store("no rows returned"))?;

        let _key: PgSerial = row.get(0);

        match select {
            Select::EntityId => {
                let scalar: Scalar = row.get(1);
                debug!("deserialized entity ID: {scalar:?}");
                Ok((
                    self.deserialize_scalar(entity.id_value_def_id, scalar)?,
                    DataOperation::Inserted,
                ))
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

                Ok((
                    Value::Struct(Box::new(attrs), def.id.into()),
                    DataOperation::Inserted,
                ))
            }
            _ => Ok((Value::unit(), DataOperation::Inserted)),
        }
    }

    fn deserialize_scalar(&self, def_id: DefId, scalar: Scalar) -> DomainResult<Value> {
        debug!("pg deserialize scalar {def_id:?}");

        match &self.ontology.def(def_id).kind {
            DefKind::Data(basic) => match basic.repr {
                DefRepr::FmtStruct(Some((attr_rel_id, attr_def_id))) => Ok(Value::Struct(
                    Box::new(
                        [(
                            attr_rel_id,
                            Attr::Unit(scalar.into_value(attr_def_id.into())),
                        )]
                        .into_iter()
                        .collect(),
                    ),
                    def_id.into(),
                )),
                DefRepr::FmtStruct(None) => {
                    unreachable!("tried to deserialize an empty FmtStruct (has no data)")
                }
                _ => Ok(scalar.into_value(def_id.into())),
            },
            _ => Err(DomainError::data_store(
                "unrecognized DefKind for PG scalar deserialization",
            )),
        }
    }
}

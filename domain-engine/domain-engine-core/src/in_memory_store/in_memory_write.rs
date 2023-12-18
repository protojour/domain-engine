use std::collections::BTreeMap;

use anyhow::anyhow;
use ontol_runtime::{
    condition::Condition,
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{
        DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget, EntityInfo, Ontology,
        ValueCardinality,
    },
    select::Select,
    smart_format,
    value::{Attribute, Data, PropertyId, Value, ValueDebug},
    value_generator::ValueGenerator,
    DefId, Role,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    in_memory_store::{
        in_memory_core::{Edge, EntityKey},
        in_memory_query::{Cursor, IncludeTotalLen, Limit},
    },
    DomainEngine, DomainError, DomainResult,
};

use super::in_memory_core::{DynamicKey, InMemoryStore};

struct Overwrite(bool);

impl InMemoryStore {
    pub fn write_new_entity(
        &mut self,
        entity: Value,
        select: &Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        debug!("write new entity: {}", ValueDebug(&entity));

        let entity_id = self.write_new_entity_inner(entity, engine)?;
        self.post_write_select(entity_id, select, engine)
    }

    pub fn update_entity(
        &mut self,
        data: Value,
        select: &Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        let entity_id = find_inherent_entity_id(engine.ontology(), &data)?
            .ok_or_else(|| DomainError::EntityNotFound)?;
        let type_info = engine.ontology().get_type_info(data.type_def_id);
        let dynamic_key = Self::extract_dynamic_key(&entity_id.data)?;

        if !self
            .collections
            .get(&type_info.def_id)
            .unwrap()
            .contains_key(&dynamic_key)
        {
            return Err(DomainError::EntityNotFound);
        }

        let mut raw_props_update: BTreeMap<PropertyId, Attribute> = Default::default();

        let Data::Struct(data_struct) = data.data else {
            return Err(DomainError::BadInput(anyhow!("Expected a struct")));
        };
        for (property_id, attribute) in data_struct {
            if let Some(data_relationship) = type_info.data_relationships.get(&property_id) {
                match data_relationship.kind {
                    DataRelationshipKind::Tree => {
                        raw_props_update.insert(property_id, attribute);
                    }
                    DataRelationshipKind::EntityGraph { .. } => {
                        match data_relationship.cardinality.1 {
                            ValueCardinality::One => {
                                self.insert_entity_relationship(
                                    type_info.def_id,
                                    &dynamic_key,
                                    (property_id, Overwrite(true)),
                                    attribute,
                                    data_relationship,
                                    engine,
                                )?;
                            }
                            ValueCardinality::Many => {
                                return Err(DomainError::DataStore(anyhow!(
                                    "Multi-relation update not yet implemented"
                                )));
                            }
                        }
                    }
                }
            } else {
                panic!("Unknown relationship for {property_id:?}");
            }
        }

        let collection = self.collections.get_mut(&type_info.def_id).unwrap();
        let raw_props = collection.get_mut(&dynamic_key).unwrap();

        for (property_id, attr) in raw_props_update {
            raw_props.insert(property_id, attr);
        }

        self.post_write_select(entity_id, select, engine)
    }

    fn post_write_select(
        &mut self,
        entity_id: Value,
        select: &Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        match select {
            Select::EntityId => Ok(entity_id),
            Select::Struct(struct_select) => {
                let target_dynamic_key = Self::extract_dynamic_key(&entity_id.data)?;
                let entity_seq = self.query_single_entity_collection(
                    struct_select,
                    &Condition::default(),
                    Limit(usize::MAX),
                    Option::<Cursor>::None,
                    IncludeTotalLen(false),
                    engine,
                )?;

                for attr in entity_seq.attrs {
                    let id = find_inherent_entity_id(engine.ontology(), &attr.value)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id.data)?;

                        if dynamic_key == target_dynamic_key {
                            return Ok(attr.value);
                        }
                    }
                }

                panic!("Not found")
            }
            _ => todo!(),
        }
    }

    /// Returns the entity ID
    fn write_new_entity_inner(
        &mut self,
        entity: Value,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        debug!("write entity {}", ValueDebug(&entity));

        let ontology = engine.ontology();
        let type_info = ontology.get_type_info(entity.type_def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(entity.type_def_id))?;

        let (id, id_generated) = match find_inherent_entity_id(ontology, &entity)? {
            Some(id) => (id, false),
            None => {
                if let Some(value_generator) = entity_info.id_value_generator {
                    let id = self.generate_entity_id(
                        ontology,
                        entity_info.id_operator_addr,
                        value_generator,
                    )?;
                    (id, true)
                } else {
                    panic!("No id provided and no ID generator");
                }
            }
        };

        debug!("write entity_id={}", ValueDebug(&id));

        let mut struct_map = match entity.data {
            Data::Struct(struct_map) => struct_map,
            _ => return Err(DomainError::EntityMustBeStruct),
        };

        if id_generated {
            struct_map.insert(
                PropertyId::subject(entity_info.id_relationship_id),
                id.clone().into(),
            );
        }

        let mut raw_props: BTreeMap<PropertyId, Attribute> = Default::default();

        let entity_key = Self::extract_dynamic_key(&id.data)?;

        for (property_id, attribute) in struct_map {
            if let Some(data_relationship) = type_info.data_relationships.get(&property_id) {
                match data_relationship.kind {
                    DataRelationshipKind::Tree => {
                        raw_props.insert(property_id, attribute);
                    }
                    DataRelationshipKind::EntityGraph { .. } => {
                        match data_relationship.cardinality.1 {
                            ValueCardinality::One => {
                                self.insert_entity_relationship(
                                    type_info.def_id,
                                    &entity_key,
                                    (property_id, Overwrite(false)),
                                    attribute,
                                    data_relationship,
                                    engine,
                                )?;
                            }
                            ValueCardinality::Many => {
                                let seq = match attribute.value.data {
                                    Data::Sequence(seq) => seq,
                                    _ => panic!("Expected sequence for ValueCardinality::Many"),
                                };

                                for attribute in seq.attrs {
                                    self.insert_entity_relationship(
                                        type_info.def_id,
                                        &entity_key,
                                        (property_id, Overwrite(false)),
                                        attribute,
                                        data_relationship,
                                        engine,
                                    )?;
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("Unknown relationship for {property_id:?}");
            }
        }

        let collection = self.collections.get_mut(&entity.type_def_id).unwrap();

        if collection.contains_key(&entity_key) {
            return Err(DomainError::EntityAlreadyExists);
        }

        collection.insert(entity_key, raw_props);

        Ok(id)
    }

    fn insert_entity_relationship(
        &mut self,
        entity_def_id: DefId,
        entity_key: &DynamicKey,
        (property_id, overwrite): (PropertyId, Overwrite),
        attribute: Attribute,
        data_relationship: &DataRelationshipInfo,
        engine: &DomainEngine,
    ) -> DomainResult<()> {
        debug!("entity rel attribute: {attribute:?}. Data relationship: {data_relationship:?}");

        let ontology = engine.ontology();
        let value = attribute.value;
        let rel_params = attribute.rel_params;

        let foreign_key = match &data_relationship.target {
            DataRelationshipTarget::Unambiguous(entity_def_id) => {
                if &value.type_def_id == entity_def_id {
                    let foreign_id = self.write_new_entity_inner(value, engine)?;
                    let dynamic_key = Self::extract_dynamic_key(&foreign_id.data)?;

                    EntityKey {
                        type_def_id: *entity_def_id,
                        dynamic_key,
                    }
                } else {
                    self.resolve_foreign_key_for_edge(
                        *entity_def_id,
                        ontology
                            .get_type_info(*entity_def_id)
                            .entity_info
                            .as_ref()
                            .unwrap(),
                        value,
                        engine,
                    )?
                }
            }
            DataRelationshipTarget::Union {
                union_def_id: _,
                variants,
            } => {
                if variants.contains(&value.type_def_id) {
                    // Explicit data struct of a given variant
                    let entity_def_id = value.type_def_id;
                    let foreign_id = self.write_new_entity_inner(value, engine)?;
                    let dynamic_key = Self::extract_dynamic_key(&foreign_id.data)?;

                    EntityKey {
                        type_def_id: entity_def_id,
                        dynamic_key,
                    }
                } else {
                    let (variant_def_id, entity_info) = variants
                        .iter()
                        .find_map(|variant_def_id| {
                            let entity_info = ontology
                                .get_type_info(*variant_def_id)
                                .entity_info
                                .as_ref()
                                .unwrap();

                            if entity_info.id_value_def_id == value.type_def_id {
                                Some((*variant_def_id, entity_info))
                            } else {
                                None
                            }
                        })
                        .expect("Corresponding entity def id not found for the given ID");

                    self.resolve_foreign_key_for_edge(variant_def_id, entity_info, value, engine)?
                }
            }
        };

        let edge_collection = self
            .edge_collections
            .get_mut(&property_id.relationship_id)
            .expect("No edge collection");

        let local_key = EntityKey {
            type_def_id: entity_def_id,
            dynamic_key: entity_key.clone(),
        };

        match property_id.role {
            Role::Subject => {
                if overwrite.0 {
                    edge_collection.edges.retain(|edge| edge.from != local_key);
                }

                edge_collection.edges.push(Edge {
                    from: local_key,
                    to: foreign_key,
                    params: rel_params,
                });
            }
            Role::Object => {
                if overwrite.0 {
                    edge_collection.edges.retain(|edge| edge.to != local_key);
                }

                edge_collection.edges.push(Edge {
                    from: foreign_key,
                    to: local_key,
                    params: rel_params,
                });
            }
        }

        Ok(())
    }

    /// This is for creating a relationship to an existing entity, using only a "foreign key".
    fn resolve_foreign_key_for_edge(
        &mut self,
        foreign_entity_def_id: DefId,
        entity_info: &EntityInfo,
        id_value: Value,
        engine: &DomainEngine,
    ) -> DomainResult<EntityKey> {
        let foreign_key = Self::extract_dynamic_key(&id_value.data)?;
        let entity_data = self.look_up_entity(foreign_entity_def_id, &foreign_key);

        if entity_data.is_none() && entity_info.is_self_identifying {
            // This type has UPSERT semantics.
            // Synthesize the entity, write it and move on..

            let entity_data = BTreeMap::from([(
                PropertyId::subject(entity_info.id_relationship_id),
                Attribute::from(id_value),
            )]);
            self.write_new_entity_inner(
                Value::new(Data::Struct(entity_data), foreign_entity_def_id),
                engine,
            )?;
        } else if entity_data.is_none() {
            let ontology = engine.ontology();
            let type_info = ontology.get_type_info(id_value.type_def_id);
            let repr = if let Some(operator_addr) = type_info.operator_addr {
                // TODO: Easier way to report values in "human readable"/JSON format

                let processor = ontology.new_serde_processor(operator_addr, ProcessorMode::Read);

                let mut buf: Vec<u8> = vec![];
                processor
                    .serialize_value(&id_value, None, &mut serde_json::Serializer::new(&mut buf))
                    .unwrap();
                String::from(std::str::from_utf8(&buf).unwrap())
            } else {
                smart_format!("N/A")
            };

            return Err(DomainError::UnresolvedForeignKey(repr));
        }

        Ok(EntityKey {
            type_def_id: foreign_entity_def_id,
            dynamic_key: foreign_key,
        })
    }

    fn generate_entity_id(
        &mut self,
        ontology: &Ontology,
        id_operator_addr: SerdeOperatorAddr,
        value_generator: ValueGenerator,
    ) -> DomainResult<Value> {
        match try_generate_entity_id(ontology, id_operator_addr, value_generator)? {
            GeneratedId::Generated(value) => Ok(value),
            GeneratedId::AutoIncrementI64(def_id) => {
                let id_value = self.int_id_counter;
                self.int_id_counter += 1;
                Ok(Value::new(Data::I64(id_value), def_id))
            }
        }
    }
}

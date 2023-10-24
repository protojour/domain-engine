use std::collections::BTreeMap;

use ontol_runtime::{
    condition::Condition,
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{DataRelationshipInfo, DataRelationshipKind, Ontology, ValueCardinality},
    select::{EntitySelect, Select, StructOrUnionSelect},
    smart_format,
    value::{Attribute, Data, PropertyId, Value, ValueDebug},
    value_generator::ValueGenerator,
    DefId, Role,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    in_memory_store::in_memory_core::{Edge, EntityKey},
    DomainEngine, DomainError, DomainResult,
};

use super::in_memory_core::{DynamicKey, InMemoryStore};

impl InMemoryStore {
    pub fn write_new_entity(
        &mut self,
        entity: Value,
        select: Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        let entity_id = self.write_new_entity_inner(entity, engine)?;

        match select {
            Select::EntityId => Ok(entity_id),
            Select::Struct(struct_select) => {
                let target_dynamic_key = Self::extract_dynamic_key(&entity_id.data)?;
                let entity_seq = self.query_entities(
                    &EntitySelect {
                        source: StructOrUnionSelect::Struct(struct_select),
                        condition: Condition::default(),
                        limit: usize::MAX,
                        cursor: None,
                    },
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
                                    property_id,
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
                                        property_id,
                                        attribute,
                                        data_relationship,
                                        engine,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let collection = self.collections.get_mut(&entity.type_def_id).unwrap();
        collection.insert(entity_key, raw_props);

        Ok(id)
    }

    fn insert_entity_relationship(
        &mut self,
        entity_def_id: DefId,
        entity_key: &DynamicKey,
        property_id: PropertyId,
        attribute: Attribute,
        data_relationship: &DataRelationshipInfo,
        engine: &DomainEngine,
    ) -> DomainResult<()> {
        debug!("entity rel attribute: {attribute:?}");

        let ontology = engine.ontology();
        let value = attribute.value;
        let rel_params = attribute.rel_params;

        let foreign_key = if value.type_def_id == data_relationship.target {
            let def_id = value.type_def_id;
            let foreign_id = self.write_new_entity_inner(value, engine)?;
            let dynamic_key = Self::extract_dynamic_key(&foreign_id.data)?;

            EntityKey {
                type_def_id: def_id,
                dynamic_key,
            }
        } else {
            // This is for creating a relationship to an existing entity,
            // using only a primary key.

            let type_info = ontology.get_type_info(data_relationship.target);
            let entity_info = type_info.entity_info.as_ref().unwrap();

            let foreign_key = Self::extract_dynamic_key(&value.data)?;
            let entity_data = self.look_up_entity(data_relationship.target, &foreign_key);

            if entity_data.is_none() && entity_info.is_self_identifying {
                // This type has UPSERT semantics.
                // Synthesize the entity, write it and move on..

                let entity_data = BTreeMap::from([(
                    PropertyId::subject(entity_info.id_relationship_id),
                    Attribute::from(value),
                )]);
                self.write_new_entity_inner(
                    Value::new(Data::Struct(entity_data), data_relationship.target),
                    engine,
                )?;
            } else if entity_data.is_none() {
                let type_info = ontology.get_type_info(value.type_def_id);
                let repr = if let Some(operator_addr) = type_info.operator_addr {
                    // TODO: Easier way to report values in "human readable"/JSON format

                    let processor =
                        ontology.new_serde_processor(operator_addr, ProcessorMode::Read);

                    let mut buf: Vec<u8> = vec![];
                    processor
                        .serialize_value(&value, None, &mut serde_json::Serializer::new(&mut buf))
                        .unwrap();
                    String::from(std::str::from_utf8(&buf).unwrap())
                } else {
                    smart_format!("N/A")
                };

                return Err(DomainError::UnresolvedForeignKey(repr));
            }

            EntityKey {
                type_def_id: type_info.def_id,
                dynamic_key: foreign_key,
            }
        };

        let edge_collection = self
            .edge_collections
            .get_mut(&property_id.relationship_id)
            .expect("No edge collection");

        match property_id.role {
            Role::Subject => {
                edge_collection.edges.push(Edge {
                    from: EntityKey {
                        type_def_id: entity_def_id,
                        dynamic_key: entity_key.clone(),
                    },
                    to: foreign_key,
                    params: rel_params,
                });
            }
            Role::Object => {
                edge_collection.edges.push(Edge {
                    from: foreign_key,
                    to: EntityKey {
                        type_def_id: entity_def_id,
                        dynamic_key: entity_key.clone(),
                    },
                    params: rel_params,
                });
            }
        }

        Ok(())
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

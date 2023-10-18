use std::collections::BTreeMap;

use ontol_runtime::{
    condition::Condition,
    interface::serde::{
        operator::{AliasOperator, SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::{DataRelationshipInfo, DataRelationshipKind, Ontology, ValueCardinality},
    select::{EntitySelect, Select, StructOrUnionSelect},
    smart_format,
    text_like_types::TextLikeType,
    value::{Attribute, Data, PropertyId, Value, ValueDebug},
    value_generator::ValueGenerator,
    Role,
};
use smartstring::alias::String;
use tracing::debug;
use uuid::Uuid;

use crate::{
    entity_id_utils::{analyze_text_pattern, find_inherent_entity_id},
    in_memory_store::in_memory_core::Edge,
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
                let entities = self.query_entities(
                    &EntitySelect {
                        source: StructOrUnionSelect::Struct(struct_select),
                        condition: Condition::default(),
                        limit: u32::MAX,
                        cursor: None,
                    },
                    engine,
                )?;

                for entity in entities {
                    let id = find_inherent_entity_id(engine.ontology(), &entity)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id.data)?;

                        if dynamic_key == target_dynamic_key {
                            return Ok(entity);
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
                        entity_info.id_operator_id,
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
                                    &entity_key,
                                    property_id,
                                    attribute,
                                    data_relationship,
                                    engine,
                                )?;
                            }
                            ValueCardinality::Many => {
                                let attributes = match attribute.value.data {
                                    Data::Sequence(attributes) => attributes,
                                    _ => panic!("Expected sequence for ValueCardinality::Many"),
                                };

                                for attribute in attributes {
                                    self.insert_entity_relationship(
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
            let foreign_id = self.write_new_entity_inner(value, engine)?;
            Self::extract_dynamic_key(&foreign_id.data)?
        } else {
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
                let repr = if let Some(operator_id) = type_info.operator_id {
                    // TODO: Easier way to report values in "human readable"/JSON format

                    let processor = ontology.new_serde_processor(
                        operator_id,
                        ProcessorMode::Read,
                        ProcessorLevel::new_root(),
                    );

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

            foreign_key
        };

        let edge_collection = self
            .edge_collections
            .get_mut(&property_id.relationship_id)
            .expect("No edge collection");

        match property_id.role {
            Role::Subject => {
                edge_collection.edges.push(Edge {
                    from: entity_key.clone(),
                    to: foreign_key,
                    params: rel_params,
                });
            }
            Role::Object => {
                edge_collection.edges.push(Edge {
                    from: foreign_key,
                    to: entity_key.clone(),
                    params: rel_params,
                });
            }
        }

        Ok(())
    }

    fn generate_entity_id(
        &mut self,
        ontology: &Ontology,
        id_operator_id: SerdeOperatorId,
        value_generator: ValueGenerator,
    ) -> DomainResult<Value> {
        match (ontology.get_serde_operator(id_operator_id), value_generator) {
            (SerdeOperator::String(def_id), ValueGenerator::UuidV4) => {
                let string = smart_format!("{}", Uuid::new_v4());
                Ok(Value::new(Data::Text(string), *def_id))
            }
            (SerdeOperator::TextPattern(def_id), _) => {
                match (ontology.get_text_like_type(*def_id), value_generator) {
                    (Some(TextLikeType::Uuid), ValueGenerator::UuidV4) => Ok(Value::new(
                        Data::OctetSequence(Uuid::new_v4().as_bytes().iter().cloned().collect()),
                        *def_id,
                    )),
                    _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
                }
            }
            (SerdeOperator::CapturingTextPattern(def_id), _) => {
                if let Some(property) =
                    analyze_text_pattern(ontology.get_text_pattern(*def_id).unwrap())
                {
                    let type_info = ontology.get_type_info(property.type_def_id);
                    let id = self.generate_entity_id(
                        ontology,
                        type_info.operator_id.unwrap(),
                        value_generator,
                    )?;

                    Ok(Value::new(
                        Data::Struct(BTreeMap::from([(property.property_id, id.into())])),
                        *def_id,
                    ))
                } else {
                    Err(DomainError::TypeCannotBeUsedForIdGeneration)
                }
            }
            (SerdeOperator::I64(def_id, _), ValueGenerator::Autoincrement) => {
                let id_value = self.int_id_counter;
                self.int_id_counter += 1;
                Ok(Value::new(Data::I64(id_value), *def_id))
            }
            (
                SerdeOperator::Alias(AliasOperator {
                    def,
                    inner_operator_id,
                    ..
                }),
                _,
            ) => {
                let mut value =
                    self.generate_entity_id(ontology, *inner_operator_id, value_generator)?;
                value.type_def_id = def.def_id;
                Ok(value)
            }
            _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
        }
    }
}

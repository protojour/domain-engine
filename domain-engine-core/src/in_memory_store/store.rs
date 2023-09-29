use std::collections::BTreeMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    interface::serde::{
        operator::{AliasOperator, SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::{
        DataRelationshipInfo, DataRelationshipKind, Ontology, PropertyCardinality, TypeInfo,
        ValueCardinality,
    },
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    smart_format,
    text_like_types::TextLikeType,
    value::{Attribute, Data, PropertyId, Value, ValueDebug},
    value_generator::ValueGenerator,
    DefId, RelationshipId, Role,
};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    entity_id_utils::{analyze_text_pattern, find_inherent_entity_id},
    DomainEngine, DomainError, DomainResult,
};

#[derive(Debug)]
pub struct InMemoryStore {
    pub collections: FnvHashMap<DefId, EntityTable<DynamicKey>>,
    #[allow(unused)]
    pub edge_collections: FnvHashMap<RelationshipId, EdgeCollection>,
    pub int_id_counter: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DynamicKey {
    Text(String),
    Octets(SmallVec<[u8; 16]>),
    Int(i64),
}

pub type EntityTable<K> = IndexMap<K, BTreeMap<PropertyId, Attribute>>;

#[derive(Debug)]
pub struct EdgeCollection {
    pub edges: Vec<Edge>,
}

#[derive(Debug)]
#[allow(unused)]
pub struct Edge {
    from: DynamicKey,
    to: DynamicKey,
    params: Value,
}

impl InMemoryStore {
    pub fn query_entities(
        &self,
        engine: &DomainEngine,
        select: &EntitySelect,
    ) -> DomainResult<Vec<Value>> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                self.query_single_entity_collection(engine, struct_select)
            }
            StructOrUnionSelect::Union(..) => todo!(),
        }
    }

    fn query_single_entity_collection(
        &self,
        engine: &DomainEngine,
        struct_select: &StructSelect,
    ) -> DomainResult<Vec<Value>> {
        debug!("query single entity collection: {struct_select:?}");
        let collection = self
            .collections
            .get(&struct_select.def_id)
            .ok_or(DomainError::InvalidEntityDefId)?;

        let type_info = engine.ontology().get_type_info(struct_select.def_id);
        let _entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(struct_select.def_id))?;

        let raw_props_vec: Vec<_> = collection
            .iter()
            .map(|(key, props)| (key.clone(), props.clone()))
            .collect();

        raw_props_vec
            .into_iter()
            .map(|(entity_key, properties)| {
                self.apply_struct_select(engine, type_info, &entity_key, properties, struct_select)
            })
            .collect()
    }

    fn apply_struct_select(
        &self,
        engine: &DomainEngine,
        type_info: &TypeInfo,
        entity_key: &DynamicKey,
        mut properties: BTreeMap<PropertyId, Attribute>,
        struct_select: &StructSelect,
    ) -> DomainResult<Value> {
        for (property_id, subselect) in &struct_select.properties {
            if properties.contains_key(property_id) {
                continue;
            }

            if let Some(data_relationship) = type_info.data_relationships.get(property_id) {
                if !matches!(
                    data_relationship.kind,
                    DataRelationshipKind::EntityGraph { .. }
                ) {
                    continue;
                }

                let attributes =
                    self.sub_query_attributes(engine, entity_key, *property_id, subselect)?;

                match data_relationship.cardinality.1 {
                    ValueCardinality::One => {
                        if let Some(attribute) = attributes.into_iter().next() {
                            properties.insert(*property_id, attribute);
                        }
                    }
                    ValueCardinality::Many => {
                        properties.insert(
                            *property_id,
                            Value::new(Data::Sequence(attributes), data_relationship.target).into(),
                        );
                    }
                }
            }
        }

        Ok(Value::new(Data::Struct(properties), struct_select.def_id))
    }

    fn sub_query_attributes(
        &self,
        engine: &DomainEngine,
        parent_key: &DynamicKey,
        property_id: PropertyId,
        select: &Select,
    ) -> DomainResult<Vec<Attribute>> {
        let relationship_id = property_id.relationship_id;
        let edge_collection = self
            .edge_collections
            .get(&relationship_id)
            .expect("No edge collection");

        match property_id.role {
            Role::Subject => edge_collection
                .edges
                .iter()
                .filter(|edge| &edge.from == parent_key)
                .map(|edge| -> DomainResult<Attribute> {
                    let entity = self.sub_query_entity(engine, &edge.to, select)?;
                    Ok(Attribute {
                        value: entity,
                        rel_params: edge.params.clone(),
                    })
                })
                .collect(),
            Role::Object => edge_collection
                .edges
                .iter()
                .filter(|edge| &edge.to == parent_key)
                .map(|edge| -> DomainResult<Attribute> {
                    let entity = self.sub_query_entity(engine, &edge.from, select)?;
                    Ok(Attribute {
                        value: entity,
                        rel_params: edge.params.clone(),
                    })
                })
                .collect(),
        }
    }

    fn sub_query_entity(
        &self,
        engine: &DomainEngine,
        entity_key: &DynamicKey,
        select: &Select,
    ) -> DomainResult<Value> {
        if let Select::Struct(struct_select) = select {
            // sanity check
            if !self.collections.contains_key(&struct_select.def_id) {
                error!(
                    "Store does not contain a collection for {:?}",
                    struct_select.def_id
                );
                return Err(DomainError::InvalidEntityDefId);
            }
        }

        // Find the def_id of the dynamic key. This is a little inefficient.
        let (def_id, properties) = self
            .collections
            .iter()
            .find_map(|(def_id, collection)| {
                collection
                    .get(entity_key)
                    .map(|properties| (*def_id, properties.clone()))
            })
            .ok_or(DomainError::IdNotFound)?;

        let type_info = engine.ontology().get_type_info(def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(def_id))?;

        match select {
            Select::Leaf => {
                // Entity leaf only includes the ID of that entity, not its other fields
                let id_attribute = properties
                    .get(&PropertyId::subject(entity_info.id_relationship_id))
                    .unwrap();
                Ok(id_attribute.value.clone())
            }
            Select::Struct(struct_select) => {
                let mut select_properties = struct_select.properties.clone();

                // Need to "infer" mandatory entity properties, because JSON serializer expects that
                for (property_id, data_relationship) in type_info.entity_relationships() {
                    if matches!(
                        data_relationship.cardinality.0,
                        PropertyCardinality::Mandatory
                    ) && !select_properties.contains_key(property_id)
                    {
                        select_properties.insert(*property_id, Select::Leaf);
                    }
                }

                self.apply_struct_select(
                    engine,
                    type_info,
                    entity_key,
                    properties,
                    &StructSelect {
                        def_id,
                        properties: select_properties,
                    },
                )
            }
            Select::StructUnion(_, _) => todo!(),
            Select::EntityId => todo!(),
            Select::Entity(entity_select) => {
                let entity_key = entity_key.clone();
                let entities = self.query_entities(engine, entity_select)?;
                for entity in entities {
                    let id = find_inherent_entity_id(engine.ontology(), &entity)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id.data)?;

                        if dynamic_key == entity_key {
                            return Ok(entity);
                        }
                    }
                }

                panic!("Not found")
            }
        }
    }

    pub fn write_new_entity(
        &mut self,
        engine: &DomainEngine,
        entity: Value,
        select: Select,
    ) -> DomainResult<Value> {
        let entity_id = self.write_new_entity_inner(engine, entity)?;

        match select {
            Select::EntityId => Ok(entity_id),
            Select::Struct(struct_select) => {
                let target_dynamic_key = Self::extract_dynamic_key(&entity_id.data)?;
                let entities = self.query_entities(
                    engine,
                    &EntitySelect {
                        source: StructOrUnionSelect::Struct(struct_select),
                        limit: u32::MAX,
                        cursor: None,
                    },
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
        engine: &DomainEngine,
        entity: Value,
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
                                    engine,
                                    &entity_key,
                                    property_id,
                                    attribute,
                                    data_relationship,
                                )?;
                            }
                            ValueCardinality::Many => {
                                let attributes = match attribute.value.data {
                                    Data::Sequence(attributes) => attributes,
                                    _ => panic!("Expected sequence for ValueCardinality::Many"),
                                };

                                for attribute in attributes {
                                    self.insert_entity_relationship(
                                        engine,
                                        &entity_key,
                                        property_id,
                                        attribute,
                                        data_relationship,
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
        engine: &DomainEngine,
        entity_key: &DynamicKey,
        property_id: PropertyId,
        attribute: Attribute,
        data_relationship: &DataRelationshipInfo,
    ) -> DomainResult<()> {
        debug!("entity rel attribute: {attribute:?}");

        let ontology = engine.ontology();
        let value = attribute.value;
        let rel_params = attribute.rel_params;

        let foreign_key = if value.type_def_id == data_relationship.target {
            let foreign_id = self.write_new_entity_inner(engine, value)?;
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
                    engine,
                    Value::new(Data::Struct(entity_data), data_relationship.target),
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

    fn extract_dynamic_key(id_data: &Data) -> DomainResult<DynamicKey> {
        match id_data {
            Data::Struct(struct_map) => {
                if struct_map.len() != 1 {
                    return Err(DomainError::IdNotFound);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::extract_dynamic_key(&attribute.1.value.data)
            }
            Data::Text(string) => Ok(DynamicKey::Text(string.clone())),
            Data::OctetSequence(octets) => Ok(DynamicKey::Octets(octets.clone())),
            Data::I64(int) => Ok(DynamicKey::Int(*int)),
            _ => Err(DomainError::IdNotFound),
        }
    }

    fn look_up_entity(
        &self,
        def_id: DefId,
        dynamic_key: &DynamicKey,
    ) -> Option<&BTreeMap<PropertyId, Attribute>> {
        let collection = self.collections.get(&def_id)?;
        collection.get(dynamic_key)
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

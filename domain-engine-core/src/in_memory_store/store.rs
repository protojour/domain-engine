use std::collections::BTreeMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    env::{EntityRelationship, Env, ValueCardinality},
    query::{EntityQuery, Query, StructOrUnionQuery, StructQuery},
    serde::{
        operator::{SerdeOperator, SerdeOperatorId, ValueOperator},
        processor::{ProcessorLevel, ProcessorMode},
    },
    smart_format,
    string_types::StringLikeType,
    value::{Attribute, Data, PropertyId, Value},
    value_generator::ValueGenerator,
    DefId, RelationshipId, Role,
};
use smartstring::alias::String;
use tracing::debug;
use uuid::Uuid;

use crate::{
    entity_id_utils::{analyze_string_pattern, find_inherent_entity_id},
    DomainEngine, DomainError, DomainResult,
};

#[derive(Debug)]
pub struct InMemoryStore {
    pub collections: FnvHashMap<DefId, EntityCollection>,
    #[allow(unused)]
    pub edge_collections: FnvHashMap<RelationshipId, EdgeCollection>,
    pub int_id_counter: i64,
}

#[derive(Debug, Clone)]
enum DynamicKey {
    String(String),
    Uuid(Uuid),
    Int(i64),
}

#[derive(Debug)]
pub enum EntityCollection {
    String(EntityTable<String>),
    Uuid(EntityTable<Uuid>),
    Int(EntityTable<i64>),
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
        query: &EntityQuery,
    ) -> DomainResult<Vec<Value>> {
        match &query.source {
            StructOrUnionQuery::Struct(struct_query) => {
                self.query_single_entity(engine, struct_query)
            }
            StructOrUnionQuery::Union(..) => todo!(),
        }
    }

    fn query_single_entity(
        &self,
        engine: &DomainEngine,
        struct_query: &StructQuery,
    ) -> DomainResult<Vec<Value>> {
        let collection = self
            .collections
            .get(&struct_query.def_id)
            .ok_or(DomainError::InvalidId)?;

        let type_info = engine.get_env().get_type_info(struct_query.def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(struct_query.def_id))?;

        let raw_props_vec: Vec<_> = match collection {
            EntityCollection::Uuid(table) => table.values().cloned().collect(),
            EntityCollection::Int(table) => table.values().cloned().collect(),
            EntityCollection::String(table) => table.values().cloned().collect(),
        };

        raw_props_vec
            .into_iter()
            .map(|mut properties| -> DomainResult<Value> {
                for (property_id, sub_query) in &struct_query.properties {
                    if properties.contains_key(property_id) {
                        continue;
                    }

                    if let Some(entity_relationship) =
                        entity_info.entity_relationships.get(property_id)
                    {
                        let attributes = self.sub_query(engine, *property_id, sub_query)?;

                        match entity_relationship.cardinality.1 {
                            ValueCardinality::One => {
                                if let Some(attribute) = attributes.into_iter().next() {
                                    properties.insert(*property_id, attribute);
                                }
                            }
                            ValueCardinality::Many => {
                                properties.insert(
                                    *property_id,
                                    Value::new(
                                        Data::Sequence(attributes),
                                        entity_relationship.target,
                                    )
                                    .into(),
                                );
                            }
                        }
                    }
                }

                Ok(Value::new(Data::Struct(properties), struct_query.def_id))
            })
            .collect()
    }

    fn sub_query(
        &self,
        _engine: &DomainEngine,
        _property_id: PropertyId,
        _query: &Query,
    ) -> DomainResult<Vec<Attribute>> {
        todo!()
    }

    pub fn write_entity(&mut self, engine: &DomainEngine, entity: Value) -> DomainResult<Value> {
        debug!("write entity {entity:#?}");

        let env = engine.get_env();
        let type_info = env.get_type_info(entity.type_def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(entity.type_def_id))?;

        let (id, id_generated) = match find_inherent_entity_id(env, &entity)? {
            Some(id) => (id, false),
            None => {
                if let Some(value_generator) = entity_info.id_value_generator {
                    let id =
                        self.generate_entity_id(env, entity_info.id_operator_id, value_generator)?;
                    (id, true)
                } else {
                    panic!("No id provided and no ID generator");
                }
            }
        };

        debug!("write entity id={id:?}");

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
            if let Some(entity_relationship) = entity_info.entity_relationships.get(&property_id) {
                match entity_relationship.cardinality.1 {
                    ValueCardinality::One => {
                        self.insert_entity_relationship(
                            engine,
                            &entity_key,
                            property_id,
                            attribute,
                            entity_relationship,
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
                                entity_relationship,
                            )?;
                        }
                    }
                }
            } else {
                raw_props.insert(property_id, attribute);
            }
        }

        let collection = self.collections.get_mut(&entity.type_def_id).unwrap();
        Self::store_in_collection(collection, entity_key, raw_props)?;

        Ok(id)
    }

    fn insert_entity_relationship(
        &mut self,
        engine: &DomainEngine,
        entity_key: &DynamicKey,
        property_id: PropertyId,
        attribute: Attribute,
        entity_relationship: &EntityRelationship,
    ) -> DomainResult<()> {
        debug!("entity rel attribute: {attribute:?}");

        let env = engine.get_env();
        let value = attribute.value;
        let rel_params = attribute.rel_params;

        let foreign_key = if value.type_def_id == entity_relationship.target {
            let foreign_id = self.write_entity(engine, value)?;
            Self::extract_dynamic_key(&foreign_id.data)?
        } else {
            let foreign_key = Self::extract_dynamic_key(&value.data)?;
            if self
                .look_up_entity(entity_relationship.target, &foreign_key)
                .is_none()
            {
                let type_info = env.get_type_info(value.type_def_id);
                let repr = if let Some(operator_id) = type_info.operator_id {
                    // TODO: Easier way to report values in "human readable"/JSON format

                    let processor = env.new_serde_processor(
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
                    return Err(DomainError::InvalidId);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::extract_dynamic_key(&attribute.1.value.data)
            }
            Data::String(string) => Ok(DynamicKey::String(string.clone())),
            Data::Uuid(uuid) => Ok(DynamicKey::Uuid(*uuid)),
            Data::Int(int) => Ok(DynamicKey::Int(*int)),
            _ => Err(DomainError::InvalidId),
        }
    }

    fn look_up_entity(
        &self,
        def_id: DefId,
        dynamic_key: &DynamicKey,
    ) -> Option<&BTreeMap<PropertyId, Attribute>> {
        let collection = self.collections.get(&def_id)?;
        match (collection, dynamic_key) {
            (EntityCollection::String(table), DynamicKey::String(string)) => table.get(string),
            (EntityCollection::Uuid(table), DynamicKey::Uuid(uuid)) => table.get(uuid),
            (EntityCollection::Int(table), DynamicKey::Int(int)) => table.get(int),
            _ => None,
        }
    }

    fn store_in_collection(
        collection: &mut EntityCollection,
        dynamic_key: DynamicKey,
        raw_props: BTreeMap<PropertyId, Attribute>,
    ) -> DomainResult<()> {
        match (collection, dynamic_key) {
            (EntityCollection::String(table), DynamicKey::String(string)) => {
                table.insert(string, raw_props);
                Ok(())
            }
            (EntityCollection::Uuid(table), DynamicKey::Uuid(uuid)) => {
                table.insert(uuid, raw_props);
                Ok(())
            }
            (EntityCollection::Int(table), DynamicKey::Int(int)) => {
                table.insert(int, raw_props);
                Ok(())
            }
            _ => Err(DomainError::InvalidId),
        }
    }

    fn generate_entity_id(
        &mut self,
        env: &Env,
        id_operator_id: SerdeOperatorId,
        value_generator: ValueGenerator,
    ) -> DomainResult<Value> {
        match (env.get_serde_operator(id_operator_id), value_generator) {
            (SerdeOperator::String(def_id), ValueGenerator::UuidV4) => {
                let string = smart_format!("{}", Uuid::new_v4());
                Ok(Value::new(Data::String(string), *def_id))
            }
            (SerdeOperator::StringPattern(def_id), _) => {
                match (env.get_string_like_type(*def_id), value_generator) {
                    (Some(StringLikeType::Uuid), ValueGenerator::UuidV4) => {
                        Ok(Value::new(Data::Uuid(Uuid::new_v4()), *def_id))
                    }
                    _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
                }
            }
            (SerdeOperator::CapturingStringPattern(def_id), _) => {
                if let Some(property) =
                    analyze_string_pattern(env.get_string_pattern(*def_id).unwrap())
                {
                    let type_info = env.get_type_info(property.type_def_id);
                    let id = self.generate_entity_id(
                        env,
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
            (SerdeOperator::Int(def_id), ValueGenerator::Autoincrement) => {
                let id_value = self.int_id_counter;
                self.int_id_counter += 1;
                Ok(Value::new(Data::Int(id_value), *def_id))
            }
            (
                SerdeOperator::ValueType(ValueOperator {
                    def_variant,
                    inner_operator_id,
                    ..
                }),
                _,
            ) => {
                let mut value =
                    self.generate_entity_id(env, *inner_operator_id, value_generator)?;
                value.type_def_id = def_variant.def_id;
                Ok(value)
            }
            _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
        }
    }
}

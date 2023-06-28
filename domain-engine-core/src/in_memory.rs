// #![allow(dead_code, unused)]

use std::collections::BTreeMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    env::Env,
    serde::operator::{SerdeOperator, SerdeOperatorId, ValueOperator},
    smart_format,
    string_types::StringLikeType,
    value::{Attribute, Data, PropertyId, Value},
    value_generator::ValueGenerator,
    DefId, PackageId, RelationshipId,
};
use smartstring::alias::String;
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

use crate::{
    data_source::DataSourceAPI,
    entity_id_utils::{analyze_string_pattern, find_inherent_entity_id},
    DomainError,
};

#[derive(Debug)]
pub struct InMemory {
    #[allow(unused)]
    package_id: PackageId,
    storage: RwLock<Storage>,
}

#[derive(Debug)]
struct Storage {
    collections: FnvHashMap<DefId, EntityCollection>,
    #[allow(unused)]
    edge_collections: FnvHashMap<RelationshipId, EdgeCollection>,
    int_id_counter: i64,
}

#[derive(Debug)]
enum EntityCollection {
    String(EntityTable<String>),
    Uuid(EntityTable<Uuid>),
    Int(EntityTable<i64>),
}

type EntityTable<K> = IndexMap<K, BTreeMap<PropertyId, Attribute>>;

#[derive(Debug)]
struct EdgeCollection {
    _edges: Vec<Edge>,
}

#[derive(Debug)]
struct Edge {
    _from: Value,
    _to: Value,
    _params: Value,
}

impl InMemory {
    pub fn new(env: &Env, package_id: PackageId) -> Self {
        let domain = env.find_domain(package_id).unwrap();

        let mut collections: FnvHashMap<DefId, EntityCollection> = Default::default();
        let mut edge_collections: FnvHashMap<RelationshipId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if let Some(entity_info) = &type_info.entity_info {
                collections.insert(
                    type_info.def_id,
                    Self::collection_from_id_operator(env, entity_info.id_operator_id),
                );

                for (property_id, _entity_relationship) in &entity_info.entity_relationships {
                    let relationship_id = property_id.relationship_id;
                    edge_collections
                        .entry(relationship_id)
                        .or_insert_with(|| EdgeCollection { _edges: vec![] });
                }
            }
        }

        Self {
            package_id,
            storage: RwLock::new(Storage {
                collections,
                edge_collections,
                int_id_counter: 0,
            }),
        }
    }

    fn collection_from_id_operator(env: &Env, id_operator_id: SerdeOperatorId) -> EntityCollection {
        match env.get_serde_operator(id_operator_id) {
            SerdeOperator::String(_) => EntityCollection::String(Default::default()),
            SerdeOperator::Int(_) => EntityCollection::Int(Default::default()),
            SerdeOperator::StringPattern(def_id) => match env.get_string_like_type(*def_id) {
                Some(StringLikeType::Uuid) => EntityCollection::Uuid(Default::default()),
                _ => panic!("string pattern unsuitable for id"),
            },
            SerdeOperator::ValueType(ValueOperator {
                inner_operator_id, ..
            }) => Self::collection_from_id_operator(env, *inner_operator_id),
            SerdeOperator::CapturingStringPattern(def_id) => {
                if let Some(property) =
                    analyze_string_pattern(env.get_string_pattern(*def_id).unwrap())
                {
                    let type_info = env.get_type_info(property.type_def_id);
                    Self::collection_from_id_operator(env, type_info.operator_id.unwrap())
                } else {
                    panic!("String pattern without any properties")
                }
            }
            operator => panic!("unrecognized operator {operator:?}"),
        }
    }
}

#[async_trait::async_trait]
impl DataSourceAPI for InMemory {
    async fn store_entity(&self, env: &Env, entity: Value) -> Result<Value, DomainError> {
        self.storage.write().await.write_entity(env, entity)
    }

    async fn query(&self, _env: &Env, def_id: DefId) -> Result<Vec<Value>, DomainError> {
        let storage = self.storage.read().await;

        let _collection = match storage.collections.get(&def_id) {
            Some(collection) => collection,
            None => return Ok(vec![]),
        };

        Ok(vec![])
    }
}

impl Storage {
    fn write_entity(&mut self, env: &Env, entity: Value) -> Result<Value, DomainError> {
        debug!("write entity {entity:#?}");

        let type_info = env.get_type_info(entity.type_def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(entity.type_def_id))?;

        let id = match find_inherent_entity_id(env, &entity)? {
            Some(id) => id,
            None => {
                if let Some(value_generator) = entity_info.id_value_generator {
                    self.generate_entity_id(env, entity_info.id_operator_id, value_generator)?
                } else {
                    panic!("No id provided and no ID generator");
                }
            }
        };

        debug!("write entity id={id:?}");

        let struct_map = match entity.data {
            Data::Struct(struct_map) => struct_map,
            _ => return Err(DomainError::EntityMustBeStruct),
        };

        let mut raw_props: BTreeMap<PropertyId, Attribute> = Default::default();

        for (property_id, attribute) in struct_map {
            if let Some(entity_relationship) = entity_info.entity_relationships.get(&property_id) {
                debug!(
                    "entity relationships: {:#?}",
                    entity_info.entity_relationships
                );

                todo!("{entity_relationship:?}");
            } else {
                raw_props.insert(property_id, attribute);
            }
        }

        let collection = self.collections.get_mut(&entity.type_def_id).unwrap();
        Self::store_in_collection(collection, &id.data, raw_props)?;

        Ok(id)
    }

    fn store_in_collection(
        collection: &mut EntityCollection,
        id_data: &Data,
        raw_props: BTreeMap<PropertyId, Attribute>,
    ) -> Result<(), DomainError> {
        match (collection, id_data) {
            (collection, Data::Struct(struct_map)) => {
                if struct_map.len() != 1 {
                    return Err(DomainError::InvalidId);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::store_in_collection(collection, &attribute.1.value.data, raw_props)
            }
            (EntityCollection::String(table), Data::String(string)) => {
                table.insert(string.clone(), raw_props);
                Ok(())
            }
            (EntityCollection::Uuid(table), Data::Uuid(uuid)) => {
                table.insert(*uuid, raw_props);
                Ok(())
            }
            (EntityCollection::Int(table), Data::Int(int)) => {
                table.insert(*int, raw_props);
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
    ) -> Result<Value, DomainError> {
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

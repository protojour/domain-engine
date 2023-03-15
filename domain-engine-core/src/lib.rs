use std::{collections::HashMap, sync::Arc};

use serde::de::DeserializeSeed;
use thiserror::Error;

use ontol_runtime::{
    env::Env,
    query::{EntityQuery, MapOrUnionQuery},
    serde::processor::{ProcessorLevel, ProcessorMode},
    value::{Attribute, Data, PropertyId},
    DefId, PackageId,
};

pub struct Config {
    pub default_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { default_limit: 20 }
    }
}

#[derive(Error, Clone, Debug)]
pub enum DomainError {}

#[unimock::unimock(api = DomainAPIMock)]
#[async_trait::async_trait]
pub trait DomainAPI: Send + Sync + 'static {
    async fn query_entities(
        &self,
        package_id: PackageId,
        entity_query: EntityQuery,
    ) -> Result<Vec<Attribute>, DomainError>;
}

pub struct DummyDomainAPI {
    env: Arc<Env>,
    entities: HashMap<DefId, Result<Vec<Attribute>, DomainError>>,
}

impl DummyDomainAPI {
    pub fn new(env: Arc<Env>) -> Self {
        Self {
            env,
            entities: Default::default(),
        }
    }

    pub fn add_entity(
        &mut self,
        def_id: DefId,
        id_json: serde_json::Value,
        json: serde_json::Value,
    ) {
        let type_info = self.env.get_type_info(def_id);
        let entity_info = type_info.entity_info.as_ref().unwrap();

        let mut attribute = self
            .env
            .new_serde_processor(
                type_info.operator_id.unwrap(),
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .deserialize(&mut serde_json::Deserializer::from_str(
                &serde_json::to_string(&json).unwrap(),
            ))
            .unwrap();

        let id = self
            .env
            .new_serde_processor(
                entity_info.id_operator_id,
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .deserialize(&mut serde_json::Deserializer::from_str(
                &serde_json::to_string(&id_json).unwrap(),
            ))
            .unwrap();

        if let Data::Map(map) = &mut attribute.value.data {
            map.insert(PropertyId::subject(entity_info.id_relation_id), id);
        } else {
            panic!();
        }

        let result = self.entities.entry(type_info.def_id).or_insert(Ok(vec![]));
        match result {
            Ok(entities) => entities.push(attribute),
            Err(_) => panic!(),
        }
    }
}

#[async_trait::async_trait]
impl DomainAPI for DummyDomainAPI {
    async fn query_entities(
        &self,
        _: PackageId,
        query: EntityQuery,
    ) -> Result<Vec<Attribute>, DomainError> {
        match &query.source {
            MapOrUnionQuery::Map(map) => match self.entities.get(&map.def_id) {
                None => Ok(vec![]),
                Some(Ok(entities)) => Ok(entities.clone()),
                Some(Err(error)) => Err(error.clone()),
            },
            _ => panic!(),
        }
    }
}

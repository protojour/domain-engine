use std::sync::Arc;

use data_store::DataStoreAPI;
use in_memory_store::api::InMemoryDb;
use thiserror::Error;

use ontol_runtime::{
    config::DataStoreConfig,
    env::Env,
    query::{EntityQuery, StructOrUnionQuery},
    value::{Attribute, Value},
    DefId,
};

pub mod data_store;

mod entity_id_utils;
mod in_memory_store;

pub struct Config {
    pub default_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { default_limit: 20 }
    }
}

#[derive(Error, Clone, Debug)]
pub enum DomainError {
    #[error("No data store")]
    NoDataStore,
    #[error("Not an entity")]
    NotAnEntity(DefId),
    #[error("Entity must be a struct")]
    EntityMustBeStruct,
    #[error("Invalid id")]
    InvalidId,
    #[error("Type cannot be used for id generation")]
    TypeCannotBeUsedForIdGeneration,
}

#[unimock::unimock(api = EngineAPIMock)]
#[async_trait::async_trait]
pub trait EngineAPI: Send + Sync + 'static {
    fn get_config(&self) -> &Config;

    /// Store an entity. Returns the entity id.
    async fn store_entity(&self, entity: Value) -> Result<Value, DomainError>;

    async fn query_entities(&self, query: EntityQuery) -> Result<Vec<Attribute>, DomainError>;

    async fn create_entity(
        &self,
        value: Value,
        query: StructOrUnionQuery,
    ) -> Result<Value, DomainError>;
}

pub struct DomainEngine {
    env: Arc<Env>,
    config: Arc<Config>,

    #[allow(unused)]
    data_store: Option<Box<dyn DataStoreAPI + Send + Sync>>,
}

impl DomainEngine {
    pub fn new(env: Arc<Env>) -> Self {
        let mut data_store: Option<Box<dyn DataStoreAPI + Send + Sync>> = None;

        for (package_id, _) in env.domains() {
            if let Some(config) = env.get_package_config(*package_id) {
                if let Some(DataStoreConfig::InMemory) = config.data_store {
                    data_store = Some(Box::new(InMemoryDb::new(&env, *package_id)))
                }
            }
        }

        Self {
            env,
            config: Arc::new(Config::default()),
            data_store,
        }
    }

    async fn store_entity_inner(&self, entity: Value) -> Result<Value, DomainError> {
        // TODO: Domain translation by finding optimal mapping path
        let data_store = self.data_store.as_ref().ok_or(DomainError::NoDataStore)?;

        data_store.store_entity(&self.env, entity).await
    }
}

#[async_trait::async_trait]
impl EngineAPI for DomainEngine {
    fn get_config(&self) -> &Config {
        &self.config
    }

    async fn store_entity(&self, entity: Value) -> Result<Value, DomainError> {
        self.store_entity_inner(entity).await
    }

    async fn query_entities(&self, _query: EntityQuery) -> Result<Vec<Attribute>, DomainError> {
        let _ = self.env.domains().count();
        Ok(vec![])
    }

    async fn create_entity(
        &self,
        _value: Value,
        _query: StructOrUnionQuery,
    ) -> Result<Value, DomainError> {
        Ok(Value::unit())
    }
}

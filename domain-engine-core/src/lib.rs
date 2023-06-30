use std::sync::Arc;

use data_store::DataStoreAPI;
use in_memory_store::api::InMemoryDb;
use smartstring::alias::String;
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
    #[error("Unresolved foreign key: {0}")]
    UnresolvedForeignKey(String),
}

pub type DomainResult<T> = Result<T, DomainError>;

#[unimock::unimock(api = EngineAPIMock)]
#[async_trait::async_trait]
pub trait EngineAPI: Send + Sync + 'static {
    fn get_config(&self) -> &Config;

    /// Store an entity. Returns the entity id.
    async fn store_entity(&self, entity: Value) -> DomainResult<Value>;

    async fn query_entities(&self, query: EntityQuery) -> DomainResult<Vec<Attribute>>;

    async fn create_entity(&self, value: Value, query: StructOrUnionQuery) -> DomainResult<Value>;
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

    fn get_env(&self) -> &Env {
        &self.env
    }

    pub fn data_store(&self) -> DomainResult<&(dyn DataStoreAPI + Send + Sync)> {
        self.data_store.as_deref().ok_or(DomainError::NoDataStore)
    }

    async fn query_entities_inner(&self, query: EntityQuery) -> DomainResult<Vec<Attribute>> {
        // TODO: Domain translation by rewriting whatever is used as query language
        self.data_store()?.query(self, query).await
    }

    async fn store_entity_inner(&self, entity: Value) -> DomainResult<Value> {
        // TODO: Domain translation by finding optimal mapping path
        self.data_store()?.store_entity(self, entity).await
    }
}

#[async_trait::async_trait]
impl EngineAPI for DomainEngine {
    fn get_config(&self) -> &Config {
        &self.config
    }

    async fn query_entities(&self, query: EntityQuery) -> DomainResult<Vec<Attribute>> {
        self.query_entities_inner(query).await
    }

    async fn store_entity(&self, entity: Value) -> DomainResult<Value> {
        self.store_entity_inner(entity).await
    }

    async fn create_entity(&self, value: Value, _query: StructOrUnionQuery) -> DomainResult<Value> {
        self.store_entity_inner(value).await
    }
}

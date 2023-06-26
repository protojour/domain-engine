use std::sync::Arc;

use data_source::DataSourceAPI;
use in_memory::InMemory;
use thiserror::Error;

use ontol_runtime::{
    config::DataSourceConfig,
    env::Env,
    query::{EntityQuery, StructOrUnionQuery},
    value::{Attribute, Value},
};

pub mod data_source;

mod in_memory;

#[cfg(test)]
mod tests;

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

#[unimock::unimock(api = EngineAPIMock)]
#[async_trait::async_trait]
pub trait EngineAPI: Send + Sync + 'static {
    fn get_config(&self) -> &Config;

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
    data_source: Option<Box<dyn DataSourceAPI + Send + Sync>>,
}

impl DomainEngine {
    pub fn new(env: Arc<Env>) -> Self {
        let mut data_source: Option<Box<dyn DataSourceAPI + Send + Sync>> = None;

        for (package_id, domain) in env.domains() {
            if let Some(config) = env.get_package_config(*package_id) {
                if let Some(DataSourceConfig::InMemory) = config.data_source {
                    data_source = Some(Box::new(InMemory::from_domain(*package_id, domain)))
                }
            }
        }

        Self {
            env,
            config: Arc::new(Config::default()),
            data_source,
        }
    }
}

#[async_trait::async_trait]
impl EngineAPI for DomainEngine {
    fn get_config(&self) -> &Config {
        &self.config
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

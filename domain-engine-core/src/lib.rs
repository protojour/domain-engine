use std::sync::Arc;

use thiserror::Error;

use ontol_runtime::{
    env::Env,
    query::{EntityQuery, StructOrUnionQuery},
    value::{Attribute, Value},
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

pub struct Engine {
    env: Arc<Env>,
    config: Arc<Config>,
}

#[async_trait::async_trait]
impl EngineAPI for Engine {
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

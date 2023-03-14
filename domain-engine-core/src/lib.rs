use thiserror::Error;

use ontol_runtime::{query::EntityQuery, value::Value, PackageId};

pub struct Config {
    pub default_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { default_limit: 20 }
    }
}

#[derive(Error, Debug)]
pub enum DomainError {}

#[async_trait::async_trait]
pub trait DomainAPI: Send + Sync + 'static {
    async fn query_entities(
        &self,
        package_id: PackageId,
        entity_query: EntityQuery,
    ) -> Result<Vec<Value>, DomainError>;
}

#[derive(Default)]
pub struct DummyDomainAPI;

#[async_trait::async_trait]
impl DomainAPI for DummyDomainAPI {
    async fn query_entities(
        &self,
        _: PackageId,
        _: EntityQuery,
    ) -> Result<Vec<Value>, DomainError> {
        Ok(vec![])
    }
}

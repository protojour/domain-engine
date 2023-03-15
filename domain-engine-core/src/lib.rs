use std::sync::Arc;

use thiserror::Error;

use ontol_runtime::{query::EntityQuery, value::Attribute, PackageId};

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

    async fn query_entities(
        &self,
        package_id: PackageId,
        entity_query: EntityQuery,
    ) -> Result<Vec<Attribute>, DomainError>;
}

/// Facade interface used by "user facing" APIs.
/// Not sure if this is the right way to design this.
pub struct EngineFrontFacade {
    pub config: Arc<Config>,
    pub engine_api: Arc<dyn EngineAPI>,
}

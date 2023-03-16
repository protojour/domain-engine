use std::sync::Arc;

use thiserror::Error;

use ontol_runtime::{env::Env, query::EntityQuery, value::Attribute, PackageId};

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

pub struct Engine {
    _env: Arc<Env>,
}

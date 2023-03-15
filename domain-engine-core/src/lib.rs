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

#[unimock::unimock(api = DomainAPIMock)]
#[async_trait::async_trait]
pub trait DomainAPI: Send + Sync + 'static {
    async fn query_entities(
        &self,
        package_id: PackageId,
        entity_query: EntityQuery,
    ) -> Result<Vec<Attribute>, DomainError>;
}

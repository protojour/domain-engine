use ontol_runtime::{value::Value, DefId};

use crate::{DomainEngine, DomainError};

#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn store_entity(
        &self,
        engine: &DomainEngine,
        entity: Value,
    ) -> Result<Value, DomainError>;

    async fn query(&self, engine: &DomainEngine, def_id: DefId) -> Result<Vec<Value>, DomainError>;
}

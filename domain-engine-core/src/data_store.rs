use ontol_runtime::{
    query::EntityQuery,
    value::{Attribute, Value},
};

use crate::{DomainEngine, DomainResult};

#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn query(
        &self,
        engine: &DomainEngine,
        query: EntityQuery,
    ) -> DomainResult<Vec<Attribute>>;

    async fn store_entity(&self, engine: &DomainEngine, entity: Value) -> DomainResult<Value>;
}

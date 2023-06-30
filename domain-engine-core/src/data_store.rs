use ontol_runtime::{
    query::{EntityQuery, Query},
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

    async fn store_new_entity(
        &self,
        engine: &DomainEngine,
        entity: Value,
        query: Query,
    ) -> DomainResult<Value>;
}

use ontol_runtime::{
    query::{EntityQuery, Query},
    value::{Attribute, Value},
    PackageId,
};

use crate::{DomainEngine, DomainResult};

pub struct DataStore {
    package_id: PackageId,
    api: Box<dyn DataStoreAPI + Send + Sync>,
}

impl DataStore {
    pub fn new(package_id: PackageId, api: Box<dyn DataStoreAPI + Send + Sync>) -> Self {
        Self { package_id, api }
    }

    pub fn package_id(&self) -> PackageId {
        self.package_id
    }

    pub fn api(&self) -> &(dyn DataStoreAPI + Send + Sync) {
        self.api.as_ref()
    }
}

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

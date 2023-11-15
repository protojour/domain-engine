use ontol_runtime::{
    config::DataStoreConfig,
    ontology::Ontology,
    select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
    PackageId,
};
use serde::{Deserialize, Serialize};
use unimock::unimock;

use crate::domain_error::DomainResult;
use crate::{domain_engine::DomainEngine, in_memory_store::api::InMemoryDb};

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

/// A request to the data store
#[derive(Serialize, Deserialize)]
pub enum Request {
    Query(EntitySelect),
    StoreNewEntity(Value, Select),
}

/// A response from the data store.
///
/// Must match the corresponding Request.
#[derive(Serialize, Deserialize)]
pub enum Response {
    Query(Sequence),
    StoreNewEntity(Value),
}

#[unimock(api = DataStoreAPIMock)]
#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn execute(&self, request: Request, engine: &DomainEngine) -> DomainResult<Response>;
}

/// Trait for creating data store APIs
#[async_trait::async_trait]
pub trait DataStoreFactory {
    async fn new_api(
        &self,
        config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>>;
}

#[derive(Default)]
pub struct DefaultDataStoreFactory;

#[async_trait::async_trait]
impl DataStoreFactory for DefaultDataStoreFactory {
    async fn new_api(
        &self,
        _config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        Ok(Box::new(InMemoryDb::new(ontology, package_id)))
    }
}

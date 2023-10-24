use ontol_runtime::{
    config::DataStoreConfig,
    ontology::Ontology,
    select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
    PackageId,
};
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

#[unimock(api = DataStoreAPIMock)]
#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn query(&self, select: EntitySelect, engine: &DomainEngine) -> DomainResult<Sequence>;

    async fn store_new_entity(
        &self,
        entity: Value,
        select: Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value>;
}

/// Trait for creating data store APIs
#[async_trait::async_trait]
pub trait DataStoreFactory {
    async fn new_api(
        config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> Box<dyn DataStoreAPI + Send + Sync>;
}

pub struct DefaultDataStoreFactory;

#[async_trait::async_trait]
impl DataStoreFactory for DefaultDataStoreFactory {
    async fn new_api(
        _config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> Box<dyn DataStoreAPI + Send + Sync> {
        Box::new(InMemoryDb::new(ontology, package_id))
    }
}

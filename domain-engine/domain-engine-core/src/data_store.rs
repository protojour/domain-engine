use ontol_runtime::{
    config::DataStoreConfig,
    ontology::Ontology,
    select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
    DefId, PackageId,
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
    /// A single query
    Query(EntitySelect),
    /// A sequence of batch writes.
    /// The corresponding responses must retain the order of inputs.
    BatchWrite(Vec<BatchWriteRequest>),
}

#[derive(Serialize, Deserialize)]
pub enum BatchWriteRequest {
    Insert(Vec<Value>, Select),
    Update(Vec<Value>, Select),
    Delete(Vec<Value>, DefId),
}

/// A response from the data store.
///
/// Must match the corresponding Request.
#[derive(Serialize, Deserialize)]
pub enum Response {
    Query(Sequence),
    BatchWrite(Vec<BatchWriteResponse>),
}

#[derive(Serialize, Deserialize)]
pub enum BatchWriteResponse {
    Inserted(Vec<Value>),
    Updated(Vec<Value>),
    Deleted(Vec<bool>),
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

pub trait DataStoreFactorySync {
    fn new_api_sync(
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
        config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        self.new_api_sync(config, ontology, package_id)
    }
}

impl DataStoreFactorySync for DefaultDataStoreFactory {
    fn new_api_sync(
        &self,
        _config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        Ok(Box::new(InMemoryDb::new(ontology, package_id)))
    }
}

impl From<Vec<BatchWriteResponse>> for Response {
    fn from(value: Vec<BatchWriteResponse>) -> Self {
        Self::BatchWrite(value)
    }
}

impl Response {
    pub fn one_inserted(value: Value) -> Self {
        Self::BatchWrite(vec![BatchWriteResponse::Inserted(vec![value])])
    }
}

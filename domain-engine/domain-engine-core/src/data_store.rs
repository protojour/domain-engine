use std::sync::Arc;

use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    query::select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
    DefId, PackageId,
};
use serde::{Deserialize, Serialize};
use unimock::unimock;

use crate::{domain_error::DomainResult, system::ArcSystemApi, Session};

#[unimock(api = DataStoreAPIMock)]
#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn execute(&self, request: Request, session: Session) -> DomainResult<Response>;
}

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
    Upsert(Vec<Value>, Select),
    Delete(Vec<Value>, DefId),
}

/// A response from the data store.
///
/// Must match the corresponding Request.
#[derive(Serialize, Deserialize)]
pub enum Response {
    Query(Sequence<Value>),
    BatchWrite(Vec<WriteResponse>),
}

impl From<Vec<WriteResponse>> for Response {
    fn from(value: Vec<WriteResponse>) -> Self {
        Self::BatchWrite(value)
    }
}

impl Response {
    pub fn one_inserted(value: Value) -> Self {
        Self::BatchWrite(vec![WriteResponse::Inserted(value)])
    }
}

#[derive(Serialize, Deserialize)]
pub enum WriteResponse {
    Inserted(Value),
    Updated(Value),
    Deleted(bool),
}

/// Trait for creating data store APIs
#[async_trait::async_trait]
pub trait DataStoreFactory {
    async fn new_api(
        &self,
        package_id: PackageId,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>>;
}

pub trait DataStoreFactorySync {
    fn new_api_sync(
        &self,
        package_id: PackageId,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>>;
}

use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream::BoxStream;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    query::select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
    DefId, PackageId,
};
use serde::{Deserialize, Serialize};

use crate::{
    domain_error::DomainResult,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage},
    Session,
};

#[async_trait::async_trait]
pub trait DataStoreAPI {
    async fn execute(&self, request: Request, session: Session) -> DomainResult<Response>;

    /// Transact.
    ///
    /// This is a duplex operation, the input messages are transformed into output messages
    /// as they are executed on the data store.
    async fn transact<'a>(
        &'a self,
        messages: BoxStream<'a, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'_, DomainResult<RespMessage>>>;
}

pub struct DataStore {
    package_ids: BTreeSet<PackageId>,
    api: Box<dyn DataStoreAPI + Send + Sync>,
}

impl DataStore {
    pub fn new(package_ids: BTreeSet<PackageId>, api: Box<dyn DataStoreAPI + Send + Sync>) -> Self {
        Self { package_ids, api }
    }

    pub fn package_ids(&self) -> &BTreeSet<PackageId> {
        &self.package_ids
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
        persisted: &BTreeSet<PackageId>,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>>;
}

pub trait DataStoreFactorySync {
    fn new_api_sync(
        &self,
        persisted: &BTreeSet<PackageId>,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>>;
}

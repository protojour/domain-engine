use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream::BoxStream;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    DomainIndex,
};

use crate::{
    domain_error::DomainResult,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage, TransactionMode},
    Session,
};

#[async_trait::async_trait]
pub trait DataStoreAPI {
    /// Transact.
    ///
    /// This is a duplex operation, the input messages are transformed into output messages
    /// as they are executed on the data store.
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>>;
}

pub struct DataStore {
    persisted: BTreeSet<DomainIndex>,
    api: Arc<dyn DataStoreAPI + Send + Sync>,
}

impl DataStore {
    pub fn new(persisted: BTreeSet<DomainIndex>, api: Arc<dyn DataStoreAPI + Send + Sync>) -> Self {
        Self { persisted, api }
    }

    pub fn persisted(&self) -> &BTreeSet<DomainIndex> {
        &self.persisted
    }

    pub fn api(&self) -> &(dyn DataStoreAPI + Send + Sync) {
        self.api.as_ref()
    }
}

#[non_exhaustive]
#[derive(Clone)]
pub struct DataStoreParams {
    pub config: DataStoreConfig,
    pub session: Session,
    pub ontology: Arc<Ontology>,
    pub system: ArcSystemApi,
    pub datastore_mutated: tokio::sync::watch::Sender<()>,
    pub index_mutated: tokio::sync::watch::Sender<()>,
}

/// Trait for creating data store APIs
#[async_trait::async_trait]
pub trait DataStoreFactory {
    /// Create a new data store, potentially migrating it.
    ///
    /// The set of persisted packages is sorted, so passed in the order compiled by the compiler.
    /// This means that the database can be migrated in that order, with upstream domains
    /// being migrated before downstream domains.
    async fn new_api(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>>;
}

pub trait DataStoreFactorySync {
    fn new_api_sync(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>>;
}

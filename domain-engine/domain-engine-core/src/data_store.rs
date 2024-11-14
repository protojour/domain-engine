use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream::BoxStream;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    DomainIndex, PropId,
};

use crate::{
    domain_error::{DomainErrorKind, DomainResult},
    search::{VertexSearchParams, VertexSearchResults},
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

    /// Perform a vertex search.
    #[expect(unused)]
    async fn vertex_search(
        &self,
        params: VertexSearchParams,
        session: Session,
    ) -> DomainResult<VertexSearchResults> {
        Err(DomainErrorKind::NotImplemented("vertex search".to_string()).into_error())
    }

    /// Returns whether the data store is currently working on building a search index in the background.
    ///
    /// The answer may not be 100% reliable, since the definition of "running" is a little vague.
    fn background_search_indexer_running(&self) -> bool {
        false
    }

    /// Initiate and await shutdown. Must release any resources
    /// so that it's safe to recreate/reinitialize the data store afterwards.
    async fn shutdown(&self) -> DomainResult<()> {
        Ok(())
    }

    #[expect(unused)]
    fn stable_property_index(&self, prop_id: PropId) -> Option<u32> {
        None
    }
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

#[derive(Clone)]
pub struct DataStoreParams {
    /// Configuration used to choose a datastore implementation.
    pub config: DataStoreConfig,

    /// Session used when initializing the datastore.
    pub session: Session,

    /// The ontology the datastore has access to.
    pub ontology: Arc<Ontology>,

    /// The system api.
    pub system: ArcSystemApi,

    /// A signal that should be emitted when the data store changes.
    /// Can trigger reindexing or UI notification.
    pub datastore_mutated: tokio::sync::watch::Sender<()>,

    /// A signal that should be emitted when the search index changes.
    /// Can be used for UI notification.
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

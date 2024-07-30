use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream::BoxStream;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    PackageId,
};

use crate::{
    domain_error::DomainResult,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage},
    Session,
};

#[async_trait::async_trait]
pub trait DataStoreAPI {
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

use std::collections::BTreeSet;
use std::sync::Arc;

use document::IndexingContext;
use domain_engine_core::{
    data_store::{DataStoreAPI, DataStoreFactory, DataStoreFactorySync, DataStoreParams},
    transact::{ReqMessage, RespMessage, TransactionMode},
    DomainResult, Session,
};
use futures_util::{stream::BoxStream, StreamExt};
use indexer::{indexer_blocking_task, synchronizer_async_task, SyncQueue, Synrchonizer};
use ontol_runtime::{ontology::Ontology, DomainIndex};
use schema::make_schema;
use tantivy::Index;
use tokio_util::sync::CancellationToken;

mod document;
mod indexer;
mod schema;

const INDEX_WRITER_MEM_LIMIT: usize = 50_000_000;

#[derive(Clone)]
pub struct TantivyParams {
    pub cancel: CancellationToken,
}

pub fn make_tantivy_layer(
    tantivy_params: TantivyParams,
    data_store_params: DataStoreParams,
    data_store: Arc<dyn DataStoreAPI + Send + Sync>,
) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
    let ontology = data_store_params.ontology.clone();
    let index_mutated = data_store_params.index_mutated.clone();
    Ok(Arc::new(TantivyDataStoreLayer::new(
        data_store,
        ontology,
        index_mutated,
        tantivy_params.cancel.clone(),
    )?))
}

pub struct TantivyDataStoreFactoryLayer<F> {
    inner: F,
    params: TantivyParams,
}

impl<F> TantivyDataStoreFactoryLayer<F> {
    pub fn new(inner: F, params: TantivyParams) -> Self {
        Self { inner, params }
    }
}

#[async_trait::async_trait]
impl<F: DataStoreFactory + Send + Sync> DataStoreFactory for TantivyDataStoreFactoryLayer<F> {
    async fn new_api(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        make_tantivy_layer(
            self.params.clone(),
            params.clone(),
            self.inner.new_api(persisted, params).await?,
        )
    }
}

impl<F: DataStoreFactorySync> DataStoreFactorySync for TantivyDataStoreFactoryLayer<F> {
    fn new_api_sync(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        make_tantivy_layer(
            self.params.clone(),
            params.clone(),
            self.inner.new_api_sync(persisted, params)?,
        )
    }
}

struct TantivyDataStoreLayer {
    data_store: Arc<dyn DataStoreAPI + Send + Sync>,
    sync_queue: Arc<SyncQueue>,

    /// Cancellation token for all indexing tasks.
    /// Cancelling happens automatically upon dropping the layer (Drop impl).
    indexer_cancel: CancellationToken,
}

#[async_trait::async_trait]
impl DataStoreAPI for TantivyDataStoreLayer {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        let resp_messages = self.data_store.transact(mode, messages, session).await?;
        let sync_queue = self.sync_queue.clone();

        Ok(resp_messages
            .map(move |message_result| match message_result {
                Ok(RespMessage::WriteComplete(stats)) => {
                    sync_queue.post_write_stats(&stats);
                    Ok(RespMessage::WriteComplete(stats))
                }
                other => other,
            })
            .boxed())
    }
}

impl TantivyDataStoreLayer {
    fn new(
        data_store: Arc<dyn DataStoreAPI + Send + Sync>,
        ontology: Arc<Ontology>,
        index_mutated: tokio::sync::watch::Sender<()>,
        cancel: CancellationToken,
    ) -> DomainResult<Self> {
        let schema = make_schema();

        let indexer_cancel = cancel.child_token();
        let (notify_tx, notify_rx) = tokio::sync::watch::channel(());
        let (vertex_tx, vertex_rx) = tokio::sync::mpsc::channel(100);
        let sync_queue = SyncQueue::new(notify_tx);

        let index = Index::create_in_ram(schema.schema.clone());
        let index_writer = index.writer(INDEX_WRITER_MEM_LIMIT).unwrap();

        // start async task that synchronizes the work queue for the real indexer task
        tokio::task::spawn({
            let indexer_queue = sync_queue.clone();
            let indexer_cancel = indexer_cancel.clone();
            let synchronizer = Synrchonizer {
                ontology: ontology.clone(),
                data_store: data_store.clone(),
                vertex_tx,
            };
            async move {
                synchronizer_async_task(
                    synchronizer,
                    indexer_queue.clone(),
                    notify_rx,
                    indexer_cancel,
                )
                .await;
            }
        });

        let indexing_context = IndexingContext { ontology, schema };

        // real indexer task
        std::thread::Builder::new()
            .name("tantivy-indexer".to_string())
            .spawn(move || {
                indexer_blocking_task(indexing_context, vertex_rx, index_mutated, index_writer);
            })
            .unwrap();

        Ok(Self {
            data_store,
            sync_queue,
            indexer_cancel,
        })
    }
}

impl Drop for TantivyDataStoreLayer {
    fn drop(&mut self) {
        self.indexer_cancel.cancel();
    }
}

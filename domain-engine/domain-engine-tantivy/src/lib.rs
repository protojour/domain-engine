use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use document::IndexingContext;
use domain_engine_core::domain_error::DomainErrorKind;
use domain_engine_core::search::{VertexSearchParams, VertexSearchResults};
use domain_engine_core::{
    data_store::{DataStoreAPI, DataStoreParams},
    transact::{ReqMessage, RespMessage, TransactionMode},
    DomainResult, Session,
};
use futures_util::{stream::BoxStream, StreamExt};
use indexer::{indexer_blocking_task, synchronizer_async_task, SyncQueue, Synrchonizer};
use ontol_runtime::ontology::Ontology;
use schema::{make_schema, SchemaWithMeta};
use tantivy::{Index, IndexReader, ReloadPolicy};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::debug;

mod document;
mod indexer;
mod schema;
mod search;
mod vertex_fetch;

#[derive(Clone)]
pub struct TantivyConfig {
    /// Original datastore params from DomainEngine
    pub data_store_params: DataStoreParams,

    /// The wrapped datastore facade used for forwarding user-originating transactions
    pub datastore: Arc<dyn DataStoreAPI + Send + Sync>,

    /// Datastore facade used for indexing requests
    ///
    /// the reason this is a separate facade is that it can circumvent sequrity checks,
    /// since indexing is an internal matter.
    pub indexing_datastore: Arc<dyn DataStoreAPI + Send + Sync>,

    pub index_source: TantivyIndexSource,

    /// The number of vertices that can fit in the indexing queue
    pub vertex_index_queue_size: usize,

    /// Max amount of memory used by the indexer
    pub index_writer_mem_budget: usize,

    /// Cancellation token for the indexer task
    pub cancel: CancellationToken,
}

#[derive(Clone)]
pub enum TantivyIndexSource {
    InMemory,
    MMapDir(PathBuf),
}

pub fn make_tantivy_layer(
    config: TantivyConfig,
) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
    Ok(Arc::new(TantivyDataStoreLayer::new(config)?))
}

#[derive(Clone)]
struct TantivyDataStoreLayer {
    config: TantivyConfig,
    sync_queue: Arc<SyncQueue>,
    schema: SchemaWithMeta,
    index: Index,
    index_reader: IndexReader,
    indexer_running: Arc<AtomicBool>,

    #[expect(unused)]
    /// Cancellation token for all indexing tasks.
    /// Cancelling happens automatically upon dropping the layer (Drop impl).
    indexer_cancel_dropguard: Arc<DropGuard>,
}

#[async_trait::async_trait]
impl DataStoreAPI for TantivyDataStoreLayer {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        let resp_messages = self
            .config
            .datastore
            .transact(mode, messages, session)
            .await?;
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

    async fn vertex_search(
        &self,
        params: VertexSearchParams,
        session: Session,
    ) -> DomainResult<VertexSearchResults> {
        self.clone().vertex_search(params, session).await
    }

    fn background_search_indexer_running(&self) -> bool {
        self.indexer_running.load(Ordering::SeqCst)
    }
}

impl TantivyDataStoreLayer {
    fn new(config: TantivyConfig) -> DomainResult<Self> {
        debug!("TantivyDataStoreLayer::new");
        let schema = make_schema();

        let indexer_cancel = config.cancel.child_token();
        let indexer_running = Arc::new(AtomicBool::new(false));
        let (notify_tx, notify_rx) = tokio::sync::watch::channel(());
        let (vertex_tx, vertex_rx) = tokio::sync::mpsc::channel(config.vertex_index_queue_size);
        let sync_queue = SyncQueue::new(notify_tx, indexer_running.clone());

        let index = match &config.index_source {
            TantivyIndexSource::InMemory => Index::create_in_ram(schema.schema.clone()),
            TantivyIndexSource::MMapDir(path) => if path.exists() {
                Index::open_in_dir(&path)
            } else {
                // TODO: Need to reindex if the schema changed
                Index::create_in_dir(&path, schema.schema.clone())
            }
            .map_err(|err| {
                DomainErrorKind::Search(format!(
                    "unable to open index at path {path}: {err:?}",
                    path = path.display()
                ))
            })?,
        };

        let index_writer = index
            .writer(config.index_writer_mem_budget)
            .map_err(|err| DomainErrorKind::Search(format!("writer init error: {err:?}")))?;
        let index_reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|err| DomainErrorKind::Search(format!("reader init error: {err:?}")))?;

        // start async task that synchronizes the work queue for the real indexer task
        tokio::task::spawn({
            let indexer_queue = sync_queue.clone();
            let indexer_cancel = indexer_cancel.clone();
            let synchronizer = Synrchonizer {
                ontology: config.data_store_params.ontology.clone(),
                data_store: config.indexing_datastore.clone(),
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

        let indexing_context = IndexingContext {
            ontology: config.data_store_params.ontology.clone(),
            schema: schema.clone(),
        };

        let index_mutated = config.data_store_params.index_mutated.clone();

        // real indexer task
        std::thread::Builder::new()
            .name("tantivy-indexer".to_string())
            .spawn({
                let indexer_running = indexer_running.clone();
                let index_reader = index_reader.clone();
                move || {
                    indexer_blocking_task(
                        indexing_context,
                        vertex_rx,
                        index_mutated,
                        index_writer,
                        index_reader,
                        indexer_running,
                    );
                }
            })
            .unwrap();

        Ok(Self {
            config,
            sync_queue,
            schema,
            index,
            index_reader,
            indexer_cancel_dropguard: Arc::new(indexer_cancel.drop_guard()),
            indexer_running,
        })
    }

    pub fn ontology(&self) -> &Ontology {
        &self.config.data_store_params.ontology
    }
}

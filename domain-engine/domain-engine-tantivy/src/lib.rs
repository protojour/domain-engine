use std::path::PathBuf;
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
use indexer::WorkTracker;
use indexer::{synchronizer_async_task, writer_blocking_task, SyncQueue, Synrchonizer};
use ontol_runtime::ontology::Ontology;
use schema::{make_schema, SchemaWithMeta};
use tantivy::directory::MmapDirectory;
use tantivy::tokenizer::{Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer};
use tantivy::{Index, IndexReader, ReloadPolicy};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::debug;

mod document;
mod indexer;
mod schema;
mod search;
mod vertex_fetch;

const TOKENIZER_NAME: &str = "default";

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
    indexer_work_tracker: Arc<WorkTracker>,

    indexer_cancel: CancellationToken,

    #[expect(unused)]
    /// Cancellation token for all indexing tasks.
    /// Cancelling happens automatically upon dropping the layer (Drop impl).
    indexer_cancel_dropguard: Arc<DropGuard>,

    writer_thread_finished: Arc<tokio::sync::Notify>,
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
        let pending_work = self.indexer_work_tracker.count_ongoing_work();
        debug!("client pending work: {pending_work}");
        pending_work > 0
    }

    async fn shutdown(&self) -> DomainResult<()> {
        self.config.datastore.shutdown().await?;

        self.indexer_cancel.cancel();
        self.writer_thread_finished.notified().await;

        Ok(())
    }
}

impl TantivyDataStoreLayer {
    fn new(config: TantivyConfig) -> DomainResult<Self> {
        debug!("TantivyDataStoreLayer::new");
        let schema = make_schema();

        let indexer_cancel = config.cancel.child_token();
        let indexer_work_tracker = Arc::new(WorkTracker::default());
        let (vertex_tx, vertex_rx) = tokio::sync::mpsc::channel(config.vertex_index_queue_size);
        let sync_queue = SyncQueue::new(indexer_work_tracker.clone());

        let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(Stemmer::new(Language::English))
            .build();

        let index = match &config.index_source {
            TantivyIndexSource::InMemory => Index::create_in_ram(schema.schema.clone()),
            TantivyIndexSource::MMapDir(path) => {
                let mmap_dir = MmapDirectory::open(path)
                    .map_err(|_| DomainErrorKind::Search(format!("could not open dir {path:?}")))?;
                Index::open_or_create(mmap_dir, schema.schema.clone()).map_err(|err| {
                    DomainErrorKind::Search(format!(
                        "unable to open index at path {path}: {err:?}",
                        path = path.display()
                    ))
                })?
            }
        };

        index.tokenizers().register(TOKENIZER_NAME, tokenizer);

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
            let sync_queue = sync_queue.clone();
            let indexer_cancel = indexer_cancel.clone();
            let synchronizer = Synrchonizer {
                ontology: config.data_store_params.ontology.clone(),
                data_store: config.indexing_datastore.clone(),
                vertex_tx,
            };
            async move {
                synchronizer_async_task(synchronizer, sync_queue.clone(), indexer_cancel).await;
            }
        });

        let indexing_context = IndexingContext {
            ontology: config.data_store_params.ontology.clone(),
            schema: schema.clone(),
            indexing_datastore: config.indexing_datastore.clone(),
        };

        let index_mutated = config.data_store_params.index_mutated.clone();

        let writer_thread_finished = Arc::new(tokio::sync::Notify::new());

        // real indexer task
        std::thread::Builder::new()
            .name("tantivy-indexer".to_string())
            .spawn({
                let index_reader = index_reader.clone();
                let indexer_work_tracker = indexer_work_tracker.clone();
                let writer_thread_finished = writer_thread_finished.clone();
                move || {
                    writer_blocking_task(
                        indexing_context,
                        vertex_rx,
                        index_mutated,
                        index_writer,
                        index_reader,
                        indexer_work_tracker,
                        writer_thread_finished,
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
            indexer_cancel: indexer_cancel.clone(),
            indexer_cancel_dropguard: Arc::new(indexer_cancel.drop_guard()),
            indexer_work_tracker,
            writer_thread_finished,
        })
    }

    pub fn ontology(&self) -> &Ontology {
        &self.config.data_store_params.ontology
    }
}

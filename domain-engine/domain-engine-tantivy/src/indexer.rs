use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use domain_engine_core::{
    data_store::DataStoreAPI,
    domain_select::domain_select_no_edges,
    transact::{ReqMessage, RespMessage, TransactionMode, WriteStats},
    Session, VertexAddr,
};
use futures_util::StreamExt;
use ontol_runtime::{
    ontology::Ontology,
    query::{
        condition::{Clause, CondTerm, SetOperator, SetPredicate},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    value::Value,
    DefId, OntolDefTag,
};
use smallvec::{smallvec, SmallVec};
use tantivy::{IndexReader, IndexWriter, Term};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

use crate::document::IndexingContext;

enum SyncMsg {
    Update(UpdateSyncMsg),
    Delete(VertexAddr, WorkToken),
}

struct UpdateSyncMsg {
    def_id: DefId,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    work_tokens: SmallVec<WorkToken, 1>,
}

pub enum VertexMsg {
    Update(Value, SmallVec<WorkToken, 1>),
    Delete(VertexAddr, WorkToken),
    Cancel,
}

pub struct Synrchonizer {
    pub ontology: Arc<Ontology>,
    pub data_store: Arc<dyn DataStoreAPI + Send + Sync>,
    pub vertex_tx: tokio::sync::mpsc::Sender<VertexMsg>,
}

pub struct SyncQueue {
    queue: Mutex<VecDeque<SyncMsg>>,
    queue_watch: tokio::sync::watch::Sender<()>,
    work_tracker: Arc<WorkTracker>,
}

#[derive(Default)]
pub struct WorkTracker {
    tokens: Mutex<Vec<WorkToken>>,
}

/// A tracker for WorkTokens.
///
/// The tracker can answer whether there is currently work ongoing.
/// "ongoing work" needs to hold on to its respective WorkToken(s).
///
/// The tracker can then answer whether all issued tokens are still alive.
impl WorkTracker {
    /// Issue a new work token
    fn issue(&self) -> WorkToken {
        let work_token = WorkToken(Arc::new(()));
        self.tokens.lock().unwrap().push(work_token.clone());
        work_token
    }

    /// Count ongoing work.
    /// This method also performs internal memory-saving cleanup.
    pub fn count_ongoing_work(&self) -> usize {
        let mut lock = self.tokens.lock().unwrap();
        // Token is still alive if its refcount is larger than 1:
        lock.retain(|token| Arc::strong_count(&token.0) > 1);
        lock.len()
    }
}

#[derive(Clone)]
pub struct WorkToken(Arc<()>);

/// The synchronizer's responsibility is to read change stats
/// from the underlying data store, read the implied vertices out of the store,
/// and send them over to the indexer task.
pub async fn synchronizer_async_task(
    synchronizer: Synrchonizer,
    sync_queue: Arc<SyncQueue>,
    mut queue_watch: tokio::sync::watch::Receiver<()>,
    indexer_cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = queue_watch.changed() => {
                // repeatedly pop messages from the queue front,
                // keeping the rest in the queue for potential merging
                while let Some(msg) = sync_queue.take_one() {
                    synchronizer.handle_incoming(msg).await;
                }
            }
            _ = indexer_cancel.cancelled() => {
                let _ = synchronizer.vertex_tx.send(VertexMsg::Cancel).await;
                return;
            }
        }
    }
}

/// The indexer's responsibility is to receive vertex data,
/// convert into searchable documents, and commit the new inverted index.
pub fn indexer_blocking_task(
    indexing_context: IndexingContext,
    mut vertex_rx: tokio::sync::mpsc::Receiver<VertexMsg>,
    index_mutated: tokio::sync::watch::Sender<()>,
    mut index_writer: IndexWriter,
    index_reader: IndexReader,
    work_tracker: Arc<WorkTracker>,
) {
    let mut update_count: usize = 0;
    let mut delete_count: usize = 0;

    loop {
        let work_tokens = match vertex_rx.blocking_recv() {
            Some(VertexMsg::Update(vertex, work_tokens)) => {
                trace!("index update {:?}", vertex.type_def_id());
                if let Err(err) = indexing_context.index(vertex, true, &mut index_writer) {
                    error!("document not reindexed: {err:?}");
                }

                update_count += 1;
                work_tokens
            }
            Some(VertexMsg::Delete(vertex_addr, work_token)) => {
                trace!("index delete {vertex_addr:?}");
                index_writer.delete_term(Term::from_field_bytes(
                    indexing_context.schema.vertex_addr,
                    &vertex_addr,
                ));

                delete_count += 1;
                smallvec![work_token]
            }
            Some(VertexMsg::Cancel) => {
                debug!("indexer task cancelled");
                return;
            }
            None => {
                debug!("indexer task killed: message queue closed");
                return;
            }
        };

        // FIXME: should commit also after a fixed number of iterations, or passed time,
        // or else there is a risk of never committing
        if vertex_rx.is_empty() {
            info!("committing index, {update_count} updated, {delete_count} deleted");
            index_writer.commit().unwrap();
            update_count = 0;
            delete_count = 0;

            if let Err(err) = index_reader.reload() {
                error!("could not reload index reader: {err:?}");
            }

            drop(work_tokens);
            let pending_work = work_tracker.count_ongoing_work();
            debug!("indexer pending work remaining: {pending_work}");

            let _ = index_mutated.send(());
        } else {
            drop(work_tokens);
        }
    }
}

impl IndexingContext {
    fn index(
        &self,
        vertex: Value,
        force_update: bool,
        index_writer: &mut IndexWriter,
    ) -> anyhow::Result<()> {
        let doc = self.make_vertex_doc(vertex)?;

        if force_update || doc.update_time > doc.create_time {
            // an update must delete the previous document
            index_writer.delete_term(Term::from_field_bytes(
                self.schema.vertex_addr,
                &doc.vertex_addr,
            ));
        }

        index_writer.add_document(doc.doc)?;

        Ok(())
    }
}

impl Synrchonizer {
    async fn handle_incoming(&self, msg: SyncMsg) {
        match msg {
            SyncMsg::Update(msg) => {
                self.update(msg).await;
            }
            SyncMsg::Delete(addr, work_token) => {
                let _ = self
                    .vertex_tx
                    .send(VertexMsg::Delete(addr, work_token))
                    .await;
            }
        }
    }

    async fn update(&self, msg: UpdateSyncMsg) {
        self.sync_updates(msg).await;
    }

    async fn sync_updates(&self, update: UpdateSyncMsg) {
        let Select::Struct(mut struct_select) =
            domain_select_no_edges(update.def_id, &self.ontology)
        else {
            return;
        };

        let work_tokens = update.work_tokens;

        struct_select.properties.insert(
            OntolDefTag::RelationDataStoreAddress.prop_id_0(),
            Select::Unit,
        );
        struct_select
            .properties
            .insert(OntolDefTag::CreateTime.prop_id_0(), Select::Unit);
        struct_select
            .properties
            .insert(OntolDefTag::UpdateTime.prop_id_0(), Select::Unit);

        let filter = {
            let mut filter = Filter::default_for_datastore();
            let condition = filter.condition_mut();
            let root_var = condition.mk_cond_var();
            condition.add_clause(root_var, Clause::Root);
            let set_var = condition.mk_cond_var();
            condition.add_clause(
                root_var,
                Clause::MatchProp(
                    OntolDefTag::UpdateTime.prop_id_0(),
                    SetOperator::ElementIn,
                    set_var,
                ),
            );
            condition.add_clause(
                set_var,
                Clause::SetPredicate(
                    SetPredicate::Gt,
                    CondTerm::Value(Value::ChronoDateTime(
                        update.start,
                        OntolDefTag::DateTime.def_id().into(),
                    )),
                ),
            );
            filter
        };

        let messages = [Ok(ReqMessage::Query(
            0,
            EntitySelect {
                source: StructOrUnionSelect::Struct(struct_select),
                filter,
                limit: None,
                after_cursor: None,
                include_total_len: false,
            },
        ))];

        match self
            .data_store
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter(messages).boxed(),
                Session::default(),
            )
            .await
        {
            Ok(resp_stream) => {
                let _: Vec<_> = resp_stream
                    .then(|resp_msg| {
                        let vertex_tx = self.vertex_tx.clone();
                        let work_tokens = work_tokens.clone();
                        async move {
                            match resp_msg {
                                Ok(RespMessage::Element(vertex, _)) => {
                                    if let Err(err) = vertex_tx.send(VertexMsg::Update(vertex, work_tokens)).await {
                                        error!(
                                            "error passing vertex update into indexing queue: {err:?}"
                                        );
                                    }
                                }
                                Ok(_ignored_message) => {}
                                Err(error) => {
                                    error!("query error: {error:?}");
                                }
                            }
                        }
                    })
                    .collect()
                    .await;
            }
            Err(error) => {
                error!("Unable to index update: {error}");
            }
        }
    }
}

impl SyncQueue {
    pub fn new(
        queue_watch: tokio::sync::watch::Sender<()>,
        work_tracker: Arc<WorkTracker>,
    ) -> Arc<Self> {
        Arc::new(SyncQueue {
            queue: Mutex::new(VecDeque::new()),
            queue_watch,
            work_tracker,
        })
    }

    pub fn post_write_stats(&self, stats: &WriteStats) {
        {
            let mut queue = self.queue.lock().unwrap();
            Self::write_to_queue(stats, &mut queue, &self.work_tracker);
        }

        // send signal to the synchronizer, so it may start processing messages from the queue
        if let Err(err) = self.queue_watch.send(()) {
            error!("could not send change notification to indexing queue: {err}");
        }
    }

    fn take_one(&self) -> Option<SyncMsg> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }

    fn write_to_queue(
        stats: &WriteStats,
        queue: &mut VecDeque<SyncMsg>,
        work_tracker: &WorkTracker,
    ) {
        for mutated in stats.mutated() {
            let work_token = work_tracker.issue();

            if let Some(update_msg) = Self::find_update_mut(queue, mutated) {
                update_msg.start = update_msg.start.min(stats.start());
                update_msg.end = update_msg.end.max(stats.end());
                update_msg.work_tokens.push(work_token);
            } else {
                queue.push_back(SyncMsg::Update(UpdateSyncMsg {
                    def_id: mutated,
                    start: stats.start(),
                    end: stats.end(),
                    work_tokens: smallvec![work_token],
                }));
            }
        }

        for deleted in stats.deleted() {
            queue.push_back(SyncMsg::Delete(deleted, work_tracker.issue()));
        }
    }

    fn find_update_mut(queue: &mut VecDeque<SyncMsg>, def_id: DefId) -> Option<&mut UpdateSyncMsg> {
        for queue_msg in queue.iter_mut() {
            if let SyncMsg::Update(update_msg) = queue_msg {
                if update_msg.def_id == def_id {
                    return Some(update_msg);
                }
            }
        }

        None
    }
}

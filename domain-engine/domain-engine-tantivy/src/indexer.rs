use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
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
use tantivy::{IndexReader, IndexWriter, Term};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

use crate::document::IndexingContext;

#[derive(Debug)]
enum SyncMsg {
    Update(UpdateSyncMsg),
    Delete(VertexAddr),
}

#[derive(Debug)]
struct UpdateSyncMsg {
    def_id: DefId,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
}

pub enum VertexMsg {
    Update(Value),
    Delete(VertexAddr),
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
    indexer_running: Arc<AtomicBool>,
}

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
                    synchronizer.handle_message(msg).await;
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
    indexer_running: Arc<AtomicBool>,
) {
    let mut update_count: usize = 0;
    let mut delete_count: usize = 0;

    loop {
        match vertex_rx.blocking_recv() {
            Some(VertexMsg::Update(vertex)) => {
                indexer_running.store(true, Ordering::SeqCst);

                trace!("index update {:?}", vertex.type_def_id());
                if let Err(err) = indexing_context.reindex(vertex, &mut index_writer) {
                    error!("document not reindexed: {err:?}");
                }

                update_count += 1;
            }
            Some(VertexMsg::Delete(vertex_addr)) => {
                indexer_running.store(true, Ordering::SeqCst);

                trace!("index delete {vertex_addr:?}");
                index_writer.delete_term(Term::from_field_bytes(
                    indexing_context.schema.vertex_addr,
                    &vertex_addr,
                ));

                delete_count += 1;
            }
            Some(VertexMsg::Cancel) | None => {
                indexer_running.store(false, Ordering::SeqCst);

                debug!("indexer task killed");
                return;
            }
        }

        // FIXME: should commit also after a fixed number of iterations, or passed time,
        // or else there is a risk of never committing
        if vertex_rx.is_empty() {
            debug!("committing index, {update_count} updated, {delete_count} deleted");
            index_writer.commit().unwrap();
            update_count = 0;
            delete_count = 0;
            indexer_running.store(false, Ordering::SeqCst);

            if let Err(err) = index_reader.reload() {
                error!("could not reload index reader: {err:?}");
            }

            let _ = index_mutated.send(());
        }
    }
}

impl IndexingContext {
    fn reindex(&self, vertex: Value, index_writer: &mut IndexWriter) -> anyhow::Result<()> {
        let doc = self.make_vertex_doc(&vertex)?;

        // update consists of deleting first
        index_writer.delete_term(Term::from_field_bytes(
            self.schema.vertex_addr,
            &doc.vertex_addr,
        ));
        index_writer.add_document(doc.doc)?;

        Ok(())
    }
}

impl Synrchonizer {
    async fn handle_message(&self, msg: SyncMsg) {
        match msg {
            SyncMsg::Update(msg) => {
                self.update(msg).await;
            }
            SyncMsg::Delete(addr) => {
                let _ = self.vertex_tx.send(VertexMsg::Delete(addr)).await;
            }
        }
    }

    async fn update(&self, msg: UpdateSyncMsg) {
        self.sync_updates(&msg).await;
    }

    async fn sync_updates(&self, update: &UpdateSyncMsg) {
        let Select::Struct(struct_select) = domain_select_no_edges(update.def_id, &self.ontology)
        else {
            return;
        };

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
                        async move {
                            match resp_msg {
                                Ok(RespMessage::Element(vertex, _)) => {
                                    if let Err(err) = vertex_tx.send(VertexMsg::Update(vertex)).await {
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
        indexer_running: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(SyncQueue {
            queue: Mutex::new(VecDeque::new()),
            queue_watch,
            indexer_running,
        })
    }

    pub fn post_write_stats(&self, stats: &WriteStats) {
        {
            let mut queue = self.queue.lock().unwrap();
            Self::write_to_queue(stats, &mut queue);
        }

        self.indexer_running.store(true, Ordering::SeqCst);

        // send signal to the synchronizer, so it may start processing messages from the queue
        if let Err(err) = self.queue_watch.send(()) {
            error!("could not send change notification to indexing queue: {err}");
        }
    }

    fn take_one(&self) -> Option<SyncMsg> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }

    fn write_to_queue(stats: &WriteStats, queue: &mut VecDeque<SyncMsg>) {
        for mutated in stats.mutated() {
            if let Some(update_msg) = Self::find_update_mut(queue, mutated) {
                update_msg.start = update_msg.start.min(stats.start());
                update_msg.end = update_msg.end.max(stats.end());
            } else {
                queue.push_back(SyncMsg::Update(UpdateSyncMsg {
                    def_id: mutated,
                    start: stats.start(),
                    end: stats.end(),
                }));
            }
        }

        for deleted in stats.deleted() {
            queue.push_back(SyncMsg::Delete(deleted));
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

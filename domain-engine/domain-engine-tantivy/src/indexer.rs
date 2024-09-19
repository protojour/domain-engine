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
use tantivy::{IndexWriter, Term};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::document::IndexingContext;

enum SyncMsg {
    Update(UpdateSyncMsg),
    Delete(VertexAddr),
}

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
    notify: tokio::sync::watch::Sender<()>,
}

pub async fn synchronizer_async_task(
    synchronizer: Synrchonizer,
    sync_queue: Arc<SyncQueue>,
    mut watch: tokio::sync::watch::Receiver<()>,
    indexer_cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = watch.changed() => {
                let messages = sync_queue.take_current();
                for msg in messages {
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

pub fn indexer_blocking_task(
    indexing_context: IndexingContext,
    mut vertex_rx: tokio::sync::mpsc::Receiver<VertexMsg>,
    index_mutated: tokio::sync::watch::Sender<()>,
    mut index_writer: IndexWriter,
) {
    loop {
        match vertex_rx.blocking_recv() {
            Some(VertexMsg::Update(vertex)) => {
                if let Err(err) = indexing_context.reindex(vertex, &mut index_writer) {
                    error!("document not reindexed: {err:?}");
                }
            }
            Some(VertexMsg::Delete(vertex_addr)) => {
                debug!("delete {vertex_addr:?}");
                index_writer.delete_term(Term::from_field_bytes(
                    indexing_context.schema.vertex_addr,
                    &vertex_addr,
                ));
            }
            Some(VertexMsg::Cancel) | None => {
                return;
            }
        }

        // FIXME: should commit also after a fixed number of iterations, or passed time,
        // or else there is a risk of never committing
        if vertex_rx.is_empty() {
            debug!("index commit");
            index_writer.commit().unwrap();

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
                            if let Ok(RespMessage::Element(vertex, _)) = resp_msg {
                                if let Err(err) = vertex_tx.send(VertexMsg::Update(vertex)).await {
                                    error!(
                                        "error passing vertex update into indexing queue: {err:?}"
                                    );
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
    pub fn new(notify: tokio::sync::watch::Sender<()>) -> Arc<Self> {
        Arc::new(SyncQueue {
            queue: Mutex::new(VecDeque::new()),
            notify,
        })
    }

    pub fn post_write_stats(&self, stats: &WriteStats) {
        {
            let mut queue = self.queue.lock().unwrap();
            Self::write_to_queue(stats, &mut queue);
        }

        if let Err(err) = self.notify.send(()) {
            error!("could not send change notification to indexing queue: {err}");
        }
    }

    fn take_current(&self) -> VecDeque<SyncMsg> {
        let mut queue = self.queue.lock().unwrap();
        std::mem::take(&mut queue)
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

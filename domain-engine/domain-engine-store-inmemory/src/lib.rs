#![forbid(unsafe_code)]

use core::{DbContext, EdgeColumn, EdgeVectorData};
use std::collections::BTreeSet;
use std::sync::Arc;

use domain_engine_core::{
    data_store::{DataStoreFactory, DataStoreFactorySync, DataStoreParams},
    make_interfacable::MakeInterfacable,
    make_storable::MakeStorable,
    system::ArcSystemApi,
    transact::{DataOperation, OpSequence, ReqMessage, RespMessage, TransactionMode, WriteStats},
    DomainError, Session,
};
use fnv::{FnvHashMap, FnvHashSet};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::domain::EdgeCardinalFlags;
use ontol_runtime::ontology::Ontology;
use ontol_runtime::query::select::Select;
use ontol_runtime::value::Value;
use ontol_runtime::{DefId, DomainIndex};
use tokio::sync::RwLock;

use constraint::ConstraintCheck;
use domain_engine_core::{data_store::DataStoreAPI, DomainResult};
use tracing::debug;

use crate::core::{DynamicKey, HyperEdgeTable, InMemoryStore, VertexTable};

mod constraint;
mod core;
mod filter;
mod query;
mod sort;
mod write;

#[derive(Clone)]
pub struct InMemoryDb {
    store: Arc<RwLock<InMemoryStore>>,
    ontology: Arc<Ontology>,
    system: ArcSystemApi,
    datastore_mutated: tokio::sync::watch::Sender<()>,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        self.clone().transact_inner(mode, messages).await
    }
}

impl InMemoryDb {
    fn new(
        persisted: &BTreeSet<DomainIndex>,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
        datastore_mutated: tokio::sync::watch::Sender<()>,
    ) -> Self {
        let mut collections: FnvHashMap<DefId, VertexTable<DynamicKey>> = Default::default();
        let mut hyper_edges: FnvHashMap<DefId, HyperEdgeTable> = Default::default();

        for domain_index in persisted {
            let domain = ontology.domain_by_index(*domain_index).unwrap();

            for def in domain.defs() {
                if let Some(entity) = def.entity() {
                    debug!(
                        "new collection {:?} (`{}`)",
                        def.id, &ontology[entity.ident]
                    );

                    collections.insert(def.id, Default::default());
                }
            }

            for (edge_id, edge_info) in domain.edges() {
                let columns = edge_info
                    .cardinals
                    .iter()
                    .map(|cardinal| {
                        let vector = if cardinal.flags.contains(EdgeCardinalFlags::ENTITY) {
                            EdgeVectorData::Keys(vec![])
                        } else {
                            EdgeVectorData::Values(vec![])
                        };

                        let mut vertex_union = FnvHashSet::default();

                        for def_id in cardinal.target.iter() {
                            vertex_union.insert(*def_id);
                        }

                        EdgeColumn {
                            data: vector,
                            vertex_union,
                            pinned: cardinal.flags.contains(EdgeCardinalFlags::PINNED_DEF),
                        }
                    })
                    .collect();

                hyper_edges
                    .entry(*edge_id)
                    .or_insert_with(|| HyperEdgeTable { columns });
            }
        }

        Self {
            store: Arc::new(RwLock::new(InMemoryStore {
                vertices: collections,
                edges: hyper_edges,
                serial_counter: 0,
            })),
            ontology,
            system,
            datastore_mutated,
        }
    }

    async fn transact_inner(
        self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        enum State {
            Insert(OpSequence, Select),
            Update(OpSequence, Select),
            Upsert(OpSequence, Select),
            Delete(OpSequence, DefId),
        }

        fn write_state(
            op_seq: OpSequence,
            new_state: State,
            state: &mut Option<State>,
        ) -> Vec<RespMessage> {
            let mut messages = vec![];

            match state {
                Some(
                    State::Insert(prev_op, _)
                    | State::Update(prev_op, _)
                    | State::Upsert(prev_op, _)
                    | State::Delete(prev_op, _),
                ) => {
                    messages.push(RespMessage::SequenceEnd(*prev_op, None));
                }
                None => {}
            }

            *state = Some(new_state);
            messages.push(RespMessage::SequenceStart(op_seq));
            messages
        }

        let mut state: Option<State> = None;

        Ok(async_stream::try_stream! {
            let mut ctx = DbContext {
                ontology_defs: self.ontology.as_ref().as_ref(),
                ontology_serde: self.ontology.as_ref().as_ref(),
                system: self.system.as_ref(),
                check: match mode {
                    TransactionMode::ReadOnly | TransactionMode::ReadWrite => ConstraintCheck::Disabled,
                    TransactionMode::ReadOnlyAtomic | TransactionMode::ReadWriteAtomic => ConstraintCheck::Deferred(Default::default()),
                },
                write_stats: WriteStats::builder(mode, self.system.as_ref())
            };

            for await req in messages {
                match req? {
                    ReqMessage::Query(op_seq, select) => {
                        state = None;
                        let store = self.store.read().await;
                        let sequence = store.query_entities(&select, &ctx)?;

                        let (elements, sub_sequence) = sequence.split();

                        yield RespMessage::SequenceStart(op_seq);

                        for mut item in elements {
                            MakeInterfacable::new(self.ontology.as_ref()).make_interfacable(&mut item)?;
                            yield RespMessage::Element(item, DataOperation::Queried);
                        }

                        yield RespMessage::SequenceEnd(op_seq, sub_sequence);

                    }
                    ReqMessage::Insert(op_seq, select) => {
                        for msg in write_state(op_seq, State::Insert(op_seq, select), &mut state) {
                            yield msg;
                        }
                    }
                    ReqMessage::Update(op_seq, select) => {
                        for msg in write_state(op_seq, State::Update(op_seq, select), &mut state) {
                            yield msg;
                        }
                    }
                    ReqMessage::Upsert(op_seq, select) => {
                        for msg in write_state(op_seq, State::Upsert(op_seq, select), &mut state) {
                            yield msg;
                        }
                    }
                    ReqMessage::Delete(op_seq, def_id) => {
                        for msg in write_state(op_seq, State::Delete(op_seq, def_id), &mut state) {
                            yield msg;
                        }
                    }
                    ReqMessage::Argument(mut value) => {
                        let mut store = self.store.write().await;

                        match state.as_ref() {
                            Some(State::Insert(_, select)) => {
                                MakeStorable::new(
                                    ProcessorMode::Create,
                                    self.ontology.as_ref(),
                                    self.system.as_ref(),
                                )
                                .make_storable(&mut value)?;

                                let value = store.write_new_entity(
                                    value,
                                    select,
                                    &mut ctx
                                )?;

                                yield RespMessage::Element(value, DataOperation::Inserted);
                            }
                            Some(State::Update(_, select)) => {
                                MakeStorable::new(
                                    ProcessorMode::Update,
                                    self.ontology.as_ref(),
                                    self.system.as_ref(),
                                )
                                .make_storable(&mut value)?;

                                let value = store.update_entity(
                                    value,
                                    select,
                                    &mut ctx
                                )?;

                                yield RespMessage::Element(value, DataOperation::Updated);
                            }
                            Some(State::Upsert(_, select)) => {
                                MakeStorable::new(
                                    ProcessorMode::Create,
                                    self.ontology.as_ref(),
                                    self.system.as_ref(),
                                )
                                .make_storable(&mut value)?;

                                let (value, reason) = store.upsert_entity(
                                    value,
                                    select,
                                    &mut ctx,
                                )?;
                                yield RespMessage::Element(value, reason);
                            }
                            Some(State::Delete(_, def_id)) => {
                                let deleted = store.delete_entities(vec![value], *def_id, &mut ctx)?;

                                for deleted in deleted {
                                    yield RespMessage::Element(Value::boolean(deleted), DataOperation::Deleted);
                                }
                            }
                            None => {
                                Err(DomainError::data_store("invalid transaction state"))?
                            }
                        }
                    }
                    ReqMessage::CrdtGet(vertex_addr, prop_id) => {
                        let octets = self.store.write().await.crdt_to_bytes(vertex_addr, prop_id)?;
                        yield RespMessage::SequenceStart(0);
                        if let Some(octets) = octets {
                            yield RespMessage::Element(Value::octet_sequence(octets), DataOperation::Queried);
                        }
                        yield RespMessage::SequenceEnd(0, None);
                    },
                    ReqMessage::CrdtSaveIncremental(vertex_addr, prop_id, _heads, payload) => {
                        self.store.write().await.crdt_save_incremental(vertex_addr, prop_id, payload)?;
                        yield RespMessage::SequenceStart(0);
                        yield RespMessage::SequenceEnd(0, None);
                    },
                }
            }

            let store = self.store.read().await;
            ctx.check.check_deferred(&store, &self.ontology)?;

            if let Some(write_stats) = ctx.write_stats.finish(ctx.system) {
                yield RespMessage::WriteComplete(Box::new(write_stats));
                let _ = self.datastore_mutated.send(());
            }
        }
        .boxed())
    }
}

#[derive(Clone, Copy, Default)]
pub struct InMemoryDataStoreFactory;

#[async_trait::async_trait]
impl DataStoreFactory for InMemoryDataStoreFactory {
    async fn new_api(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        self.new_api_sync(persisted, params)
    }
}

impl DataStoreFactorySync for InMemoryDataStoreFactory {
    fn new_api_sync(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        Ok(Arc::new(InMemoryDb::new(
            persisted,
            params.ontology,
            params.system,
            params.datastore_mutated,
        )))
    }
}

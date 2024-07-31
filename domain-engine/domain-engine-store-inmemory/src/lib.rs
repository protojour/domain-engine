#![forbid(unsafe_code)]

use core::{DbContext, EdgeColumn, EdgeVectorData};
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use domain_engine_core::data_store::{DataStoreFactory, DataStoreFactorySync};
use domain_engine_core::object_generator::ObjectGenerator;
use domain_engine_core::system::ArcSystemApi;
use domain_engine_core::transact::{DataOperation, ReqMessage, RespMessage};
use domain_engine_core::{DomainError, Session};
use fnv::{FnvHashMap, FnvHashSet};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::domain::EdgeCardinalFlags;
use ontol_runtime::ontology::{config::DataStoreConfig, Ontology};
use ontol_runtime::query::select::Select;
use ontol_runtime::{DefId, EdgeId, PackageId};
use tokio::sync::RwLock;

use domain_engine_core::{data_store::DataStoreAPI, DomainResult};
use tracing::debug;

use crate::core::{DynamicKey, HyperEdgeTable, InMemoryStore, VertexTable};

mod core;
mod filter;
mod query;
mod sort;
mod write;

#[derive(Clone)]
pub struct InMemoryDb {
    store: Arc<RwLock<InMemoryStore>>,
    context: DbContext,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn transact(
        &self,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        self.clone().transact_inner(messages).await
    }
}

impl InMemoryDb {
    fn new(
        package_ids: &BTreeSet<PackageId>,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> Self {
        let mut collections: FnvHashMap<DefId, VertexTable<DynamicKey>> = Default::default();
        let mut hyper_edges: FnvHashMap<EdgeId, HyperEdgeTable> = Default::default();

        for package_id in package_ids {
            let domain = ontology.find_domain(*package_id).unwrap();

            for def in domain.defs() {
                if let Some(entity) = def.entity() {
                    debug!("new collection {:?} (`{}`)", def.id, &ontology[entity.name]);

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
                            unique: cardinal.flags.contains(EdgeCardinalFlags::UNIQUE),
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
            context: DbContext { ontology, system },
        }
    }

    async fn transact_inner(
        self,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        enum State {
            Insert(Select),
            Update(Select),
            Upsert(Select),
            Delete(DefId),
        }

        let mut state: Option<State> = None;

        Ok(async_stream::try_stream! {
            for await req in messages {
                match req? {
                    ReqMessage::Query(op_seq, select) => {
                        let store = self.store.read().await;
                        let sequence = store.query_entities(&select, &self.context)?;

                        yield RespMessage::SequenceStart(op_seq, sequence.sub().map(|sub_seq| Box::new(sub_seq.clone())));

                        for item in sequence.into_elements() {
                            yield RespMessage::Element(item, DataOperation::Queried);
                        }
                    }
                    ReqMessage::Insert(op_seq, select) => {
                        state = Some(State::Insert(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Update(op_seq, select) => {
                        state = Some(State::Update(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Upsert(op_seq, select) => {
                        state = Some(State::Upsert(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Delete(op_seq, def_id) => {
                        state = Some(State::Delete(def_id));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Argument(mut value) => {
                        let mut store = self.store.write().await;

                        match state.as_ref() {
                            Some(State::Insert(select)) => {
                                ObjectGenerator::new(
                                    ProcessorMode::Create,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(&mut value);

                                let value = store.write_new_entity(
                                    value,
                                    select,
                                    &self.context,
                                )?;

                                yield RespMessage::Element(value, DataOperation::Inserted);
                            }
                            Some(State::Update(select)) => {
                                ObjectGenerator::new(
                                    ProcessorMode::Update,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(&mut value);

                                let value = store.update_entity(
                                    value,
                                    select,
                                    &self.context,
                                )?;

                                yield RespMessage::Element(value, DataOperation::Updated);
                            }
                            Some(State::Upsert(select)) => {
                                ObjectGenerator::new(
                                    ProcessorMode::Create,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(&mut value);

                                let (value, reason) = store.upsert_entity(
                                    value,
                                    select,
                                    &self.context,
                                )?;
                                yield RespMessage::Element(value, reason);
                            }
                            Some(State::Delete(def_id)) => {
                                let deleted = store.delete_entities(vec![value], *def_id)?;

                                for deleted in deleted {
                                    yield RespMessage::Element(self.context.ontology.bool_value(deleted), DataOperation::Deleted);
                                }
                            }
                            None => {
                                Err(DomainError::DataStore(anyhow!("invalid transaction state")))?
                            }
                        }
                    }
                }
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
        package_ids: &BTreeSet<PackageId>,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        self.new_api_sync(package_ids, config, session, ontology, system)
    }
}

impl DataStoreFactorySync for InMemoryDataStoreFactory {
    fn new_api_sync(
        &self,
        package_ids: &BTreeSet<PackageId>,
        _config: DataStoreConfig,
        _session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        Ok(Box::new(InMemoryDb::new(package_ids, ontology, system)))
    }
}

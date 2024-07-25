#![forbid(unsafe_code)]

use core::{DbContext, EdgeColumn, EdgeVectorData};
use std::collections::BTreeSet;
use std::sync::Arc;

use domain_engine_core::data_store::{DataStoreFactory, DataStoreFactorySync};
use domain_engine_core::object_generator::ObjectGenerator;
use domain_engine_core::system::ArcSystemApi;
use domain_engine_core::Session;
use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::domain::EdgeCardinalFlags;
use ontol_runtime::ontology::{config::DataStoreConfig, Ontology};
use ontol_runtime::{DefId, EdgeId, PackageId};
use tokio::sync::RwLock;

use domain_engine_core::{
    data_store::{BatchWriteRequest, DataStoreAPI, Request, Response, WriteResponse},
    DomainResult,
};
use tracing::debug;

use crate::core::{DynamicKey, HyperEdgeTable, InMemoryStore, VertexTable};

mod core;
// mod filter;
mod filter;
mod query;
mod sort;
mod write;

pub struct InMemoryDb {
    store: RwLock<InMemoryStore>,
    context: DbContext,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn execute(
        &self,
        request: Request,
        _session: domain_engine_core::Session,
    ) -> DomainResult<Response> {
        self.exec_inner(request).await
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
            store: RwLock::new(InMemoryStore {
                vertices: collections,
                edges: hyper_edges,
                serial_counter: 0,
            }),
            context: DbContext { ontology, system },
        }
    }

    async fn exec_inner(&self, request: Request) -> DomainResult<Response> {
        match request {
            Request::Query(select) => {
                // debug!("{select:#?}");
                Ok(Response::Query(
                    self.store
                        .read()
                        .await
                        .query_entities(&select, &self.context)?,
                ))
            }
            Request::BatchWrite(write_requests) => {
                // debug!("{write_requests:#?}");

                let mut store = self.store.write().await;
                let mut responses = vec![];

                for write_request in write_requests {
                    match write_request {
                        BatchWriteRequest::Insert(mut entities, select) => {
                            for value in entities.iter_mut() {
                                ObjectGenerator::new(
                                    ProcessorMode::Create,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(value);
                            }

                            for value in entities {
                                responses.push(WriteResponse::Inserted(store.write_new_entity(
                                    value,
                                    &select,
                                    &self.context,
                                )?))
                            }
                        }
                        BatchWriteRequest::Update(mut entities, select) => {
                            for value in entities.iter_mut() {
                                ObjectGenerator::new(
                                    ProcessorMode::Update,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(value);
                            }

                            for value in entities {
                                responses.push(WriteResponse::Inserted(store.update_entity(
                                    value,
                                    &select,
                                    &self.context,
                                )?))
                            }
                        }
                        BatchWriteRequest::Upsert(mut entities, select) => {
                            for value in entities.iter_mut() {
                                ObjectGenerator::new(
                                    ProcessorMode::Create,
                                    &self.context.ontology,
                                    self.context.system.as_ref(),
                                )
                                .generate_objects(value);
                            }

                            for value in entities {
                                responses.push(store.upsert_entity(
                                    value,
                                    &select,
                                    &self.context,
                                )?);
                            }
                        }
                        BatchWriteRequest::Delete(ids, def_id) => {
                            let deleted = store.delete_entities(ids, def_id)?;

                            responses.extend(deleted.into_iter().map(WriteResponse::Deleted));
                        }
                    }
                }

                Ok(Response::BatchWrite(responses))
            }
        }
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

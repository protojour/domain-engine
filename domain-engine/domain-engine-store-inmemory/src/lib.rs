use core::{DbContext, EdgeColumn, EdgeVectorData};
use std::sync::Arc;

use domain_engine_core::data_store::{DataStoreFactory, DataStoreFactorySync};
use domain_engine_core::object_generator::ObjectGenerator;
use domain_engine_core::system::ArcSystemApi;
use domain_engine_core::Session;
use fnv::FnvHashMap;
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::{config::DataStoreConfig, Ontology};
use ontol_runtime::{DefId, EdgeId, PackageId};
use tokio::sync::RwLock;

use domain_engine_core::{
    data_store::{BatchWriteRequest, BatchWriteResponse, DataStoreAPI, Request, Response},
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
    fn new(package_id: PackageId, ontology: Arc<Ontology>, system: ArcSystemApi) -> Self {
        let domain = ontology.find_domain(package_id).unwrap();

        let mut collections: FnvHashMap<DefId, VertexTable<DynamicKey>> = Default::default();
        let mut hyper_edges: FnvHashMap<EdgeId, HyperEdgeTable> = Default::default();

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
                    let vector = if cardinal.is_entity {
                        EdgeVectorData::Keys(vec![])
                    } else {
                        EdgeVectorData::Values(vec![])
                    };

                    EdgeColumn {
                        data: vector,
                        unique: cardinal.unique,
                    }
                })
                .collect();

            hyper_edges
                .entry(*edge_id)
                .or_insert_with(|| HyperEdgeTable { columns });
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

                            responses.push(BatchWriteResponse::Inserted(
                                entities
                                    .into_iter()
                                    .map(|entity| {
                                        store.write_new_entity(entity, &select, &self.context)
                                    })
                                    .collect::<DomainResult<_>>()?,
                            ));
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

                            responses.push(BatchWriteResponse::Updated(
                                entities
                                    .into_iter()
                                    .map(|entity| {
                                        store.update_entity(entity, &select, &self.context)
                                    })
                                    .collect::<DomainResult<_>>()?,
                            ));
                        }
                        BatchWriteRequest::Delete(ids, def_id) => {
                            responses.push(BatchWriteResponse::Deleted(
                                store.delete_entities(ids, def_id)?,
                            ));
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
        package_id: PackageId,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        self.new_api_sync(package_id, config, session, ontology, system)
    }
}

impl DataStoreFactorySync for InMemoryDataStoreFactory {
    fn new_api_sync(
        &self,
        package_id: PackageId,
        _config: DataStoreConfig,
        _session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        Ok(Box::new(InMemoryDb::new(package_id, ontology, system)))
    }
}

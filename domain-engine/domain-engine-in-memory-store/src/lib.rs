use core::DbContext;
use std::sync::Arc;

use domain_engine_core::data_store::{DataStoreFactory, DataStoreFactorySync};
use domain_engine_core::object_generator::ObjectGenerator;
use domain_engine_core::system::ArcSystemApi;
use domain_engine_core::Session;
use fnv::FnvHashMap;
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::{config::DataStoreConfig, Ontology};
use ontol_runtime::property::ValueCardinality;
use ontol_runtime::{DefId, PackageId};
use tokio::sync::RwLock;

use domain_engine_core::{
    data_store::{BatchWriteRequest, BatchWriteResponse, DataStoreAPI, Request, Response},
    DomainResult,
};
use tracing::debug;

use crate::core::{DynamicKey, EdgeCollection, EntityTable, InMemoryStore};

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

        let mut collections: FnvHashMap<DefId, EntityTable<DynamicKey>> = Default::default();
        let mut edge_collections: FnvHashMap<DefId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if let Some(entity_info) = type_info.entity_info() {
                debug!(
                    "new collection {:?} (`{}`)",
                    type_info.def_id, &ontology[entity_info.name]
                );

                collections.insert(type_info.def_id, Default::default());
            }
        }

        for (edge_id, edge_info) in domain.edges() {
            edge_collections
                .entry(*edge_id)
                .or_insert_with(|| EdgeCollection {
                    edges: vec![],
                    subject_unique: matches!(
                        edge_info.cardinals[0].cardinality.1,
                        ValueCardinality::Unit
                    ),
                    object_unique: matches!(
                        edge_info.cardinals[1].cardinality.1,
                        ValueCardinality::Unit
                    ),
                });
        }

        Self {
            store: RwLock::new(InMemoryStore {
                collections,
                edge_collections,
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

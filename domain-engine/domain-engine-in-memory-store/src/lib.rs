use domain_engine_core::data_store::{DataStoreFactory, DataStoreFactorySync};
use domain_engine_core::object_generator::ObjectGenerator;
use fnv::FnvHashMap;
use ontol_runtime::config::DataStoreConfig;
use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_runtime::ontology::DataRelationshipSource;
use ontol_runtime::{ontology::Ontology, DefId, PackageId, RelationshipId};
use tokio::sync::RwLock;

use domain_engine_core::{
    data_store::{BatchWriteRequest, BatchWriteResponse, DataStoreAPI, Request, Response},
    DomainEngine, DomainResult,
};

use crate::core::{DynamicKey, EdgeCollection, EntityTable, InMemoryStore};

mod core;
mod filter;
mod query;
mod write;

#[derive(Debug)]
pub struct InMemoryDb {
    #[allow(unused)]
    package_id: PackageId,
    store: RwLock<InMemoryStore>,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn execute(&self, request: Request, engine: &DomainEngine) -> DomainResult<Response> {
        match request {
            Request::Query(select) => {
                // debug!("{select:#?}");
                Ok(Response::Query(
                    self.store.read().await.query_entities(&select, engine)?,
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
                                ObjectGenerator::new(engine, ProcessorMode::Create)
                                    .generate_objects(value);
                            }

                            responses.push(BatchWriteResponse::Inserted(
                                entities
                                    .into_iter()
                                    .map(|entity| store.write_new_entity(entity, &select, engine))
                                    .collect::<DomainResult<_>>()?,
                            ));
                        }
                        BatchWriteRequest::Update(mut entities, select) => {
                            for value in entities.iter_mut() {
                                ObjectGenerator::new(engine, ProcessorMode::Update)
                                    .generate_objects(value);
                            }

                            responses.push(BatchWriteResponse::Updated(
                                entities
                                    .into_iter()
                                    .map(|entity| store.update_entity(entity, &select, engine))
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

impl InMemoryDb {
    pub fn new(ontology: &Ontology, package_id: PackageId) -> Self {
        let domain = ontology.find_domain(package_id).unwrap();

        let mut collections: FnvHashMap<DefId, EntityTable<DynamicKey>> = Default::default();
        let mut edge_collections: FnvHashMap<RelationshipId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if type_info.entity_info.is_some() {
                collections.insert(type_info.def_id, Default::default());

                for (property_id, entity_relationship) in type_info.entity_relationships() {
                    match entity_relationship.source {
                        DataRelationshipSource::Inherent => {
                            let relationship_id = property_id.relationship_id;
                            edge_collections.entry(relationship_id).or_insert_with(|| {
                                EdgeCollection {
                                    edges: vec![],
                                    subject_cardinality: entity_relationship.subject_cardinality,
                                    object_cardinality: entity_relationship.object_cardinality,
                                }
                            });
                        }
                        DataRelationshipSource::ByUnionProxy => {}
                    }
                }
            }
        }

        Self {
            package_id,
            store: RwLock::new(InMemoryStore {
                collections,
                edge_collections,
                int_id_counter: 0,
            }),
        }
    }
}

#[derive(Default)]
pub struct InMemoryDataStoreFactory;

#[async_trait::async_trait]
impl DataStoreFactory for InMemoryDataStoreFactory {
    async fn new_api(
        &self,
        config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        self.new_api_sync(config, ontology, package_id)
    }
}

impl DataStoreFactorySync for InMemoryDataStoreFactory {
    fn new_api_sync(
        &self,
        _config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        Ok(Box::new(InMemoryDb::new(ontology, package_id)))
    }
}

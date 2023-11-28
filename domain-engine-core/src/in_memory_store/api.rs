use fnv::FnvHashMap;
use ontol_runtime::{ontology::Ontology, DefId, PackageId, RelationshipId};
use tokio::sync::RwLock;

use crate::data_store::{BatchWriteRequest, BatchWriteResponse, DataStoreAPI, Request, Response};
use crate::domain_engine::DomainEngine;
use crate::domain_error::DomainResult;

use super::in_memory_core::{DynamicKey, EdgeCollection, EntityTable, InMemoryStore};

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
            Request::Query(select) => Ok(Response::Query(
                self.store.read().await.query_entities(&select, engine)?,
            )),
            Request::BatchWrite(write_requests) => {
                let mut store = self.store.write().await;
                let mut responses = vec![];

                for write_request in write_requests {
                    match write_request {
                        BatchWriteRequest::Insert(entities, select) => {
                            let mut values = vec![];

                            for entity in entities {
                                let value =
                                    store.write_new_entity(entity, select.clone(), engine)?;
                                values.push(value);
                            }

                            responses.push(BatchWriteResponse::Inserted(values));
                        }
                        BatchWriteRequest::Update(_entities, _select) => {
                            todo!()
                        }
                        BatchWriteRequest::Delete(_ids, _def_id) => {
                            todo!()
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

                for (property_id, _entity_relationship) in type_info.entity_relationships() {
                    let relationship_id = property_id.relationship_id;
                    edge_collections
                        .entry(relationship_id)
                        .or_insert_with(|| EdgeCollection { edges: vec![] });
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

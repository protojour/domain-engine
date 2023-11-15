use fnv::FnvHashMap;
use ontol_runtime::{ontology::Ontology, DefId, PackageId, RelationshipId};
use tokio::sync::RwLock;

use crate::data_store::{DataStoreAPI, Request, Response};
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
            Request::StoreNewEntity(value, select) => Ok(Response::StoreNewEntity(
                self.store
                    .write()
                    .await
                    .write_new_entity(value, select, engine)?,
            )),
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

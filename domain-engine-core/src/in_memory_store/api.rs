use fnv::FnvHashMap;
use ontol_runtime::{
    env::Env,
    query::{EntityQuery, Query},
    value::{Attribute, Value},
    DefId, PackageId, RelationshipId,
};
use tokio::sync::RwLock;

use crate::data_store::DataStoreAPI;
use crate::domain_engine::DomainEngine;
use crate::domain_error::DomainResult;

use super::store::{DynamicKey, EdgeCollection, EntityTable, InMemoryStore};

#[derive(Debug)]
pub struct InMemoryDb {
    #[allow(unused)]
    package_id: PackageId,
    store: RwLock<InMemoryStore>,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn query(
        &self,
        engine: &DomainEngine,
        query: EntityQuery,
    ) -> DomainResult<Vec<Attribute>> {
        Ok(self
            .store
            .read()
            .await
            .query_entities(engine, &query)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn store_new_entity(
        &self,
        engine: &DomainEngine,
        entity: Value,
        query: Query,
    ) -> DomainResult<Value> {
        self.store
            .write()
            .await
            .write_new_entity(engine, entity, query)
    }
}

impl InMemoryDb {
    pub fn new(env: &Env, package_id: PackageId) -> Self {
        let domain = env.find_domain(package_id).unwrap();

        let mut collections: FnvHashMap<DefId, EntityTable<DynamicKey>> = Default::default();
        let mut edge_collections: FnvHashMap<RelationshipId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if let Some(entity_info) = &type_info.entity_info {
                collections.insert(type_info.def_id, Default::default());

                for (property_id, _entity_relationship) in &entity_info.entity_relationships {
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

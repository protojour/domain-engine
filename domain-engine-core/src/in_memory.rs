#![allow(dead_code, unused)]

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    env::{Domain, Env},
    value::Value,
    DefId, PackageId, RelationshipId,
};
use tracing::debug;

use crate::{data_source::DataSourceAPI, DomainError};

#[derive(Debug)]
pub struct InMemory {
    package_id: PackageId,
    collections: FnvHashMap<DefId, Collection>,
    edge_collections: FnvHashMap<RelationshipId, EdgeCollection>,
}

#[derive(Debug)]
struct Collection {
    entities: IndexMap<Value, Value>,
}

#[derive(Debug)]
struct EdgeCollection {
    edges: Vec<Edge>,
}

#[derive(Debug)]
struct Edge {
    from: Value,
    to: Value,
    params: Value,
}

impl InMemory {
    pub fn from_domain(package_id: PackageId, domain: &Domain) -> Self {
        let mut collections: FnvHashMap<DefId, Collection> = Default::default();
        let edge_collections: FnvHashMap<RelationshipId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if let Some(entity_info) = &type_info.entity_info {
                collections.insert(
                    type_info.def_id,
                    Collection {
                        entities: Default::default(),
                    },
                );
            }
        }

        let in_memory = Self {
            package_id,
            collections,
            edge_collections,
        };

        debug!("in_memory db: {in_memory:?}");

        in_memory
    }
}

#[async_trait::async_trait]
impl DataSourceAPI for InMemory {
    async fn query(&self, env: &Env, def_id: DefId) -> Result<Vec<Value>, DomainError> {
        let collection = match self.collections.get(&def_id) {
            Some(collection) => collection,
            None => return Ok(vec![]),
        };

        Ok(vec![])
    }
}

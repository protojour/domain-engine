use std::collections::{BTreeMap, HashSet};

use anyhow::anyhow;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    value::{Attribute, Data, PropertyId, Value},
    DefId, RelationshipId,
};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::warn;

use crate::{DomainError, DomainResult};

#[derive(Debug)]
pub(super) struct InMemoryStore {
    pub collections: FnvHashMap<DefId, EntityTable<DynamicKey>>,
    pub edge_collections: FnvHashMap<RelationshipId, EdgeCollection>,
    pub int_id_counter: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) enum DynamicKey {
    Text(String),
    Octets(SmallVec<[u8; 16]>),
    Int(i64),
}

pub type EntityTable<K> = IndexMap<K, BTreeMap<PropertyId, Attribute>>;

#[derive(Debug)]
pub(super) struct EdgeCollection {
    pub edges: Vec<Edge>,
}

#[derive(Debug)]
pub(super) struct Edge {
    pub from: EntityKey,
    pub to: EntityKey,
    pub params: Value,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(super) struct EntityKey {
    pub type_def_id: DefId,
    pub dynamic_key: DynamicKey,
}

impl InMemoryStore {
    pub fn delete_entities(&mut self, ids: Vec<Value>, def_id: DefId) -> DomainResult<Vec<bool>> {
        let mut result_vec = Vec::with_capacity(ids.len());
        let mut deleted_set: HashSet<EntityKey> = HashSet::with_capacity(ids.len());

        let collection = self
            .collections
            .get_mut(&def_id)
            .ok_or_else(|| DomainError::DataStore(anyhow!("Collection not found")))?;

        for id in ids {
            let dynamic_key = Self::extract_dynamic_key(&id.data)?;

            let status = if collection.remove(&dynamic_key).is_some() {
                deleted_set.insert(EntityKey {
                    type_def_id: def_id,
                    dynamic_key,
                });
                true
            } else {
                false
            };

            result_vec.push(status);
        }

        // filter deleted entity from edge collections
        for (_, edge_collection) in self.edge_collections.iter_mut() {
            edge_collection
                .edges
                .retain(|edge| !deleted_set.contains(&edge.from) && !deleted_set.contains(&edge.to))
        }

        Ok(result_vec)
    }

    pub fn extract_dynamic_key(id_data: &Data) -> DomainResult<DynamicKey> {
        match id_data {
            Data::Struct(struct_map) => {
                if struct_map.len() != 1 {
                    warn!("struct map was not 1: {struct_map:?}");
                    return Err(DomainError::InherentIdNotFound);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::extract_dynamic_key(&attribute.1.value.data)
            }
            Data::Text(string) => Ok(DynamicKey::Text(string.clone())),
            Data::OctetSequence(octets) => Ok(DynamicKey::Octets(octets.clone())),
            Data::I64(int) => Ok(DynamicKey::Int(*int)),
            other => {
                warn!("inherent id from {other:?}");
                Err(DomainError::InherentIdNotFound)
            }
        }
    }

    pub fn look_up_entity(
        &self,
        def_id: DefId,
        dynamic_key: &DynamicKey,
    ) -> Option<&BTreeMap<PropertyId, Attribute>> {
        let collection = self.collections.get(&def_id)?;
        collection.get(dynamic_key)
    }
}

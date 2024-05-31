use std::{collections::HashSet, sync::Arc};

use anyhow::anyhow;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    ontology::{
        domain::{DataRelationshipInfo, TypeInfo},
        Ontology,
    },
    property::PropertyId,
    value::{Attribute, Serial, Value},
    DefId,
};
use smallvec::SmallVec;
use tracing::warn;

use domain_engine_core::{system::ArcSystemApi, DomainError, DomainResult};

pub(super) struct InMemoryStore {
    pub collections: FnvHashMap<DefId, EntityTable<DynamicKey>>,
    pub edge_collections: FnvHashMap<DefId, EdgeCollection>,
    pub serial_counter: u64,
}

pub(super) struct DbContext {
    pub ontology: Arc<Ontology>,
    pub system: ArcSystemApi,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) enum DynamicKey {
    Text(String),
    Octets(SmallVec<[u8; 16]>),
    Serial(u64),
}

pub type EntityTable<K> = IndexMap<K, FnvHashMap<PropertyId, Attribute>>;

#[derive(Debug)]
pub(super) struct EdgeCollection {
    pub edges: Vec<Edge>,
    pub subject_unique: bool,
    pub object_unique: bool,
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
            let dynamic_key = Self::extract_dynamic_key(&id)?;

            let status = if collection.swap_remove(&dynamic_key).is_some() {
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

    pub fn extract_dynamic_key(id_value: &Value) -> DomainResult<DynamicKey> {
        match id_value {
            Value::Struct(struct_map, _) | Value::StructUpdate(struct_map, _) => {
                if struct_map.len() != 1 {
                    warn!("struct map was not 1: {struct_map:?}");
                    return Err(DomainError::InherentIdNotFound);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::extract_dynamic_key(&attribute.1.val)
            }
            Value::Text(string, _) => Ok(DynamicKey::Text(string.as_str().into())),
            Value::OctetSequence(octets, _) => {
                Ok(DynamicKey::Octets(octets.iter().cloned().collect()))
            }
            Value::Serial(Serial(value), _) => Ok(DynamicKey::Serial(*value)),
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
    ) -> Option<&FnvHashMap<PropertyId, Attribute>> {
        let collection = self.collections.get(&def_id)?;
        collection.get(dynamic_key)
    }
}

pub(crate) fn find_data_relationship<'a>(
    type_info: &'a TypeInfo,
    property_id: &PropertyId,
) -> DomainResult<&'a DataRelationshipInfo> {
    type_info
        .data_relationships
        .get(property_id)
        .ok_or_else(|| {
            warn!(
                "data relationship {property_id:?} not found in {keys:?}",
                keys = type_info.data_relationships.keys()
            );

            DomainError::DataStoreBadRequest(anyhow!(
                "data relationship {def_id:?} -> {property_id} does not exist",
                def_id = type_info.def_id
            ))
        })
}

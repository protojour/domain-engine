use std::collections::BTreeMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    value::{Attribute, Data, PropertyId, Value},
    DefId, RelationshipId,
};
use smallvec::SmallVec;
use smartstring::alias::String;

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
    pub fn extract_dynamic_key(id_data: &Data) -> DomainResult<DynamicKey> {
        match id_data {
            Data::Struct(struct_map) => {
                if struct_map.len() != 1 {
                    return Err(DomainError::InherentIdNotFound);
                }

                let attribute = struct_map.iter().next().unwrap();
                Self::extract_dynamic_key(&attribute.1.value.data)
            }
            Data::Text(string) => Ok(DynamicKey::Text(string.clone())),
            Data::OctetSequence(octets) => Ok(DynamicKey::Octets(octets.clone())),
            Data::I64(int) => Ok(DynamicKey::Int(*int)),
            _ => Err(DomainError::InherentIdNotFound),
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

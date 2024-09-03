use std::collections::{BTreeSet, HashSet};

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use itertools::Itertools;
use ontol_runtime::{
    attr::Attr,
    ontology::{
        domain::{DataRelationshipInfo, Def},
        Ontology,
    },
    tuple::CardinalIdx,
    value::{Serial, Value},
    DefId, PropId,
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{debug, warn};

use domain_engine_core::{
    domain_error::DomainErrorKind, system::SystemAPI, DomainError, DomainResult,
};

use crate::constraint::ConstraintCheck;

pub(super) struct InMemoryStore {
    pub vertices: FnvHashMap<DefId, VertexTable<DynamicKey>>,
    pub edges: FnvHashMap<DefId, HyperEdgeTable>,
    pub serial_counter: u64,
}

pub(super) struct DbContext<'a> {
    pub ontology: &'a Ontology,
    pub system: &'a dyn SystemAPI,
    pub check: ConstraintCheck,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub(super) enum DynamicKey {
    Text(String),
    Octets(SmallVec<u8, 16>),
    Serial(u64),
}

impl AsRef<DynamicKey> for DynamicKey {
    fn as_ref(&self) -> &DynamicKey {
        self
    }
}

pub type VertexTable<K> = IndexMap<K, FnvHashMap<PropId, Attr>>;

#[derive(Debug)]
pub(super) struct HyperEdgeTable {
    pub columns: Vec<EdgeColumn>,
}

#[derive(Debug)]
pub(super) struct EdgeColumn {
    pub data: EdgeVectorData,
    pub vertex_union: FnvHashSet<DefId>,
    pub pinned: bool,
}

#[derive(Debug)]
pub(super) enum EdgeData<K> {
    Key(K),
    Value(Value),
}

#[derive(Debug)]
pub(super) enum EdgeVectorData {
    Keys(Vec<VertexKey<DynamicKey>>),
    Values(Vec<Value>),
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub(super) struct VertexKey<K: AsRef<DynamicKey> = DynamicKey> {
    pub type_def_id: DefId,
    pub dynamic_key: K,
}

impl<K: AsRef<DynamicKey>> AsRef<DynamicKey> for VertexKey<K> {
    fn as_ref(&self) -> &DynamicKey {
        self.dynamic_key.as_ref()
    }
}

pub enum EdgeColumnMatch {
    VertexIdOf(DefId),
    VertexValue(DefId),
    EdgeValue,
}

impl InMemoryStore {
    pub fn match_edge_column(
        &self,
        edge_id: DefId,
        cardinal_idx: CardinalIdx,
        value_def_id: DefId,
        ctx: &DbContext,
    ) -> EdgeColumnMatch {
        let edge_store = self.edges.get(&edge_id).expect("no edge store");
        let column = &edge_store.columns[cardinal_idx.0 as usize];
        match &column.data {
            EdgeVectorData::Keys(_) => {
                if column.vertex_union.contains(&value_def_id) {
                    EdgeColumnMatch::VertexValue(value_def_id)
                } else {
                    let vertex_def_id = column
                        .vertex_union
                        .iter()
                        .find(|vertex_def_id| {
                            let entity = ctx.ontology.def(**vertex_def_id).entity().unwrap();
                            entity.id_value_def_id == value_def_id
                        })
                        .unwrap_or_else(|| {
                            panic!("cardinal {cardinal_idx}: Corresponding entity def id not found for the given ID {value_def_id:?}. vertex_union = {:?}", column.vertex_union)
                        });

                    EdgeColumnMatch::VertexIdOf(*vertex_def_id)
                }
            }
            EdgeVectorData::Values(_) => EdgeColumnMatch::EdgeValue,
        }
    }

    pub fn delete_entities(&mut self, ids: Vec<Value>, def_id: DefId) -> DomainResult<Vec<bool>> {
        let mut result_vec = Vec::with_capacity(ids.len());
        let mut deleted_set: HashSet<VertexKey> = HashSet::with_capacity(ids.len());

        let collection = self
            .vertices
            .get_mut(&def_id)
            .ok_or_else(|| DomainError::data_store("Collection not found"))?;

        for id in ids {
            let dynamic_key = Self::extract_dynamic_key(&id)?;

            let status = if collection.swap_remove(&dynamic_key).is_some() {
                deleted_set.insert(VertexKey {
                    type_def_id: def_id,
                    dynamic_key,
                });
                true
            } else {
                false
            };

            result_vec.push(status);
        }

        // cascade delete all edges
        for (_, edge_store) in self.edges.iter_mut() {
            let mut edge_delete_set: BTreeSet<usize> = Default::default();

            for column in &edge_store.columns {
                if let EdgeVectorData::Keys(keys) = &column.data {
                    for (index, key) in keys.iter().enumerate() {
                        if deleted_set.contains(key) {
                            edge_delete_set.insert(index);
                        }
                    }
                }
            }

            edge_store.delete_edges(edge_delete_set);
        }

        Ok(result_vec)
    }

    pub fn extract_dynamic_key(id_value: &Value) -> DomainResult<DynamicKey> {
        match id_value {
            Value::Struct(struct_map, _) => {
                if struct_map.len() != 1 {
                    warn!("struct map was not 1: {struct_map:?}");
                    return Err(DomainErrorKind::InherentIdNotFound.into_error());
                }

                let value = struct_map
                    .values()
                    .next()
                    .unwrap()
                    .as_unit()
                    .ok_or(DomainErrorKind::InherentIdNotFound.into_error())?;
                Self::extract_dynamic_key(value)
            }
            Value::Text(string, _) => Ok(DynamicKey::Text(string.as_str().into())),
            Value::OctetSequence(seq, _) => Ok(DynamicKey::Octets(seq.0.iter().cloned().collect())),
            Value::Serial(Serial(value), _) => Ok(DynamicKey::Serial(*value)),
            other => {
                warn!("inherent id from {other:?}");
                Err(DomainErrorKind::InherentIdNotFound.into_error())
            }
        }
    }

    pub fn look_up_vertex(
        &self,
        def_id: DefId,
        dynamic_key: &DynamicKey,
    ) -> Option<&FnvHashMap<PropId, Attr>> {
        self.vertices.get(&def_id)?.get(dynamic_key)
    }
}

pub(crate) fn find_data_relationship<'a>(
    def: &'a Def,
    prop_id: &PropId,
) -> DomainResult<&'a DataRelationshipInfo> {
    def.data_relationships.get(prop_id).ok_or_else(|| {
        warn!(
            "data relationship {prop_id:?} not found in {keys:?}",
            keys = def.data_relationships.keys()
        );

        DomainError::data_store_bad_request(format!(
            "data relationship {def_id:?} -> {prop_id} does not exist",
            def_id = def.id
        ))
    })
}

impl HyperEdgeTable {
    pub(super) fn collect_column_eq<K: AsRef<DynamicKey>>(
        &self,
        cardinal_idx: CardinalIdx,
        data: &EdgeData<VertexKey<K>>,
        output: &mut BTreeSet<usize>,
    ) {
        let column = &self.columns[cardinal_idx.0 as usize];

        Self::collect_matching(&column.data, data, output);
    }

    pub(super) fn collect_unique_violations<K: AsRef<DynamicKey>>(
        &self,
        tuple: &[EdgeData<VertexKey<K>>],
        output: &mut BTreeSet<usize>,
    ) {
        for (cardinal_idx, column) in self.columns.iter().enumerate() {
            if !column.pinned {
                continue;
            }

            Self::collect_matching(&column.data, &tuple[cardinal_idx], output);
        }
    }

    fn collect_matching<K: AsRef<DynamicKey>>(
        vec_data: &EdgeVectorData,
        data: &EdgeData<VertexKey<K>>,
        output: &mut BTreeSet<usize>,
    ) {
        match (vec_data, data) {
            (EdgeVectorData::Keys(keys), EdgeData::Key(key)) => {
                output.extend(keys.iter().positions(|elem| {
                    elem.type_def_id == key.type_def_id
                        && &elem.dynamic_key == key.dynamic_key.as_ref()
                }));
            }
            (EdgeVectorData::Values(values), EdgeData::Value(value)) => {
                output.extend(values.iter().positions(|elem| elem == value));
            }
            _ => {}
        }
    }

    pub(super) fn push_tuple(&mut self, mut tuple: Vec<EdgeData<VertexKey>>) {
        for column in self.columns.iter_mut().rev() {
            let data = tuple.pop().unwrap();

            match (&mut column.data, data) {
                (EdgeVectorData::Keys(keys), EdgeData::Key(key)) => {
                    keys.push(key);
                }
                (EdgeVectorData::Values(values), EdgeData::Value(value)) => {
                    values.push(value);
                }
                _ => panic!(),
            }
        }
    }

    pub(super) fn delete_edges(&mut self, edge_set: BTreeSet<usize>) -> bool {
        fn retain_next(
            current: &mut usize,
            next_delete: &mut usize,
            to_delete: &mut impl Iterator<Item = usize>,
        ) -> bool {
            let retain = if *current == *next_delete {
                if let Some(next) = to_delete.next() {
                    *next_delete = next;
                }
                false
            } else {
                true
            };

            *current += 1;
            retain
        }

        if edge_set.is_empty() {
            return false;
        }

        debug!("delete edges {edge_set:?}");

        for column in &mut self.columns {
            let mut to_delete = edge_set.iter().copied();

            if let Some(mut next_delete) = to_delete.next() {
                let mut current: usize = 0;

                match &mut column.data {
                    EdgeVectorData::Keys(keys) => {
                        keys.retain(|_| retain_next(&mut current, &mut next_delete, &mut to_delete))
                    }
                    EdgeVectorData::Values(values) => values
                        .retain(|_| retain_next(&mut current, &mut next_delete, &mut to_delete)),
                }
            }
        }

        true
    }
}

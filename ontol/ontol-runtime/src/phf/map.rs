use std::fmt::Debug;

use arcstr::ArcStr;
use serde::{Deserialize, Serialize};

use crate::{
    debug::{Fmt, OntolDebug, OntolFormatter},
    ontology::{Ontology, OntologyInit},
};

use super::key::PhfKey;

/// A constant, string-keyed HashMap using perfect hashing.
///
/// It can be used by Ontology since it is built ahead-of-time by ontol-compiler.
///
/// This version is iterated in non-deterministic order (based on the hash function).
#[derive(Clone, Serialize, Deserialize)]
pub struct PhfMap<V> {
    key: u64,
    disps: Box<[(u32, u32)]>,
    entries: Box<[(PhfKey, V)]>,
}

/// A constant, indexed string-keyed HashMap using perfect hashing.
///
/// Indexed means that it gets iterated in deterministic/insertion order.
///
/// It can be used by Ontology since it is built ahead-of-time by ontol-compiler.
#[derive(Clone, Serialize, Deserialize)]
pub struct PhfIndexMap<V> {
    map: PhfMap<V>,
    order: Box<[usize]>,
}

impl<V> PhfMap<V> {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get_entry(key).is_some()
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.get_entry(key).map(|(_, value)| value)
    }

    pub fn get_entry(&self, key: &str) -> Option<(&ArcStr, &V)> {
        if self.disps.is_empty() {
            return None;
        }

        let hashes = phf_shared::hash(key, &self.key);
        let index = phf_shared::get_index(&hashes, &self.disps, self.entries.len());
        let entry = &self.entries[index as usize];
        let entry_key = &entry.0.string;

        if entry_key == key {
            Some((&entry.0.string, &entry.1))
        } else {
            None
        }
    }

    pub fn iter(&self) -> MapIter<V> {
        MapIter {
            entry_iter: self.entries.iter(),
        }
    }
}

impl<V> PhfIndexMap<V> {
    pub fn is_empty(&self) -> bool {
        self.map.entries.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get_entry(key).is_some()
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_entry(&self, key: &str) -> Option<(&ArcStr, &V)> {
        self.map.get_entry(key)
    }

    pub fn iter(&self) -> IndexMapIter<V> {
        IndexMapIter {
            order_iter: self.order.iter(),
            map: &self.map,
        }
    }
}

pub struct MapIter<'a, V> {
    entry_iter: std::slice::Iter<'a, (PhfKey, V)>,
}

impl<'a, V> Iterator for MapIter<'a, V> {
    type Item = (&'a PhfKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.entry_iter.next()?;
        Some((key, value))
    }
}

pub struct IndexMapIter<'a, V> {
    order_iter: std::slice::Iter<'a, usize>,
    map: &'a PhfMap<V>,
}

impl<'a, V> Iterator for IndexMapIter<'a, V> {
    type Item = (&'a PhfKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let next_index = self.order_iter.next()?;
        let (key, value) = &self.map.entries[*next_index];
        Some((key, value))
    }
}

impl<V: Debug> Debug for PhfMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (k, v) in self.iter() {
            map.entry(&k.string, v);
        }
        map.finish()
    }
}

impl<V: OntolDebug> OntolDebug for PhfMap<V> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.iter() {
            map.entry(key.arc_str(), &Fmt(ofmt, value));
        }

        map.finish()
    }
}

impl<V: Debug> Debug for PhfIndexMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (k, v) in self.iter() {
            map.entry(&k.string, v);
        }
        map.finish()
    }
}

impl<V: OntolDebug> OntolDebug for PhfIndexMap<V> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.iter() {
            map.entry(key.arc_str(), &Fmt(ofmt, value));
        }

        map.finish()
    }
}

impl<V> From<(u64, Box<[(u32, u32)]>, Box<[(PhfKey, V)]>)> for PhfMap<V> {
    fn from((key, disps, entries): (u64, Box<[(u32, u32)]>, Box<[(PhfKey, V)]>)) -> Self {
        Self {
            key,
            disps,
            entries,
        }
    }
}

impl<V> From<(PhfMap<V>, Box<[usize]>)> for PhfIndexMap<V> {
    fn from((map, order): (PhfMap<V>, Box<[usize]>)) -> Self {
        Self { map, order }
    }
}

impl<V> OntologyInit for PhfMap<V> {
    fn ontology_init(&mut self, ontology: &Ontology) {
        for entry in self.entries.iter_mut() {
            let key = &mut entry.0;
            key.string = ontology.get_text_constant(key.constant).clone();
        }
    }
}

impl<V> OntologyInit for PhfIndexMap<V> {
    fn ontology_init(&mut self, ontology: &Ontology) {
        self.map.ontology_init(ontology);
    }
}

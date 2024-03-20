use std::fmt::Debug;

use arcstr::ArcStr;
use fnv::FnvHashMap;
use phf_shared::PhfHash;
use serde::{Deserialize, Serialize};

use crate::{
    debug::{OntolDebug, OntolFormatter},
    ontology::{Ontology, OntologyInit},
};

use super::key::PhfKey;

/// A constant, string-keyed HashMap with using perfect hashing.
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

/// A constant, indexed string-keyed HashMap with using perfect hashing.
///
/// Indexed means that it gets iterated in deterministic/insertion order.
///
/// It can be used by Ontology since it is built ahead-of-time by ontol-compiler.
#[derive(Clone, Serialize, Deserialize)]
pub struct PhfIndexMap<V> {
    map: PhfMap<V>,
    order: Box<[usize]>,
}

impl<V> Default for PhfMap<V> {
    fn default() -> Self {
        Self::build([])
    }
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

    pub fn build(entries: impl IntoIterator<Item = (PhfKey, V)>) -> Self {
        let (map, _) = build(entries);
        map
    }
}

impl<V> Default for PhfIndexMap<V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            order: Default::default(),
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

    pub fn build(entries: impl IntoIterator<Item = (PhfKey, V)>) -> Self {
        let (map, relocations) = build(entries);

        // The relocation table needs to be inverted..
        // It tells where each input item moved _to_,
        // But the index map needs to know where it moved _from_,
        let mut inverse_order: FnvHashMap<usize, usize> = relocations
            .into_iter()
            .enumerate()
            .map(|(from, to)| (to, from))
            .collect();

        Self {
            map,
            order: (0..inverse_order.len())
                .map(|index| inverse_order.remove(&index).unwrap())
                .collect(),
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

impl<V: Debug> OntolDebug for PhfMap<V> {
    fn fmt(&self, _: &dyn OntolFormatter, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Debug>::fmt(self, f)
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

impl<V: Debug> OntolDebug for PhfIndexMap<V> {
    fn fmt(&self, _: &dyn OntolFormatter, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Debug>::fmt(self, f)
    }
}

fn build<V>(entries: impl IntoIterator<Item = (PhfKey, V)>) -> (PhfMap<V>, Vec<usize>) {
    // build two collections:
    // 1. a hash map of original index to target entry
    // 2. a vector of HashKey keys
    let (mut table, keys): (FnvHashMap<usize, (PhfKey, V)>, Vec<HashKey>) = entries
        .into_iter()
        .enumerate()
        .map(|(index, (key, value))| {
            let hash_key = HashKey(key.string.clone());
            let table_entry = (index, (key, value));

            (table_entry, hash_key)
        })
        .unzip();

    // generate the perfect hash function
    let state = phf_generator::generate_hash(&keys);

    (
        PhfMap {
            key: state.key,
            // reorder entries according to how the perfect hash function wants it:
            entries: state
                .map
                .iter()
                .map(|index| table.remove(index).unwrap())
                .collect(),
            disps: state.disps.into(),
        },
        state.map,
    )
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

struct HashKey(ArcStr);

impl PhfHash for HashKey {
    fn phf_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.phf_hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{mem::size_of, ops::Index};

    use arcstr::{literal, ArcStr};

    use crate::ontology::ontol::TextConstant;

    #[derive(Default)]
    struct Consts {
        constants: Vec<ArcStr>,
    }

    impl Consts {
        fn add(&mut self, c: ArcStr) -> PhfKey {
            let tc = TextConstant(self.constants.len() as u32);
            self.constants.push(c.clone());
            PhfKey {
                constant: tc,
                string: c,
            }
        }
    }

    impl Index<TextConstant> for Consts {
        type Output = str;

        fn index(&self, index: TextConstant) -> &Self::Output {
            &self.constants[index.0 as usize]
        }
    }

    struct ConstsAsArcStr<'u>(&'u Consts);

    impl<'u> Index<TextConstant> for ConstsAsArcStr<'u> {
        type Output = ArcStr;

        fn index(&self, index: TextConstant) -> &Self::Output {
            &self.0.constants[index.0 as usize]
        }
    }

    fn entries() -> Vec<(PhfKey, &'static str)> {
        let mut c = Consts::default();
        vec![
            (c.add(literal!("foobar")), "FOOBAR"),
            (c.add(literal!("yo")), "YO"),
            (c.add(literal!("foo")), "FOO"),
            (c.add(literal!("bar")), "BAR"),
            (c.add(literal!("a")), "A"),
            (c.add(literal!("b")), "B"),
            (c.add(literal!("c")), "C"),
            (c.add(literal!("d")), "D"),
            (c.add(literal!("e")), "E"),
            (c.add(literal!("f")), "F"),
            (c.add(literal!("g")), "G"),
            (c.add(literal!("h")), "H"),
            (c.add(literal!("i")), "I"),
            (c.add(literal!("j")), "J"),
            (c.add(literal!("k")), "K"),
            (c.add(literal!("l")), "L"),
            (c.add(literal!("m")), "M"),
            (c.add(literal!("n")), "N"),
            (c.add(literal!("o")), "O"),
            (c.add(literal!("p")), "P"),
        ]
    }

    #[test]
    fn empty_map() {
        let map: PhfMap<()> = PhfMap::build([]);

        assert_eq!(map.get("foo"), None);
        assert_eq!(map.get("bar"), None);
        assert_eq!(map.get("foobar"), None);
        assert_eq!(map.get("k"), None);
    }

    #[test]
    fn test_map() {
        let map = PhfMap::build(entries());

        assert_eq!(map.get("foo"), Some(&"FOO"));
        assert_eq!(map.get("bar"), Some(&"BAR"));
        assert_eq!(map.get("foobar"), Some(&"FOOBAR"));
        assert_eq!(map.get("k"), Some(&"K"));
    }

    #[test]
    fn test_index_map() {
        let map = PhfIndexMap::build(entries());

        assert_eq!(map.get("foo"), Some(&"FOO"));
        assert_eq!(map.get("bar"), Some(&"BAR"));
        assert_eq!(map.get("foobar"), Some(&"FOOBAR"));
        assert_eq!(map.get("k"), Some(&"K"));

        println!("{map:#?}");

        let mut iter = map.iter();

        assert_eq!(*iter.next().unwrap().1, "FOOBAR");
        assert_eq!(*iter.next().unwrap().1, "YO");
    }

    #[test]
    fn check_size() {
        type CheckSize = PhfMap<u64>;

        assert_eq!(size_of::<CheckSize>(), 40);
    }
}

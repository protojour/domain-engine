use arcstr::ArcStr;
use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    debug::OntolDebug,
    ontology::ontol::TextConstant,
    phf::{PhfIndexMap, PhfKey, PhfMap},
};
use phf_shared::PhfHash;

struct HashKey(ArcStr);

impl PhfHash for HashKey {
    fn phf_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.phf_hash(state);
    }
}

pub fn build_phf_map<V>(entries: impl IntoIterator<Item = (PhfKey, V)>) -> PhfMap<V> {
    build(entries).0
}

pub fn build_phf_index_map<V>(entries: impl IntoIterator<Item = (PhfKey, V)>) -> PhfIndexMap<V> {
    let (map, relocations) = build(entries);

    // The relocation table needs to be inverted..
    // It tells where each input item moved _to_,
    // But the index map needs to know where it moved _from_,
    let mut inverse_order: FnvHashMap<usize, usize> = relocations
        .into_iter()
        .enumerate()
        .map(|(from, to)| (to, from))
        .collect();

    PhfIndexMap::from((
        map,
        (0..inverse_order.len())
            .map(|index| inverse_order.remove(&index).unwrap())
            .collect(),
    ))
}

fn build<V>(entries: impl IntoIterator<Item = (PhfKey, V)>) -> (PhfMap<V>, Vec<usize>) {
    let mut duplication_guard: FnvHashSet<TextConstant> = Default::default();

    // build two collections:
    // 1. a hash map of original index to target entry
    // 2. a vector of HashKey keys
    let (mut table, keys): (FnvHashMap<usize, (PhfKey, V)>, Vec<HashKey>) = entries
        .into_iter()
        .enumerate()
        .map(|(index, (phf_key, value))| {
            let hash_key = HashKey(phf_key.string.clone());

            if !duplication_guard.insert(phf_key.constant) {
                panic!(
                    "BUG: duplicate key in phf map: {key:?}",
                    key = phf_key.debug(&())
                );
            }

            let table_entry = (index, (phf_key, value));

            (table_entry, hash_key)
        })
        .unzip();

    // generate the perfect hash function
    let state = phf_generator::generate_hash(&keys);

    (
        PhfMap::from((
            state.key,
            state.disps.into_boxed_slice(),
            // reorder entries according to how the perfect hash function wants it:
            state
                .map
                .iter()
                .map(|index| table.remove(index).unwrap())
                .collect::<Box<[_]>>(),
        )),
        state.map,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{mem::size_of, ops::Index};

    use arcstr::{literal, ArcStr};
    use ontol_runtime::ontology::ontol::TextConstant;

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
        let map: PhfMap<()> = build_phf_map([]);

        assert_eq!(map.get("foo"), None);
        assert_eq!(map.get("bar"), None);
        assert_eq!(map.get("foobar"), None);
        assert_eq!(map.get("k"), None);
    }

    #[test]
    fn test_map() {
        let map = build_phf_map(entries());

        assert_eq!(map.get("foo"), Some(&"FOO"));
        assert_eq!(map.get("bar"), Some(&"BAR"));
        assert_eq!(map.get("foobar"), Some(&"FOOBAR"));
        assert_eq!(map.get("k"), Some(&"K"));
    }

    #[test]
    fn test_index_map() {
        let map = build_phf_index_map(entries());

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

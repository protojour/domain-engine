use std::ops::Index;

use arcstr::ArcStr;
use fnv::FnvHashMap;

use crate::text::TextConstant;

#[derive(Debug)]
struct PhfConstMap<K, V> {
    key: u64,

    // It would be better if the map stored shallow clones of ArcStr
    // directly, instead of needing to ask the ontology every time.
    // But then the map wouldn't be directly deserializable anymore.
    // There would need to be an initialization step where T<TextConstant> are
    // converted to T<ArcStr> by doing the lookup once.
    entries: Box<[(K, V)]>,

    disps: Box<[(u32, u32)]>,
}

impl<V> PhfConstMap<TextConstant, V> {
    pub fn get<S>(&self, key: &str, source: &S) -> Option<&V>
    where
        S: Index<TextConstant, Output = str>,
    {
        self.get_entry(key, source).map(|(_, value)| value)
    }

    pub fn get_entry<S>(&self, key: &str, source: &S) -> Option<(TextConstant, &V)>
    where
        S: Index<TextConstant, Output = str>,
    {
        let hashes = phf_shared::hash(key, &self.key);
        let index = phf_shared::get_index(&hashes, &self.disps, self.entries.len());
        let entry = &self.entries[index as usize];
        let entry_key_str = &source[entry.0];

        if entry_key_str == key {
            Some((entry.0, &entry.1))
        } else {
            None
        }
    }
}

impl<V> PhfConstMap<ArcStr, V> {
    pub fn get(&self, key: &str) -> Option<&V> {
        self.get_entry(key).map(|(_, value)| value)
    }

    pub fn get_entry(&self, key: &str) -> Option<(&str, &V)> {
        let hashes = phf_shared::hash(key, &self.key);
        let index = phf_shared::get_index(&hashes, &self.disps, self.entries.len());
        let entry = &self.entries[index as usize];

        if entry.0 == key {
            Some((&entry.0, &entry.1))
        } else {
            None
        }
    }
}

impl<V> PhfConstMap<TextConstant, V> {
    pub fn build<S>(entries: impl IntoIterator<Item = (TextConstant, V)>, source: &S) -> Self
    where
        S: Index<TextConstant, Output = str>,
    {
        // build two collections:
        // 1. a hash map of original index to target entry
        // 2. a vector of keys as &str
        let (mut table, keys): (FnvHashMap<usize, (TextConstant, V)>, Vec<&str>) = entries
            .into_iter()
            .enumerate()
            .map(|(index, (text_const, value))| {
                let table_entry = (index, (text_const, value));
                let key_str = &source[text_const];

                (table_entry, key_str)
            })
            .unzip();

        // generate the perfect hash
        let hash = phf_generator::generate_hash(&keys);

        Self {
            key: hash.key,
            // reorder entries according to how the perfect hash function wants it:
            entries: hash
                .map
                .into_iter()
                .map(|index| table.remove(&index).unwrap())
                .collect(),
            disps: hash.disps.into(),
        }
    }

    pub fn import_keys<S>(self, source: &S) -> PhfConstMap<ArcStr, V>
    where
        S: Index<TextConstant, Output = ArcStr>,
    {
        PhfConstMap {
            key: self.key,
            entries: Vec::from(self.entries)
                .into_iter()
                .map(|(key, val)| (source[key].clone(), val))
                .collect(),
            disps: self.disps,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, ops::Index};

    use arcstr::{literal, ArcStr};

    use crate::text::TextConstant;

    use super::PhfConstMap;

    #[derive(Default)]
    struct Consts {
        constants: Vec<ArcStr>,
    }

    impl Consts {
        fn add(&mut self, c: ArcStr) -> TextConstant {
            let tc = TextConstant(self.constants.len() as u32);
            self.constants.push(c);
            tc
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

    fn test_map() -> (PhfConstMap<TextConstant, &'static str>, Consts) {
        let mut c = Consts::default();

        let map = PhfConstMap::build(
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
            ],
            &c,
        );

        (map, c)
    }

    #[test]
    fn test_text_constant() {
        let (map, c) = test_map();

        assert_eq!(map.get("foo", &c), Some(&"FOO"));
        assert_eq!(map.get("bar", &c), Some(&"BAR"));
        assert_eq!(map.get("foobar", &c), Some(&"FOOBAR"));
        assert_eq!(map.get("k", &c), Some(&"K"));
    }

    #[test]
    fn test_arcstr() {
        let (map, c) = test_map();
        let map = map.import_keys(&ConstsAsArcStr(&c));

        assert_eq!(map.get("foo"), Some(&"FOO"));
        assert_eq!(map.get("bar"), Some(&"BAR"));
        assert_eq!(map.get("foobar"), Some(&"FOOBAR"));
        assert_eq!(map.get("k"), Some(&"K"));
    }

    #[test]
    fn check_size() {
        type CheckSize = PhfConstMap<TextConstant, u64>;

        assert_eq!(size_of::<CheckSize>(), 40);
    }
}

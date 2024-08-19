use std::hash::{BuildHasher, Hash, Hasher};

use ahash::AHasher;
use fnv::FnvHashMap;
use ordered_float::NotNan;

use crate::{attr::Attr, sequence::Sequence, tuple::EndoTupleElements, value::Value, PropId};

/// Determine ONTOL equality.
///
/// This is not the same as Rust's [Eq] and doesn't need to follow Rust's rules.
/// This is not relied upon for correct hashmap behaviour.
///
/// Its intended use is comparing things that are semantically equal in ONTOL,
/// but might not be semantically equal in Rust.
///
/// note: Two values that are [OntolEquals::ontol_equals] must yield the same [OntolHash::ontol_hash].
pub trait OntolEquals {
    fn ontol_equals(&self, other: &Self) -> bool;
}

/// Determine the ONTOL hash of some value.
///
/// A non-cryptographic hash function is used, that yields a 64 bit hash.
///
/// This trait exists to provide a "filter" before using [OntolEquals],
/// so hashing is intended to be more performant than comparison.
pub trait OntolHash {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState);
}

impl OntolEquals for Attr {
    fn ontol_equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Attr::Unit(a), Attr::Unit(b)) => a.ontol_equals(b),
            (Attr::Tuple(a), Attr::Tuple(b)) => a.elements.ontol_equals(&b.elements),
            (Attr::Matrix(a), Attr::Matrix(b)) => a.columns.ontol_equals(&b.columns),
            _ => false,
        }
    }
}

impl OntolHash for Attr {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState) {
        core::mem::discriminant(self).hash(h);

        match self {
            Attr::Unit(b) => b.ontol_hash(h, builder),
            Attr::Tuple(t) => t.elements.ontol_hash(h, builder),
            Attr::Matrix(m) => m.columns.ontol_hash(h, builder),
        }
    }
}

impl<T: OntolEquals> OntolEquals for EndoTupleElements<T> {
    fn ontol_equals(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter()
            .zip(other.iter())
            .all(|(a, b)| a.ontol_equals(b))
    }
}

impl<T: OntolHash> OntolHash for EndoTupleElements<T> {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState) {
        self.len().hash(h);

        for element in self {
            element.ontol_hash(h, builder);
        }
    }
}

impl<T: OntolEquals> OntolEquals for Sequence<T> {
    fn ontol_equals(&self, other: &Self) -> bool {
        self.elements().len() == other.elements().len()
            && self.sub() == other.sub()
            && self
                .elements()
                .iter()
                .zip(other.elements().iter())
                .all(|(a, b)| a.ontol_equals(b))
    }
}

impl<T: OntolHash> OntolHash for Sequence<T> {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState) {
        self.elements().len().hash(h);
        self.sub().hash(h);

        for attr in self.elements() {
            attr.ontol_hash(h, builder);
        }
    }
}

impl OntolEquals for Value {
    fn ontol_equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Unit(tag_a), Value::Unit(tag_b)) => tag_a.def_id() == tag_b.def_id(),
            (Value::Void(_), Value::Void(_)) => true,
            (Value::I64(a, tag_a), Value::I64(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::F64(a, tag_a), Value::F64(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::Serial(a, tag_a), Value::Serial(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::Rational(a, tag_a), Value::Rational(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::Text(a, tag_a), Value::Text(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::OctetSequence(a, tag_a), Value::OctetSequence(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::ChronoDateTime(a, tag_a), Value::ChronoDateTime(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::ChronoDate(a, tag_a), Value::ChronoDate(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::ChronoTime(a, tag_a), Value::ChronoTime(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && a == b
            }
            (Value::Struct(a, tag_a), Value::Struct(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id() && property_map_equals(a, b)
            }
            (Value::Dict(a, tag_a), Value::Dict(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id()
                    && a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((key_a, val_a), (key_b, val_b))| {
                            key_a == key_b && val_a.ontol_equals(val_b)
                        })
            }
            (Value::Sequence(a, tag_a), Value::Sequence(b, tag_b)) => {
                tag_a.def_id() == tag_b.def_id()
                    && a.elements().len() == b.elements().len()
                    && a.sub() == b.sub()
                    && a.elements()
                        .iter()
                        .zip(b.elements().iter())
                        .all(|(a, b)| a.ontol_equals(b))
            }
            (Value::DeleteRelationship(tag_a), Value::DeleteRelationship(tag_b)) => {
                tag_a.def_id() == tag_b.def_id()
            }
            (Value::Filter(_, _), Value::Filter(_, _)) => {
                // Does not make sense to compare, all filters are "equal"
                true
            }
            _ => false,
        }
    }
}

impl OntolHash for Value {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState) {
        std::mem::discriminant(self).hash(h);

        match self {
            Value::Unit(tag) => tag.def_id().hash(h),
            Value::Void(_) => {}
            Value::I64(v, tag) => (v, tag.def_id()).hash(h),
            Value::F64(v, tag) => match NotNan::new(*v) {
                Ok(f) => (f, tag.def_id()).hash(h),
                Err(_) => (-1, tag.def_id()).hash(h),
            },
            Value::Serial(v, tag) => (v, tag.def_id()).hash(h),
            Value::Rational(v, tag) => (v, tag.def_id()).hash(h),
            Value::Text(v, tag) => (v, tag.def_id()).hash(h),
            Value::OctetSequence(v, tag) => (v, tag.def_id()).hash(h),
            Value::ChronoDateTime(v, tag) => (v, tag.def_id()).hash(h),
            Value::ChronoDate(v, tag) => (v, tag.def_id()).hash(h),
            Value::ChronoTime(v, tag) => (v, tag.def_id()).hash(h),
            Value::Struct(v, tag) => {
                tag.def_id().hash(h);
                property_map_ontol_hash(v, h, builder);
            }
            Value::Dict(v, tag) => {
                tag.def_id().hash(h);
                v.len().hash(h);
                for (key, val) in v.iter() {
                    key.hash(h);
                    val.ontol_hash(h, builder);
                }
            }
            Value::Sequence(v, tag) => {
                tag.def_id().hash(h);
                v.ontol_hash(h, builder);
            }
            Value::DeleteRelationship(tag) => {
                tag.def_id().hash(h);
            }
            Value::Filter(_, _) => {}
        }
    }
}

type PropertyMap = FnvHashMap<PropId, Attr>;

fn property_map_equals(a: &PropertyMap, b: &PropertyMap) -> bool {
    if a.len() != b.len() {
        return false;
    }

    fn sort_properties(iter: impl Iterator<Item = PropId>) -> Vec<PropId> {
        let mut properties: Vec<_> = iter.collect();
        properties.sort_unstable();
        properties
    }

    let props_a = sort_properties(a.keys().copied());
    let props_b = sort_properties(b.keys().copied());

    if props_a != props_b {
        return false;
    }

    for prop_id in props_a {
        if !a
            .get(&prop_id)
            .unwrap()
            .ontol_equals(b.get(&prop_id).unwrap())
        {
            return false;
        }
    }

    true
}

fn property_map_ontol_hash(map: &PropertyMap, h: &mut AHasher, builder: &ahash::RandomState) {
    map.len().hash(h);

    let mut items_hash: u64 = 0;

    // this XOR hashing trick does not depend on the order
    // of the entries in the map:
    for (prop_id, attr) in map {
        let mut hasher = builder.build_hasher();

        prop_id.hash(&mut hasher);
        attr.ontol_hash(&mut hasher, builder);

        items_hash ^= hasher.finish();
    }

    items_hash.hash(h);
}

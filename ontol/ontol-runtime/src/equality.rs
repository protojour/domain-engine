use std::hash::{BuildHasher, Hash, Hasher};

use ahash::AHasher;
use fnv::FnvHashMap;
use ordered_float::NotNan;

use crate::{
    value::{Attribute, Value},
    RelationshipId,
};

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

impl OntolEquals for Attribute {
    fn ontol_equals(&self, other: &Self) -> bool {
        self.rel.ontol_equals(&other.rel) && self.val.ontol_equals(&other.val)
    }
}

impl OntolHash for Attribute {
    fn ontol_hash(&self, h: &mut ahash::AHasher, builder: &ahash::RandomState) {
        self.rel.ontol_hash(h, builder);
        self.val.ontol_hash(h, builder);
    }
}

impl OntolEquals for Value {
    fn ontol_equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Unit(def_a), Value::Unit(def_b)) => def_a == def_b,
            (Value::Void(_), Value::Void(_)) => true,
            (Value::I64(a, def_a), Value::I64(b, def_b)) => def_a == def_b && a == b,
            (Value::F64(a, def_a), Value::F64(b, def_b)) => def_a == def_b && a == b,
            (Value::Serial(a, def_a), Value::Serial(b, def_b)) => def_a == def_b && a == b,
            (Value::Rational(a, def_a), Value::Rational(b, def_b)) => def_a == def_b && a == b,
            (Value::Text(a, def_a), Value::Text(b, def_b)) => def_a == def_b && a == b,
            (Value::OctetSequence(a, def_a), Value::OctetSequence(b, def_b)) => {
                def_a == def_b && a == b
            }
            (Value::ChronoDateTime(a, def_a), Value::ChronoDateTime(b, def_b)) => {
                def_a == def_b && a == b
            }
            (Value::ChronoDate(a, def_a), Value::ChronoDate(b, def_b)) => def_a == def_b && a == b,
            (Value::ChronoTime(a, def_a), Value::ChronoTime(b, def_b)) => def_a == def_b && a == b,
            (Value::Struct(a, def_a), Value::Struct(b, def_b)) => {
                def_a == def_b && property_map_equals(a, b)
            }
            (Value::StructUpdate(a, def_a), Value::StructUpdate(b, def_b)) => {
                def_a == def_b && property_map_equals(a, b)
            }
            (Value::Dict(a, def_a), Value::Dict(b, def_b)) => {
                def_a == def_b
                    && a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((key_a, val_a), (key_b, val_b))| {
                            key_a == key_b && val_a.ontol_equals(val_b)
                        })
            }
            (Value::Sequence(a, def_a), Value::Sequence(b, def_b)) => {
                def_a == def_b
                    && a.elements().len() == b.elements().len()
                    && a.sub() == b.sub()
                    && a.elements()
                        .iter()
                        .zip(b.elements().iter())
                        .all(|(a, b)| a.ontol_equals(b))
            }
            (Value::Patch(a, def_a), Value::Patch(b, def_b)) => {
                def_a == def_b
                    && a.len() == b.len()
                    && a.iter().zip(b.iter()).all(|(a, b)| a.ontol_equals(b))
            }
            (Value::DeleteRelationship(def_a), Value::DeleteRelationship(def_b)) => def_a == def_b,
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
            Value::Unit(def) => def.hash(h),
            Value::Void(_) => {}
            Value::I64(v, def) => (v, def).hash(h),
            Value::F64(v, def) => match NotNan::new(*v) {
                Ok(f) => (f, def).hash(h),
                Err(_) => (-1, def).hash(h),
            },
            Value::Serial(v, def) => (v, def).hash(h),
            Value::Rational(v, def) => (v, def).hash(h),
            Value::Text(v, def) => (v, def).hash(h),
            Value::OctetSequence(v, def) => (v, def).hash(h),
            Value::ChronoDateTime(v, def) => (v, def).hash(h),
            Value::ChronoDate(v, def) => (v, def).hash(h),
            Value::ChronoTime(v, def) => (v, def).hash(h),
            Value::Struct(v, def) => {
                def.hash(h);
                property_map_ontol_hash(v, h, builder);
            }
            Value::StructUpdate(v, def) => {
                def.hash(h);
                property_map_ontol_hash(v, h, builder);
            }
            Value::Dict(v, def) => {
                def.hash(h);
                v.len().hash(h);
                for (key, val) in v.iter() {
                    key.hash(h);
                    val.ontol_hash(h, builder);
                }
            }
            Value::Sequence(v, def) => {
                def.hash(h);
                v.elements().len().hash(h);
                v.sub().hash(h);

                for attr in v.elements() {
                    attr.ontol_hash(h, builder);
                }
            }
            Value::Patch(v, def) => {
                def.hash(h);
                v.len().hash(h);
                for attr in v.iter() {
                    attr.ontol_hash(h, builder);
                }
            }
            Value::DeleteRelationship(def) => {
                def.hash(h);
            }
            Value::Filter(_, _) => {}
        }
    }
}

type PropertyMap = FnvHashMap<RelationshipId, Attribute<Value>>;

fn property_map_equals(a: &PropertyMap, b: &PropertyMap) -> bool {
    if a.len() != b.len() {
        return false;
    }

    fn sort_properties(iter: impl Iterator<Item = RelationshipId>) -> Vec<RelationshipId> {
        let mut properties: Vec<_> = iter.collect();
        properties.sort_unstable();
        properties
    }

    let props_a = sort_properties(a.keys().copied());
    let props_b = sort_properties(b.keys().copied());

    if props_a != props_b {
        return false;
    }

    for rel_id in props_a {
        if !a
            .get(&rel_id)
            .unwrap()
            .ontol_equals(b.get(&rel_id).unwrap())
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

use std::collections::BTreeMap;

use smartstring::alias::String;

use crate::{DefId, RelationId, Role};

#[derive(Clone, Debug)]
pub struct Value {
    /// The data associated with this value
    pub data: Data,

    /// The runtime type associated with this value.
    /// This is used to make quick type checks,
    /// as well as figuring out how to serialize.
    pub type_def_id: DefId,
}

impl Value {
    pub const fn new(data: Data, type_def_id: DefId) -> Self {
        Self { data, type_def_id }
    }

    #[inline]
    pub const fn unit() -> Self {
        Self {
            data: Data::Unit,
            type_def_id: DefId::unit(),
        }
    }

    pub fn is_unit(&self) -> bool {
        self.type_def_id == DefId::unit()
    }

    pub fn filter_non_unit(&self) -> Option<&Self> {
        if self.is_unit() {
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Clone, Debug)]
pub enum Data {
    Unit,
    Int(i64),
    Float(f64),
    Rational(Box<num::rational::BigRational>),
    String(String),
    Uuid(uuid::Uuid),
    /// A collection of attributes keyed by relation
    Map(BTreeMap<PropertyId, Attribute>),
    /// Represents both dynamic lists and static tuples at runtime
    Vec(Vec<Attribute>),
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PropertyId {
    pub role: Role,
    pub relation_id: RelationId,
}

impl PropertyId {
    pub fn subject(relation_id: RelationId) -> Self {
        Self {
            role: Role::Subject,
            relation_id,
        }
    }

    pub fn object(relation_id: RelationId) -> Self {
        Self {
            role: Role::Object,
            relation_id,
        }
    }
}

/// An Attribute is a Value that is part of another value.
///
/// An attribute may be parameterized (edge_params).
/// The parameter _value_ is itself a `Value`.
/// Most attribute parameters is usually just `unit`, i.e. no parameters.
///
/// The attribute value is also just a Value.
#[derive(Clone, Debug)]
pub struct Attribute {
    pub value: Value,
    pub edge_params: Value,
}

impl Attribute {
    /// Create an attribute with a unit edge
    #[inline]
    pub const fn with_unit_params(value: Value) -> Self {
        Self {
            value,
            edge_params: Value::unit(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use indexmap::IndexMap;
    use num::rational::BigRational;
    use smallvec::SmallVec;

    #[test]
    fn rational_arithmetic() {
        let a = BigRational::new(9.into(), 1.into());
        let b = BigRational::new(5.into(), 1.into());

        let c = a / b;

        assert_eq!(BigRational::new(9.into(), 5.into()), c);
    }

    #[test]
    fn value_size() {
        assert_eq!(40, std::mem::size_of::<Value>());
        assert_eq!(16, std::mem::size_of::<[u8; 16]>());

        assert_eq!(24, std::mem::size_of::<BTreeMap<RelationId, Value>>());
        assert_eq!(24, std::mem::size_of::<Vec<Value>>());
        assert_eq!(32, std::mem::size_of::<SmallVec<[u32; 0]>>());
        assert_eq!(48, std::mem::size_of::<HashMap<RelationId, Value>>());
        assert_eq!(72, std::mem::size_of::<IndexMap<RelationId, Value>>());
    }

    #[test]
    fn attributes() {
        let mut map = BTreeMap::new();
        map.insert(
            RelationId(DefId(666)),
            Attribute::with_unit_params(Value::new(Data::Int(42), DefId(42))),
        );
    }
}

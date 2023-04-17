use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use smartstring::alias::String;

use crate::{cast::Cast, DefId, RelationId, Role};

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

    pub fn get_attribute(&self, property_id: PropertyId) -> Option<&Attribute> {
        match &self.data {
            Data::Map(map) => map.get(&property_id),
            _ => None,
        }
    }

    pub fn get_attribute_value(&self, property_id: PropertyId) -> Option<&Value> {
        self.get_attribute(property_id).map(|attr| &attr.value)
    }

    pub fn to_attribute(self, rel_params: Value) -> Attribute {
        Attribute {
            value: self,
            rel_params,
        }
    }

    pub fn cast_ref<T>(&self) -> &<Self as Cast<T>>::Ref
    where
        Self: Cast<T>,
    {
        <Self as Cast<T>>::cast_ref(self)
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
    ChronoDateTime(chrono::DateTime<chrono::Utc>),
    ChronoDate(chrono::NaiveDate),
    ChronoTime(chrono::NaiveTime),

    /// A collection of attributes keyed by property.
    Map(BTreeMap<PropertyId, Attribute>),

    /// A sequence of attributes.
    ///
    /// The difference between a Sequence and a Map is that
    /// sequences use numeric keys instead of PropertyId.
    ///
    /// Some sequences will be uniform (all elements have the same type).
    /// Other sequences will behave more like tuples.
    Sequence(Vec<Attribute>),
}

pub struct FormatStringData<'d>(pub &'d Data);

impl<'d> Display for FormatStringData<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Data::String(s) => write!(f, "{s}"),
            Data::Uuid(uuid) => write!(f, "{uuid}"),
            Data::ChronoDateTime(datetime) => {
                // FIXME: A way to not do this via String
                // Chrono 0.5 hopefully fixes this
                write!(f, "{}", datetime.to_rfc3339())
            }
            data => panic!("not a string-like type: {data:?}"),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PropertyId {
    pub role: Role,
    pub relation_id: RelationId,
}

impl Debug for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PropertyId({:?}, {:?}, {:?})",
            self.role, self.relation_id.0 .0 .0, self.relation_id.0 .1
        )
    }
}

impl PropertyId {
    pub const fn subject(relation_id: RelationId) -> Self {
        Self {
            role: Role::Subject,
            relation_id,
        }
    }

    pub const fn object(relation_id: RelationId) -> Self {
        Self {
            role: Role::Object,
            relation_id,
        }
    }
}

/// An Attribute is a Value that is part of another value.
///
/// An attribute may be parameterized (rel_params).
/// The attribute parameter should be non-unit when the relationship between
/// the container (subject) and the map (object) is parameterized.
///
/// The parameter _value_ is itself a `Value`.
///
/// The attribute value is also just a Value.
#[derive(Clone, Debug)]
pub struct Attribute {
    pub value: Value,
    pub rel_params: Value,
}

impl Attribute {
    /// Create an attribute with a unit edge
    #[inline]
    pub const fn with_unit_params(value: Value) -> Self {
        Self {
            value,
            rel_params: Value::unit(),
        }
    }
}

pub struct ValueDebug<'v>(pub &'v Value);

impl<'v> Display for ValueDebug<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = &self.0;
        match &value.data {
            Data::Unit => write!(f, "#u"),
            Data::Int(i) => write!(f, "int({i})"),
            Data::Float(n) => write!(f, "flt({n})"),
            Data::Rational(r) => write!(f, "rat({r})"),
            Data::String(s) => write!(f, "'{s}'"),
            Data::Uuid(u) => write!(f, "uuid({u})"),
            Data::ChronoDateTime(dt) => write!(f, "datetime({dt})"),
            Data::ChronoDate(d) => write!(f, "date({d})"),
            Data::ChronoTime(t) => write!(f, "time({t})"),
            Data::Map(m) => {
                write!(f, "{{")?;
                let mut iter = m.iter().peekable();
                while let Some((prop, attr)) = iter.next() {
                    match prop.role {
                        Role::Subject => write!(f, "subj")?,
                        Role::Object => write!(f, "obj")?,
                    }

                    write!(
                        f,
                        "({}, {}): {}",
                        prop.relation_id.0 .0 .0,
                        prop.relation_id.0 .1,
                        AttrDebug(attr),
                    )?;

                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")
            }
            Data::Sequence(s) => {
                write!(f, "[")?;
                let mut iter = s.iter().peekable();
                while let Some(attr) = iter.next() {
                    write!(f, "{}", AttrDebug(attr),)?;

                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")
            }
        }
    }
}

struct AttrDebug<'a>(&'a Attribute);

impl<'a> Display for AttrDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.0;
        write!(f, "{}", ValueDebug(&attr.value))?;

        if !attr.rel_params.is_unit() {
            write!(f, "~{}", ValueDebug(&attr.rel_params))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use crate::PackageId;

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
            RelationId(DefId(PackageId(0), 666)),
            Attribute::with_unit_params(Value::new(Data::Int(42), DefId(PackageId(0), 42))),
        );
    }
}

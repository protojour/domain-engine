use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    cast::Cast, condition::Condition, ontology::Ontology, DefId, PackageId, RelationshipId, Role,
};

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

    pub fn sequence_of(values: impl IntoIterator<Item = Value>) -> Self {
        let attributes: Vec<_> = values
            .into_iter()
            .map(|value| Attribute {
                value,
                rel_params: Self::unit(),
            })
            .collect();
        let type_def_id = attributes
            .first()
            .map(|attr| attr.value.type_def_id)
            .unwrap_or(DefId::unit());
        Self {
            data: Data::Sequence(attributes),
            type_def_id,
        }
    }

    #[inline]
    pub const fn unit() -> Self {
        Self {
            data: Data::Unit,
            type_def_id: DefId::unit(),
        }
    }

    /// Take the value, leave a Unit value behind.
    pub fn take(&mut self) -> Value {
        let mut value = Self::unit();
        std::mem::swap(self, &mut value);
        value
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
            Data::Struct(map) => map.get(&property_id),
            _ => None,
        }
    }

    pub fn get_attribute_value(&self, property_id: PropertyId) -> Option<&Value> {
        self.get_attribute(property_id).map(|attr| &attr.value)
    }

    #[inline]
    pub const fn to_attr(self, rel_params: Value) -> Attribute {
        Attribute {
            value: self,
            rel_params,
        }
    }

    #[inline]
    pub const fn to_unit_attr(self) -> Attribute {
        Attribute {
            value: self,
            rel_params: Value::unit(),
        }
    }

    pub fn cast_ref<T>(&self) -> &<Self as Cast<T>>::Ref
    where
        Self: Cast<T>,
    {
        <Self as Cast<T>>::cast_ref(self)
    }

    pub fn cast_into<T>(self) -> T
    where
        Self: Cast<T>,
    {
        <Self as Cast<T>>::cast_into(self)
    }
}

#[derive(Clone, Debug)]
pub enum Data {
    Unit,
    I64(i64),
    F64(f64),
    Rational(Box<num::rational::BigRational>),
    Text(String),
    OctetSequence(SmallVec<[u8; 16]>),
    ChronoDateTime(chrono::DateTime<chrono::Utc>),
    ChronoDate(chrono::NaiveDate),
    ChronoTime(chrono::NaiveTime),

    /// A collection of attributes keyed by property.
    Struct(BTreeMap<PropertyId, Attribute>),

    /// A sequence of attributes.
    ///
    /// The difference between a Sequence and a Map is that
    /// sequences use numeric keys instead of PropertyId.
    ///
    /// Some sequences will be uniform (all elements have the same type).
    /// Other sequences will behave more like tuples.
    Sequence(Vec<Attribute>),

    Condition(Condition),
}

impl Data {
    pub fn new_rational_i64(numer: i64, denom: i16) -> Self {
        Self::Rational(Box::new(num::rational::BigRational::new(
            numer.into(),
            denom.into(),
        )))
    }
}

pub struct FormatDataAsText<'d, 'o> {
    pub data: &'d Data,
    pub type_def_id: DefId,
    pub ontology: &'o Ontology,
}

impl<'d, 'o> Display for FormatDataAsText<'d, 'o> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(text_like_type) = self.ontology.get_text_like_type(self.type_def_id) {
            text_like_type.format(self.data, f)
        } else {
            match self.data {
                Data::Text(s) => write!(f, "{s}"),
                Data::OctetSequence(vec) => write!(f, "b'{vec:x?}'"),
                Data::ChronoDateTime(datetime) => {
                    // FIXME: A way to not do this via String
                    // Chrono 0.5 hopefully fixes this
                    write!(f, "{}", datetime.to_rfc3339())
                }
                data => panic!("not a text-like type: {data:?}"),
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PropertyId {
    pub role: Role,
    pub relationship_id: RelationshipId,
}

impl Display for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            match self.role {
                Role::Subject => 'S',
                Role::Object => 'O',
            },
            self.relationship_id.0 .0 .0,
            self.relationship_id.0 .1,
        )
    }
}

impl FromStr for PropertyId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iterator = s.split(':');
        let role = match iterator.next().ok_or(())? {
            "S" => Role::Subject,
            "O" => Role::Object,
            _ => Err(())?,
        };
        let package_id = PackageId(iterator.next().ok_or(())?.parse().map_err(|_| ())?);
        let def_idx: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;

        if iterator.next().is_some() {
            return Err(());
        }

        Ok(PropertyId {
            role,
            relationship_id: RelationshipId(DefId(package_id, def_idx)),
        })
    }
}

impl Debug for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl PropertyId {
    pub const fn subject(relationship_id: RelationshipId) -> Self {
        Self {
            role: Role::Subject,
            relationship_id,
        }
    }

    pub const fn object(relationship_id: RelationshipId) -> Self {
        Self {
            role: Role::Object,
            relationship_id,
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
///
/// FIXME: There is probably a flaw in the modelling of one-to-many attributes.
/// One-to-many has many rel_params too (currently represented using Data::Seq for the value).
/// So should Attribute be an enum instead of a struct?
#[derive(Clone, Debug)]
pub struct Attribute {
    pub value: Value,
    pub rel_params: Value,
}

impl From<Value> for Attribute {
    fn from(value: Value) -> Self {
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
            Data::I64(i) => write!(f, "int({i})"),
            Data::F64(n) => write!(f, "flt({n})"),
            Data::Rational(r) => write!(f, "rat({r})"),
            Data::Text(s) => write!(f, "'{s}'"),
            Data::OctetSequence(vec) => write!(f, "b'{vec:x?}'"),
            Data::ChronoDateTime(dt) => write!(f, "datetime({dt})"),
            Data::ChronoDate(d) => write!(f, "date({d})"),
            Data::ChronoTime(t) => write!(f, "time({t})"),
            Data::Struct(m) => {
                write!(f, "{{")?;
                let mut iter = m.iter().peekable();
                while let Some((prop, attr)) = iter.next() {
                    write!(f, "{prop} -> {}", AttrDebug(attr),)?;

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
            Data::Condition(_) => write!(f, "condition"),
        }
    }
}

struct AttrDebug<'a>(&'a Attribute);

impl<'a> Display for AttrDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.0;
        write!(f, "{}", ValueDebug(&attr.value))?;

        if !attr.rel_params.is_unit() {
            write!(f, "::{}", ValueDebug(&attr.rel_params))?;
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

        assert_eq!(24, std::mem::size_of::<BTreeMap<RelationshipId, Value>>());
        assert_eq!(24, std::mem::size_of::<Vec<Value>>());
        assert_eq!(24, std::mem::size_of::<SmallVec<[u32; 0]>>());
        assert_eq!(48, std::mem::size_of::<HashMap<RelationshipId, Value>>());
        assert_eq!(72, std::mem::size_of::<IndexMap<RelationshipId, Value>>());
    }

    #[test]
    fn attributes() {
        let mut map = BTreeMap::new();
        map.insert(
            RelationshipId(DefId(PackageId(0), 666)),
            Value::new(Data::I64(42), DefId(PackageId(0), 42)).to_unit_attr(),
        );
    }
}

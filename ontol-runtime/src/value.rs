use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use smartstring::alias::String;
use thin_vec::ThinVec;

use crate::{
    cast::Cast,
    condition::{CondTerm, Condition},
    ontology::Ontology,
    sequence::Sequence,
    DefId, PackageId, RelationshipId, Role,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Unit(DefId),
    I64(i64, DefId),
    F64(f64, DefId),
    Rational(Box<num::rational::BigRational>, DefId),
    Text(String, DefId),
    OctetSequence(ThinVec<u8>, DefId),
    ChronoDateTime(chrono::DateTime<chrono::Utc>, DefId),
    ChronoDate(chrono::NaiveDate, DefId),
    ChronoTime(chrono::NaiveTime, DefId),

    /// A collection of attributes keyed by property.
    Struct(Box<FnvHashMap<PropertyId, Attribute>>, DefId),

    /// A collection of attributes keyed by property, but contains
    /// only partial information, and must contain the ID of the struct (entity) to update.
    StructUpdate(Box<FnvHashMap<PropertyId, Attribute>>, DefId),

    /// A collection of arbitrary values keyed by strings.
    Dict(Box<HashMap<String, Value>>, DefId),

    /// A sequence of attributes.
    ///
    /// The difference between a Sequence and a Map is that
    /// sequences use numeric keys instead of PropertyId.
    ///
    /// Some sequences will be uniform (all elements have the same type).
    /// Other sequences will behave more like tuples.
    Sequence(Sequence, DefId),

    /// A patching of some graph property of entities.
    ///
    /// The type of each attribute carry the patch semantics:
    ///
    /// * `(value: Struct, rel_params)`: Write a new entity
    /// * `(value: StructUpdate, rel_params)`: Update the given entity with new rel_params
    /// * `(valud: ID, rel_params: Value::Delete)`: Delete the given ID
    Patch(Vec<Attribute>, DefId),

    /// Special rel_params used for edge deletion
    DeleteRelationship(DefId),

    Condition(Condition<CondTerm>, DefId),
}

impl Value {
    pub fn new_struct(
        props: impl IntoIterator<Item = (PropertyId, Attribute)>,
        type_id: DefId,
    ) -> Self {
        Self::Struct(Box::new(FnvHashMap::from_iter(props)), type_id)
    }

    pub fn sequence_of(values: impl IntoIterator<Item = Value>) -> Self {
        let attrs: Vec<_> = values
            .into_iter()
            .map(|value| Attribute {
                value,
                rel_params: Self::unit(),
            })
            .collect();
        let type_def_id = attrs
            .first()
            .map(|attr| attr.value.type_def_id())
            .unwrap_or(DefId::unit());
        Self::Sequence(Sequence::new(attrs), type_def_id)
    }

    #[inline]
    pub const fn unit() -> Self {
        Self::Unit(DefId::unit())
    }

    pub fn type_def_id(&self) -> DefId {
        match self {
            Value::Unit(def_id) => *def_id,
            Value::I64(_, def_id) => *def_id,
            Value::F64(_, def_id) => *def_id,
            Value::Rational(_, def_id) => *def_id,
            Value::Text(_, def_id) => *def_id,
            Value::OctetSequence(_, def_id) => *def_id,
            Value::ChronoDateTime(_, def_id) => *def_id,
            Value::ChronoDate(_, def_id) => *def_id,
            Value::ChronoTime(_, def_id) => *def_id,
            Value::Struct(_, def_id) => *def_id,
            Value::Dict(_, def_id) => *def_id,
            Value::Sequence(_, def_id) => *def_id,
            Value::Patch(_, def_id) => *def_id,
            Value::StructUpdate(_, def_id) => *def_id,
            Value::DeleteRelationship(def_id) => *def_id,
            Value::Condition(_, def_id) => *def_id,
        }
    }

    pub fn type_def_id_mut(&mut self) -> &mut DefId {
        match self {
            Value::Unit(def_id) => def_id,
            Value::I64(_, def_id) => def_id,
            Value::F64(_, def_id) => def_id,
            Value::Rational(_, def_id) => def_id,
            Value::Text(_, def_id) => def_id,
            Value::OctetSequence(_, def_id) => def_id,
            Value::ChronoDateTime(_, def_id) => def_id,
            Value::ChronoDate(_, def_id) => def_id,
            Value::ChronoTime(_, def_id) => def_id,
            Value::Struct(_, def_id) => def_id,
            Value::Dict(_, def_id) => def_id,
            Value::Sequence(_, def_id) => def_id,
            Value::Patch(_, def_id) => def_id,
            Value::StructUpdate(_, def_id) => def_id,
            Value::DeleteRelationship(def_id) => def_id,
            Value::Condition(_, def_id) => def_id,
        }
    }

    /// Take the value, leave a Unit value behind.
    pub fn take(&mut self) -> Value {
        let mut value = Self::unit();
        std::mem::swap(self, &mut value);
        value
    }

    pub fn is_unit(&self) -> bool {
        self.type_def_id() == DefId::unit()
    }

    pub fn filter_non_unit(&self) -> Option<&Self> {
        if self.is_unit() {
            None
        } else {
            Some(self)
        }
    }

    pub fn get_attribute(&self, property_id: PropertyId) -> Option<&Attribute> {
        match self {
            Self::Struct(map, _) => map.get(&property_id),
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

pub struct FormatValueAsText<'d, 'o> {
    pub value: &'d Value,
    pub type_def_id: DefId,
    pub ontology: &'o Ontology,
}

impl<'d, 'o> Display for FormatValueAsText<'d, 'o> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(text_like_type) = self.ontology.get_text_like_type(self.type_def_id) {
            text_like_type.format(self.value, f)
        } else {
            match &self.value {
                Value::Text(s, _) => write!(f, "{s}"),
                Value::I64(int, _) => write!(f, "{int}"),
                Value::F64(float, _) => write!(f, "{float}"),
                Value::OctetSequence(vec, _) => write!(f, "b'{vec:x?}'"),
                Value::ChronoDateTime(datetime, _) => {
                    // FIXME: A way to not do this via String
                    // Chrono 0.5 hopefully fixes this
                    write!(f, "{}", datetime.to_rfc3339())
                }
                Value::Struct(props, type_def_id) => {
                    // concatenate every prop (not sure this is a good idea, since the order is not defined)
                    for attr in props.values() {
                        FormatValueAsText {
                            value: &attr.value,
                            type_def_id: *type_def_id,
                            ontology: self.ontology,
                        }
                        .fmt(f)?;
                    }

                    Ok(())
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        match &value {
            Value::Unit(_) => write!(f, "#u"),
            Value::I64(i, _) => write!(f, "int({i})"),
            Value::F64(n, _) => write!(f, "flt({n})"),
            Value::Rational(r, _) => write!(f, "rat({r})"),
            Value::Text(s, _) => write!(f, "'{s}'"),
            Value::OctetSequence(vec, _) => write!(f, "b'{vec:x?}'"),
            Value::ChronoDateTime(dt, _) => write!(f, "datetime({dt})"),
            Value::ChronoDate(d, _) => write!(f, "date({d})"),
            Value::ChronoTime(t, _) => write!(f, "time({t})"),
            Value::Struct(m, _) | Value::StructUpdate(m, _) => {
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
            Value::Dict(..) => {
                write!(f, "dict")
            }
            Value::Sequence(seq, _) => {
                write!(f, "[")?;
                let mut iter = seq.attrs.iter().peekable();
                while let Some(attr) = iter.next() {
                    write!(f, "{}", AttrDebug(attr),)?;

                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")
            }
            Value::Patch(patch, _) => {
                write!(f, "patch{{")?;
                let mut iter = patch.iter().peekable();
                while let Some(attr) = iter.next() {
                    write!(f, "{}", AttrDebug(attr),)?;

                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")
            }
            Value::DeleteRelationship(_) => {
                write!(f, "DELETE_RELATIONSHIP")
            }
            Value::Condition(..) => write!(f, "condition"),
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
        assert_eq!(32, std::mem::size_of::<Value>());

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
            Value::I64(42, DefId(PackageId(0), 42)).to_unit_attr(),
        );
    }
}

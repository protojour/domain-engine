use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::{Debug, Display},
    ops::Index,
};

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use itertools::{Itertools, Position};
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String;
use thin_vec::ThinVec;
use tracing::debug;

use crate::{
    cast::Cast,
    ontology::Ontology,
    query::filter::Filter,
    sequence::Sequence,
    tuple::{EndoTuple, EndoTupleElements},
    DefId, RelationshipId,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// Unit value for DefIds that have only one possible value
    Unit(DefId),
    /// Void represents the absence of value - used in error detection and try mechanism
    Void(DefId),
    I64(i64, DefId),
    F64(f64, DefId),
    Serial(Serial, DefId),
    Rational(Box<num::rational::BigRational>, DefId),
    Text(String, DefId),
    OctetSequence(ThinVec<u8>, DefId),
    ChronoDateTime(chrono::DateTime<chrono::Utc>, DefId),
    ChronoDate(chrono::NaiveDate, DefId),
    ChronoTime(chrono::NaiveTime, DefId),

    /// A collection of attributes keyed by property.
    Struct(Box<FnvHashMap<RelationshipId, Attr>>, DefId),

    /// A collection of attributes keyed by property, but contains
    /// only partial information, and must contain the ID of the struct (entity) to update.
    StructUpdate(Box<FnvHashMap<RelationshipId, Attr>>, DefId),

    /// A collection of arbitrary values keyed by strings.
    Dict(Box<BTreeMap<String, Value>>, DefId),

    /// A sequence of attributes.
    ///
    /// The difference between a Sequence and a Map is that
    /// sequences use numeric keys instead of RelationshipId.
    ///
    /// Some sequences will be uniform (all elements have the same type).
    /// Other sequences will behave more like tuples.
    ///
    /// FIXME: Refactor this to be a pure Array of Values
    Sequence(Sequence<Value>, DefId),

    /// A patching of some graph property of entities.
    ///
    /// The type of each attribute carry the patch semantics:
    ///
    /// * `Attr::Tuple(Struct, rel_params)`: Write a new entity
    /// * `Attr::Tuple(StructUpdate, rel_params)`: Update the given entity with new rel_params
    /// * `Attr::Tuple(ID, Value::Delete)`: Delete the given ID
    Patch(Vec<Attr>, DefId),

    /// Special rel_params used for edge deletion
    DeleteRelationship(DefId),

    Filter(Box<Filter>, DefId),
}

impl Value {
    pub fn new_struct(
        props: impl IntoIterator<Item = (RelationshipId, Attr)>,
        type_id: DefId,
    ) -> Self {
        Self::Struct(Box::new(FnvHashMap::from_iter(props)), type_id)
    }

    pub fn sequence_of(values: impl IntoIterator<Item = Value>) -> Self {
        let sequence: Sequence<Value> = values.into_iter().collect();
        let type_def_id = sequence
            .elements()
            .first()
            .map(|value| value.type_def_id())
            .unwrap_or(DefId::unit());
        Self::Sequence(sequence, type_def_id)
    }

    #[inline]
    pub const fn unit() -> Self {
        Self::Unit(DefId::unit())
    }

    pub fn type_def_id(&self) -> DefId {
        match self {
            Value::Unit(def_id) => *def_id,
            Value::Void(def_id) => *def_id,
            Value::I64(_, def_id) => *def_id,
            Value::F64(_, def_id) => *def_id,
            Value::Serial(_, def_id) => *def_id,
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
            Value::Filter(_, def_id) => *def_id,
        }
    }

    pub fn type_def_id_mut(&mut self) -> &mut DefId {
        match self {
            Value::Unit(def_id) => def_id,
            Value::Void(def_id) => def_id,
            Value::I64(_, def_id) => def_id,
            Value::F64(_, def_id) => def_id,
            Value::Serial(_, def_id) => def_id,
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
            Value::Filter(_, def_id) => def_id,
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

    pub fn get_attribute(&self, rel_id: RelationshipId) -> Option<&Attr> {
        match self {
            Self::Struct(map, _) => map.get(&rel_id),
            _ => None,
        }
    }

    pub fn get_attribute_value(&self, rel_id: RelationshipId) -> Option<&Value> {
        match self.get_attribute(rel_id)? {
            Attr::Unit(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub const fn to_attr(self, rel_params: Value) -> Attribute<Self> {
        Attribute {
            rel: rel_params,
            val: self,
        }
    }

    #[inline]
    pub const fn to_unit_attr(self) -> Attribute<Self> {
        Attribute {
            rel: Value::unit(),
            val: self,
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

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unit(_), Self::Unit(_)) => true,
            (Self::I64(a, _), Self::I64(b, _)) => a == b,
            (Self::F64(a, _), Self::F64(b, _)) => a == b,
            (Self::Rational(a, _), Self::Rational(b, _)) => a == b,
            (Self::Text(a, _), Self::Text(b, _)) => a == b,
            (Self::OctetSequence(a, _), Self::OctetSequence(b, _)) => a == b,
            (Self::ChronoDateTime(a, _), Self::ChronoDateTime(b, _)) => a == b,
            (Self::ChronoDate(a, _), Self::ChronoDate(b, _)) => a == b,
            (Self::ChronoTime(a, _), Self::ChronoTime(b, _)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Unit(_), Self::Unit(_)) => Some(Ordering::Equal),
            (Self::I64(a, _), Self::I64(b, _)) => Some(a.cmp(b)),
            (Self::F64(a, _), Self::F64(b, _)) => a.partial_cmp(b),
            (Self::Rational(a, _), Self::Rational(b, _)) => a.partial_cmp(b),
            (Self::Text(a, _), Self::Text(b, _)) => Some(a.cmp(b)),
            (Self::OctetSequence(a, _), Self::OctetSequence(b, _)) => Some(a.cmp(b)),
            (Self::Serial(a, _), Self::Serial(b, _)) => Some(a.0.cmp(&b.0)),
            (Self::ChronoDateTime(a, _), Self::ChronoDateTime(b, _)) => Some(a.cmp(b)),
            (Self::ChronoDate(a, _), Self::ChronoDate(b, _)) => Some(a.cmp(b)),
            (Self::ChronoTime(a, _), Self::ChronoTime(b, _)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Serial(pub u64);

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
                Value::Serial(Serial(int), _) => write!(f, "{int}"),
                Value::OctetSequence(vec, _) => write!(f, "b'{vec:x?}'"),
                Value::ChronoDateTime(datetime, _) => {
                    // FIXME: A way to not do this via String
                    // Chrono 0.5 hopefully fixes this
                    write!(f, "{}", datetime.to_rfc3339())
                }
                Value::Struct(props, type_def_id) => {
                    // concatenate every prop (not sure this is a good idea, since the order is not defined)
                    for attr in props.values() {
                        if let Attr::Unit(value) = attr {
                            FormatValueAsText {
                                value,
                                type_def_id: *type_def_id,
                                ontology: self.ontology,
                            }
                            .fmt(f)?;
                        }
                    }

                    Ok(())
                }
                data => panic!("not a text-like type: {data:?}"),
            }
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
/// An attribute existing of (relation parameter, value)
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct Attribute<T = Value> {
    /// The relation parameter(s)
    pub rel: T,
    /// The attribute value
    pub val: T,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Attr {
    /// The attribute has one value
    Unit(Value),
    /// The attribute is horizontally multivalued,
    /// the size is known at (ontol-)compile time.
    ///
    /// The tuple represents siblings in the hyper-edge this attribute is derived from.
    Tuple(Box<EndoTuple<Value>>),
    /// Column-oriented matrix, i.e. tuples first, then sequences within those tuples
    Matrix(AttrMatrix),
}

impl Attr {
    pub fn as_ref(&self) -> AttrRef {
        match self {
            Attr::Unit(v) => AttrRef::Unit(v),
            Attr::Tuple(t) => AttrRef::Tuple(&t.elements),
            Attr::Matrix(m) => AttrRef::Matrix(&m.elements),
        }
    }

    pub fn as_unit(&self) -> Option<&Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => {
                if t.elements.len() == 1 {
                    t.elements.get(0)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_matrix(&self) -> Option<&AttrMatrix> {
        match self {
            Self::Matrix(m) => Some(m),
            _ => None,
        }
    }

    pub fn into_unit(self) -> Option<Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => {
                if t.elements.len() == 1 {
                    t.elements.into_iter().next()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn into_tuple(self) -> Option<EndoTupleElements<Value>> {
        match self {
            Self::Unit(v) => Some(smallvec![v]),
            Self::Tuple(t) => Some(t.elements),
            Self::Matrix(_) => None,
        }
    }

    pub fn unwrap_unit(self) -> Value {
        match self {
            Self::Unit(value) => value,
            Self::Tuple(tuple) => {
                if tuple.elements.len() != 1 {
                    panic!("not a singleton tuple")
                }
                tuple.elements.into_iter().next().unwrap()
            }
            Self::Matrix(_) => {
                panic!("matrix is not a unit")
            }
        }
    }

    // Hack for now
    pub fn unit_or_tuple(value: Value, rel_params: Value) -> Self {
        if rel_params.is_unit() {
            Self::Unit(value)
        } else {
            debug!("making a tuple for rel_params: {}", ValueDebug(&rel_params));
            Self::Tuple(Box::new(EndoTuple {
                elements: smallvec![value, rel_params],
            }))
        }
    }
}

/// A column-first matrix of attributes.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AttrMatrix {
    pub elements: EndoTupleElements<Sequence<Value>>,
}

impl AttrMatrix {
    pub fn get_attr_cloned(&self, index: usize) -> Option<Attr> {
        let mut elements: SmallVec<Value, 1> = smallvec![];

        for e in &self.elements {
            elements.push(e.elements.get(index)?.clone());
        }

        Some(Attr::Tuple(Box::new(EndoTuple { elements })))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AttrRef<'v> {
    /// One value
    Unit(&'v Value),
    /// Tuple not in a matrix
    Tuple(&'v [Value]),
    /// Tuple borrowed from a Matrix
    RowTuple(&'v [&'v Value]),
    /// A whole matrix
    Matrix(&'v [Sequence<Value>]),
}

impl<'v> AttrRef<'v> {
    #[inline]
    pub fn coerce_to_unit(self) -> Self {
        match self {
            Self::Unit(v) => Self::Unit(v),
            Self::Tuple(t) => {
                if t.len() == 1 {
                    Self::Unit(&t[0])
                } else {
                    self
                }
            }
            Self::RowTuple(t) => {
                if t.len() == 1 {
                    Self::Unit(t[0])
                } else {
                    self
                }
            }
            Self::Matrix(m) => Self::Matrix(m),
        }
    }

    pub fn first_unit(self) -> Option<&'v Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => t.get(0),
            Self::RowTuple(t) => t.get(0).map(|val| *val),
            Self::Matrix(..) => None,
        }
    }

    pub fn as_unit(&self) -> Option<&Value> {
        match self {
            Self::Unit(v) => Some(v),
            _ => None,
        }
    }
}

impl From<Value> for Attribute<Value> {
    fn from(value: Value) -> Self {
        Self {
            rel: Value::unit(),
            val: value,
        }
    }
}

impl From<Value> for Attr {
    fn from(value: Value) -> Self {
        Self::Unit(value)
    }
}

impl From<EndoTuple<Value>> for Attr {
    fn from(value: EndoTuple<Value>) -> Self {
        Self::Tuple(Box::new(value))
    }
}

impl<R, V, T> From<(R, V)> for Attribute<T>
where
    T: From<R> + From<V>,
{
    fn from((rel, val): (R, V)) -> Self {
        Self {
            rel: rel.into(),
            val: val.into(),
        }
    }
}

impl<T> Index<usize> for Attribute<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        match index {
            0 => &self.rel,
            1 => &self.val,
            _ => panic!("Out of bounds for Attribute"),
        }
    }
}

pub struct ValueDebug<'v, V>(pub &'v V);

impl<'v> Display for ValueDebug<'v, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = &self.0;
        match &value {
            Value::Unit(_) => write!(f, "#u"),
            Value::Void(_) => write!(f, "#void"),
            Value::I64(i, _) => write!(f, "int({i})"),
            Value::F64(n, _) => write!(f, "flt({n})"),
            Value::Rational(r, _) => write!(f, "rat({r})"),
            Value::Serial(Serial(int), _) => write!(f, "serial({int})"),
            Value::Text(s, _) => write!(f, "'{s}'"),
            Value::OctetSequence(vec, _) => write!(f, "b'{vec:x?}'"),
            Value::ChronoDateTime(dt, _) => write!(f, "datetime({dt})"),
            Value::ChronoDate(d, _) => write!(f, "date({d})"),
            Value::ChronoTime(t, _) => write!(f, "time({t})"),
            Value::Struct(m, _) | Value::StructUpdate(m, _) => {
                if matches!(value, Value::StructUpdate(..)) {
                    write!(f, "update")?;
                }

                write!(f, "{{")?;
                let mut iter = m.iter().peekable();
                while let Some((prop, attr)) = iter.next() {
                    write!(f, "{prop} -> {}", ValueDebug(attr),)?;

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
                write!(f, "{}", ValueDebug(seq))
            }
            Value::Patch(patch, _) => {
                write!(f, "patch{{")?;
                for (pos, attr) in patch.iter().with_position() {
                    write!(f, "{}", ValueDebug(attr),)?;
                    if matches!(pos, Position::First | Position::Middle) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")
            }
            Value::DeleteRelationship(_) => {
                write!(f, "DELETE_RELATIONSHIP")
            }
            Value::Filter(..) => write!(f, "filter"),
        }
    }
}

impl<'a> Display for ValueDebug<'a, Attribute> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.0;
        write!(f, "{}", ValueDebug(&attr.val))?;

        if !attr.rel.is_unit() {
            write!(f, "::{}", ValueDebug(&attr.rel))?;
        }

        Ok(())
    }
}

impl<'a> Display for ValueDebug<'a, Attr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.0;

        match attr {
            Attr::Unit(value) => {
                write!(f, "{}", ValueDebug(value))?;
            }
            Attr::Tuple(tuple) => {
                write!(f, "{}", ValueDebug(&tuple.elements))?;
            }
            Attr::Matrix(mat) => {
                write!(f, "{}", ValueDebug(mat))?;
            }
        }

        Ok(())
    }
}

impl<'a, T> Display for ValueDebug<'a, EndoTupleElements<T>>
where
    ValueDebug<'a, T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        for (pos, element) in self.0.iter().with_position() {
            write!(f, "{}", ValueDebug(element))?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }
        write!(f, ")")
    }
}

impl<'a, T> Display for ValueDebug<'a, Sequence<T>>
where
    ValueDebug<'a, T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (pos, element) in self.0.elements.iter().with_position() {
            write!(f, "{}", ValueDebug(element))?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

impl<'a> Display for ValueDebug<'a, AttrMatrix> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mat{}", ValueDebug(&self.0.elements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use crate::{PackageId, RelationshipId};

    use super::*;
    use num::rational::BigRational;

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
        assert_eq!(48, std::mem::size_of::<HashMap<RelationshipId, Value>>());

        assert_eq!(32, std::mem::size_of::<Attr>());
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

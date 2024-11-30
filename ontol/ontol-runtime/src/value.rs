use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use ::serde::{Deserialize, Serialize};
use compact_str::CompactString;
use fnv::FnvHashMap;
use itertools::{Itertools, Position};
use thin_vec::ThinVec;

use crate::{
    attr::{Attr, AttrMatrix},
    cast::Cast,
    crdt::CrdtStruct,
    ontology::aspects::DefsAspect,
    query::filter::Filter,
    sequence::Sequence,
    tuple::EndoTupleElements,
    DefId, DomainIndex, OntolDefTag, PropId,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// Unit value for DefIds that have only one possible value
    Unit(ValueTag),
    /// Void represents the absence of value - used in error detection and try mechanism
    Void(ValueTag),
    I64(i64, ValueTag),
    F64(f64, ValueTag),
    Serial(Serial, ValueTag),
    Rational(Box<num::rational::BigRational>, ValueTag),
    Text(CompactString, ValueTag),
    OctetSequence(OctetSequence, ValueTag),
    ChronoDateTime(chrono::DateTime<chrono::Utc>, ValueTag),
    ChronoDate(chrono::NaiveDate, ValueTag),
    ChronoTime(chrono::NaiveTime, ValueTag),

    /// A collection of attributes keyed by property.
    Struct(Box<FnvHashMap<PropId, Attr>>, ValueTag),

    /// A conflict-free collection of attributes keyed by property, will full editing history
    CrdtStruct(CrdtStruct, ValueTag),

    /// A collection of arbitrary values keyed by strings.
    Dict(Box<BTreeMap<CompactString, Value>>, ValueTag),

    /// A sequence of values.
    ///
    /// The difference between a Sequence and a Map is that
    /// sequences use numeric keys instead of RelationshipId.
    ///
    /// Some sequences will be uniform (all elements have the same type).
    /// Other sequences will behave more like tuples.
    ///
    /// FIXME: Refactor this to be a pure Array of Values
    Sequence(Sequence<Value>, ValueTag),

    /// Special rel_params used for edge deletion
    DeleteRelationship(ValueTag),

    Filter(Box<Filter>, ValueTag),
}

impl Value {
    pub fn new_struct(props: impl IntoIterator<Item = (PropId, Attr)>, tag: ValueTag) -> Self {
        Self::Struct(Box::new(FnvHashMap::from_iter(props)), tag)
    }

    pub fn boolean(value: bool) -> Self {
        Self::I64(
            if value { 1 } else { 0 },
            OntolDefTag::Boolean.def_id().into(),
        )
    }

    pub fn i64(value: i64) -> Self {
        Self::I64(value, OntolDefTag::I64.def_id().into())
    }

    pub fn f64(value: f64) -> Self {
        Self::F64(value, OntolDefTag::F64.def_id().into())
    }

    pub fn text(value: impl Into<CompactString>) -> Self {
        Self::Text(value.into(), OntolDefTag::Text.def_id().into())
    }

    pub fn datetime(value: chrono::DateTime<chrono::Utc>) -> Self {
        Self::ChronoDateTime(value, OntolDefTag::DateTime.def_id().into())
    }

    pub fn octet_sequence(value: ThinVec<u8>) -> Self {
        Self::OctetSequence(OctetSequence(value), OntolDefTag::Octets.def_id().into())
    }

    pub fn sequence_of(values: impl IntoIterator<Item = Value>) -> Self {
        let sequence: Sequence<Value> = values.into_iter().collect();
        let type_def_id = sequence
            .elements()
            .first()
            .map(|value| value.tag())
            .unwrap_or(ValueTag::unit());
        Self::Sequence(sequence, type_def_id)
    }

    #[inline]
    pub const fn unit() -> Self {
        Self::Unit(ValueTag::unit())
    }

    pub const fn tag(&self) -> ValueTag {
        match self {
            Value::Unit(tag) => *tag,
            Value::Void(tag) => *tag,
            Value::I64(_, tag) => *tag,
            Value::F64(_, tag) => *tag,
            Value::Serial(_, tag) => *tag,
            Value::Rational(_, tag) => *tag,
            Value::Text(_, tag) => *tag,
            Value::OctetSequence(_, tag) => *tag,
            Value::ChronoDateTime(_, tag) => *tag,
            Value::ChronoDate(_, tag) => *tag,
            Value::ChronoTime(_, tag) => *tag,
            Value::Struct(_, tag) => *tag,
            Value::CrdtStruct(_, tag) => *tag,
            Value::Dict(_, tag) => *tag,
            Value::Sequence(_, tag) => *tag,
            Value::DeleteRelationship(tag) => *tag,
            Value::Filter(_, tag) => *tag,
        }
    }

    pub const fn type_def_id(&self) -> DefId {
        self.tag().def_id()
    }

    pub fn tag_mut(&mut self) -> &mut ValueTag {
        match self {
            Value::Unit(tag) => tag,
            Value::Void(tag) => tag,
            Value::I64(_, tag) => tag,
            Value::F64(_, tag) => tag,
            Value::Serial(_, tag) => tag,
            Value::Rational(_, tag) => tag,
            Value::Text(_, tag) => tag,
            Value::OctetSequence(_, tag) => tag,
            Value::ChronoDateTime(_, tag) => tag,
            Value::ChronoDate(_, tag) => tag,
            Value::ChronoTime(_, tag) => tag,
            Value::Struct(_, tag) => tag,
            Value::CrdtStruct(_, tag) => tag,
            Value::Dict(_, tag) => tag,
            Value::Sequence(_, tag) => tag,
            Value::DeleteRelationship(tag) => tag,
            Value::Filter(_, tag) => tag,
        }
    }

    /// Take the value, leave a Unit value behind.
    pub fn take(&mut self) -> Value {
        let mut value = Self::unit();
        std::mem::swap(self, &mut value);
        value
    }

    pub fn is_unit(&self) -> bool {
        DefId::from(self.tag()) == DefId::unit()
    }

    pub fn filter_non_unit(&self) -> Option<&Self> {
        if self.is_unit() {
            None
        } else {
            Some(self)
        }
    }

    pub fn get_attribute(&self, prop_id: PropId) -> Option<&Attr> {
        match self {
            Self::Struct(map, _) => map.get(&prop_id),
            _ => None,
        }
    }

    pub fn get_attribute_value(&self, prop_id: PropId) -> Option<&Value> {
        match self.get_attribute(prop_id)? {
            Attr::Unit(value) => Some(value),
            _ => None,
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OctetSequence(pub ThinVec<u8>);

impl Debug for OctetSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }

        Ok(())
    }
}

/// Formatter for values, outputs a "raw" format
/// that may not be in accordance with domain interfaces for complex values.
/// For that, the value needs to be serialized instead.
pub struct ValueFormatRaw<'d, 'on> {
    value: &'d Value,
    type_def_id: DefId,
    defs: &'on DefsAspect,
}

impl<'d, 'on> ValueFormatRaw<'d, 'on> {
    pub fn new(value: &'d Value, type_def_id: DefId, defs: &'on DefsAspect) -> Self {
        Self {
            value,
            type_def_id,
            defs,
        }
    }
}

impl Display for ValueFormatRaw<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(text_like_type) = self.defs.text_like_types.get(&self.type_def_id) {
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
                    let use_z = true;
                    write!(
                        f,
                        "{}",
                        datetime.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, use_z)
                    )
                }
                Value::Struct(props, tag) => {
                    // concatenate every prop (not sure this is a good idea, since the order is not defined)
                    for attr in props.values() {
                        if let Attr::Unit(value) = attr {
                            ValueFormatRaw {
                                value,
                                type_def_id: (*tag).into(),
                                defs: self.defs,
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

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct ValueTag {
    first: u16,
    second: u16,
}

impl ValueTag {
    pub const fn unit() -> Self {
        Self {
            first: 0,
            second: OntolDefTag::Unit as u16,
        }
    }

    pub const fn def_id(&self) -> DefId {
        DefId(
            DomainIndex(self.first & TagFlags::PKG_MASk.bits()),
            self.second,
        )
    }

    pub fn with_is_update(mut self, is_update: bool) -> Self {
        if is_update {
            self.set_is_update()
        }
        self
    }

    pub fn set_def_id(&mut self, def_id: DefId) {
        self.first =
            def_id.domain_index().0 | (self.first & TagFlags::PKG_MASk.complement().bits());
        self.second = def_id.1;
    }

    pub fn set_is_update(&mut self) {
        self.first |= TagFlags::UPDATE.bits();
        assert!(self.is_update());
    }

    pub fn set_is_delete(&mut self) {
        self.first |= TagFlags::DELETE.bits();
        assert!(self.is_delete());
    }

    pub fn is_update(&self) -> bool {
        (self.first & TagFlags::UPDATE.bits()) != 0
    }

    pub fn is_delete(&self) -> bool {
        (self.first & TagFlags::DELETE.bits()) != 0
    }
}

impl Debug for ValueTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let flags = TagFlags::from_bits(self.first & TagFlags::PKG_MASk.complement().bits());
        write!(f, "tag({:?}, {:?})", self.def_id(), flags)
    }
}

impl From<ValueTag> for DefId {
    fn from(value: ValueTag) -> Self {
        value.def_id()
    }
}

impl From<DefId> for ValueTag {
    fn from(value: DefId) -> Self {
        Self {
            first: value.domain_index().0,
            second: value.1,
        }
    }
}

pub struct ValueTagError;

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub(crate) struct TagFlags: u16 {
        const PKG_MASk = 0b0011111111111111;
        const DELETE   = 0b1000000000000000;
        const UPDATE   = 0b0100000000000000;
    }
}

pub struct ValueDebug<'v, V>(pub &'v V);

impl Display for ValueDebug<'_, Value> {
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
            Value::Struct(m, _) => {
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
            Value::CrdtStruct(..) => {
                write!(f, "CRDT")
            }
            Value::Dict(..) => {
                write!(f, "dict")
            }
            Value::Sequence(seq, _) => {
                write!(f, "{}", ValueDebug(seq))
            }
            Value::DeleteRelationship(_) => {
                write!(f, "DELETE_RELATIONSHIP")
            }
            Value::Filter(..) => write!(f, "filter"),
        }
    }
}

impl Display for ValueDebug<'_, Attr> {
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

impl Display for ValueDebug<'_, AttrMatrix> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mat{}", ValueDebug(&self.0.columns))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use crate::PropId;

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

        assert_eq!(24, std::mem::size_of::<BTreeMap<PropId, Value>>());
        assert_eq!(24, std::mem::size_of::<Vec<Value>>());
        assert_eq!(48, std::mem::size_of::<HashMap<PropId, Value>>());

        assert_eq!(32, std::mem::size_of::<Attr>());
    }
}

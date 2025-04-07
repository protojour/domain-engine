use std::fmt::Debug;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use crate::{impl_ontol_debug, vec_map::VecMapKey};

/// NB: The numbers here get serialized and persisted.
/// Think twice before changing number values.
#[repr(u16)]
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "kebab-case")]
pub enum OntolDefTag {
    /// The ONTOL domain itself
    Ontol = 0,
    Unit = 1,
    False = 2,
    True = 3,
    Boolean = 4,
    EmptySequence = 5,
    EmptyText = 6,
    Number = 7,
    Integer = 8,
    I64 = 9,
    Float = 10,
    F32 = 11,
    F64 = 12,
    Serial = 13,
    Text = 14,
    Octets = 15,
    Vertex = 16,
    Uuid = 17,
    Ulid = 18,
    DateTime = 19,
    /// An open, domainless relationship between some value and arbitrary, quasi-structured data
    RelationOpenData = 20,
    RelationEdge = 21,
    RelationFlatUnion = 22,
    RelationIs = 23,
    RelationIdentifies = 24,
    RelationId = 25,
    RelationIndexed = 26,
    RelationStoreKey = 27,
    RelationMin = 28,
    RelationMax = 29,
    RelationDefault = 30,
    RelationGen = 31,
    RelationOrder = 32,
    RelationDirection = 33,
    RelationExample = 34,
    RelationDataStoreAddress = 35,
    /// Union of `ascending` and `descending`
    /// TODO: RelationDirection and this union can be the same def, which would be cleaner:
    UnionDirection = 36,
    SymAscending = 37,
    SymDescending = 38,
    /// The `auto` value generator mode
    Auto = 39,
    /// Create time for generators, can also be used as a domain-independent property
    CreateTime = 40,
    /// Update time for generators, can also be used as a domain-independent property
    UpdateTime = 41,
    Format = 42,
    FormatHex = 43,
    FormatBase64 = 44,

    RelationRepr = 45,
    /// The `crdt` symbol
    Crdt = 46,
    /// This must be the last entry. Update the value accordingly.
    _LastEntry = 47,
}

impl_ontol_debug!(OntolDefTag);

#[derive(Clone, Copy)]
pub enum OntolDefKind {
    Domain,
    Module,
    Primitive,
    Relation,
}

impl OntolDefTag {
    pub fn ontol_path(self) -> &'static [&'static str] {
        match self {
            OntolDefTag::Ontol => &[],
            OntolDefTag::Unit => &[],
            OntolDefTag::False => &["false"],
            OntolDefTag::True => &["true"],
            OntolDefTag::Boolean => &["boolean"],
            OntolDefTag::EmptySequence => &[],
            OntolDefTag::EmptyText => &[],
            OntolDefTag::Number => &["number"],
            OntolDefTag::Integer => &["integer"],
            OntolDefTag::I64 => &["i64"],
            OntolDefTag::Float => &["float"],
            OntolDefTag::F32 => &["f32"],
            OntolDefTag::F64 => &["f64"],
            OntolDefTag::Serial => &["serial"],
            OntolDefTag::Text => &["text"],
            OntolDefTag::Octets => &["octets"],
            OntolDefTag::Vertex => &["vertex"],
            OntolDefTag::Uuid => &["uuid"],
            OntolDefTag::Ulid => &["ulid"],
            OntolDefTag::DateTime => &["datetime"],
            OntolDefTag::RelationOpenData => &[],
            OntolDefTag::RelationEdge => &[],
            OntolDefTag::RelationFlatUnion => &[],
            OntolDefTag::RelationIs => &["is"],
            OntolDefTag::RelationIdentifies => &[],
            OntolDefTag::RelationId => &[],
            OntolDefTag::RelationIndexed => &[],
            OntolDefTag::RelationStoreKey => &["store_key"],
            OntolDefTag::RelationMin => &["min"],
            OntolDefTag::RelationMax => &["max"],
            OntolDefTag::RelationDefault => &["default"],
            OntolDefTag::RelationGen => &["gen"],
            OntolDefTag::RelationOrder => &["order"],
            OntolDefTag::RelationDirection => &["direction"],
            OntolDefTag::RelationExample => &["example"],
            OntolDefTag::RelationDataStoreAddress => &[],
            OntolDefTag::UnionDirection => &[],
            OntolDefTag::SymAscending => &["ascending"],
            OntolDefTag::SymDescending => &["descending"],
            OntolDefTag::Auto => &["auto"],
            OntolDefTag::CreateTime => &["create_time"],
            OntolDefTag::UpdateTime => &["update_time"],
            OntolDefTag::Format => &["format"],
            OntolDefTag::FormatHex => &["format", "hex"],
            OntolDefTag::FormatBase64 => &["format", "base64"],
            OntolDefTag::RelationRepr => &["repr"],
            OntolDefTag::Crdt => &["crdt"],
            OntolDefTag::_LastEntry => &[],
        }
    }

    pub fn def_kind(self) -> Option<OntolDefKind> {
        match self {
            OntolDefTag::Ontol => Some(OntolDefKind::Domain),
            OntolDefTag::Unit => Some(OntolDefKind::Primitive),
            OntolDefTag::False => Some(OntolDefKind::Primitive),
            OntolDefTag::True => Some(OntolDefKind::Primitive),
            OntolDefTag::Boolean => Some(OntolDefKind::Primitive),
            OntolDefTag::EmptySequence => Some(OntolDefKind::Primitive),
            OntolDefTag::EmptyText => Some(OntolDefKind::Primitive),
            OntolDefTag::Number => Some(OntolDefKind::Primitive),
            OntolDefTag::Integer => Some(OntolDefKind::Primitive),
            OntolDefTag::I64 => Some(OntolDefKind::Primitive),
            OntolDefTag::Float => Some(OntolDefKind::Primitive),
            OntolDefTag::F32 => Some(OntolDefKind::Primitive),
            OntolDefTag::F64 => Some(OntolDefKind::Primitive),
            OntolDefTag::Serial => Some(OntolDefKind::Primitive),
            OntolDefTag::Text => Some(OntolDefKind::Primitive),
            OntolDefTag::Octets => Some(OntolDefKind::Primitive),
            OntolDefTag::Vertex => Some(OntolDefKind::Primitive),
            OntolDefTag::Uuid => Some(OntolDefKind::Primitive),
            OntolDefTag::Ulid => Some(OntolDefKind::Primitive),
            OntolDefTag::DateTime => Some(OntolDefKind::Primitive),
            OntolDefTag::RelationOpenData => Some(OntolDefKind::Relation),
            OntolDefTag::RelationEdge => Some(OntolDefKind::Relation),
            OntolDefTag::RelationFlatUnion => Some(OntolDefKind::Relation),
            OntolDefTag::RelationIs => Some(OntolDefKind::Relation),
            OntolDefTag::RelationIdentifies => Some(OntolDefKind::Relation),
            OntolDefTag::RelationId => Some(OntolDefKind::Relation),
            OntolDefTag::RelationIndexed => Some(OntolDefKind::Relation),
            OntolDefTag::RelationStoreKey => Some(OntolDefKind::Relation),
            OntolDefTag::RelationMin => Some(OntolDefKind::Relation),
            OntolDefTag::RelationMax => Some(OntolDefKind::Relation),
            OntolDefTag::RelationDefault => Some(OntolDefKind::Relation),
            OntolDefTag::RelationGen => Some(OntolDefKind::Relation),
            OntolDefTag::RelationOrder => Some(OntolDefKind::Relation),
            OntolDefTag::RelationDirection => Some(OntolDefKind::Relation),
            OntolDefTag::RelationExample => Some(OntolDefKind::Relation),
            OntolDefTag::RelationDataStoreAddress => Some(OntolDefKind::Relation),
            OntolDefTag::UnionDirection => Some(OntolDefKind::Primitive),
            OntolDefTag::SymAscending => Some(OntolDefKind::Primitive),
            OntolDefTag::SymDescending => Some(OntolDefKind::Primitive),
            OntolDefTag::Auto => Some(OntolDefKind::Relation),
            OntolDefTag::CreateTime => Some(OntolDefKind::Primitive),
            OntolDefTag::UpdateTime => Some(OntolDefKind::Primitive),
            OntolDefTag::Format => Some(OntolDefKind::Module),
            OntolDefTag::FormatHex => Some(OntolDefKind::Primitive),
            OntolDefTag::FormatBase64 => Some(OntolDefKind::Primitive),
            OntolDefTag::RelationRepr => Some(OntolDefKind::Relation),
            OntolDefTag::Crdt => Some(OntolDefKind::Primitive),
            OntolDefTag::_LastEntry => None,
        }
    }
}

bitflags::bitflags! {
    /// This is both compiler, runtime and parser related, and thus lives in core, for now.
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct TagFlags: u16 {
        const PKG_MASK = 0b0011111111111111;
        const DELETE   = 0b1000000000000000;
        const UPDATE   = 0b0100000000000000;
    }
}

pub struct ValueTagError;

/// Identifies one domain of ONTOL code within one ontology or compiler session.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DomainIndex(u16);

impl DomainIndex {
    /// The ontol domain is always the first domain in an ontology
    pub const fn ontol() -> Self {
        Self(0)
    }

    pub const fn second() -> Self {
        Self(1)
    }

    pub const fn from_u16(value: u16) -> Result<Self, ValueTagError> {
        if value <= TagFlags::PKG_MASK.bits() {
            Ok(Self(value))
        } else {
            Err(ValueTagError)
        }
    }

    pub const fn from_u16_and_mask(value: u16, mask: TagFlags) -> Self {
        Self(value & mask.bits())
    }

    pub fn increase(&mut self) -> Result<(), ValueTagError> {
        if self.0 < TagFlags::PKG_MASK.bits() {
            self.0 += 1;
            Ok(())
        } else {
            Err(ValueTagError)
        }
    }

    pub const fn index(self) -> u16 {
        self.0
    }
}

impl VecMapKey for DomainIndex {
    fn index(&self) -> usize {
        self.0 as usize
    }
}

impl TryFrom<u16> for DomainIndex {
    type Error = ValueTagError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::from_u16(value)
    }
}

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for DomainIndex {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "DomainIndex({:?})", self.0)
    }
}

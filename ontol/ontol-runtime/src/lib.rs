#![forbid(unsafe_code)]

use std::{collections::BTreeSet, fmt::Debug, str::FromStr};

use ::serde::{Deserialize, Serialize};
use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use num_enum::TryFromPrimitive;
use ontol_core::impl_ontol_debug;
use ontol_macros::OntolDebug;
use smallvec::SmallVec;
use value::{TagFlags, ValueTagError};
use vec_map::VecMapKey;

pub use ontol_core::property;

pub mod attr;
pub mod cast;
pub mod constant;
pub mod crdt;
pub mod debug;
pub mod format_utils;
pub mod interface;
pub mod ontology;
pub mod phf;
pub mod query;
pub mod resolve_path;
pub mod sequence;
pub mod tuple;
pub mod value;
pub mod var;
pub mod vec_map;
pub mod vm;

mod equality;

extern crate self as ontol_runtime;

/// Identifies one domain of ONTOL code within one ontology or or compiler session.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
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
        if value <= TagFlags::PKG_MASk.bits() {
            Ok(Self(value))
        } else {
            Err(ValueTagError)
        }
    }

    pub fn increase(&mut self) -> Result<(), ValueTagError> {
        if self.0 < TagFlags::PKG_MASk.bits() {
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

/// One definition inside some ONTOL domain.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct DefId(pub DomainIndex, pub u16);

impl DefId {
    pub fn domain_index(&self) -> DomainIndex {
        self.0
    }
}

impl Debug for DefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "def@{}:{}", self.0.0, self.1)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseDefIdError;

impl FromStr for DefId {
    type Err = ParseDefIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (domain_idx, def_tag) = s
            .strip_prefix("def@")
            .and_then(|s| s.split_once(':'))
            .ok_or(ParseDefIdError)?;
        let domain_idx = domain_idx.parse::<u16>().map_err(|_| ParseDefIdError)?;
        let def_tag = def_tag.parse::<u16>().map_err(|_| ParseDefIdError)?;
        Ok(DefId(DomainIndex(domain_idx), def_tag))
    }
}

impl_ontol_debug!(DefId);

impl DefId {
    #[inline]
    pub const fn unit() -> Self {
        DefId(DomainIndex(0), OntolDefTag::Unit as u16)
    }
}

/// NB: The numbers here get serialized and persisted.
/// Think twice before changing number values.
#[repr(u16)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TryFromPrimitive)]
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

impl OntolDefTag {
    pub const fn def_id(self) -> DefId {
        DefId(DomainIndex(0), self as u16)
    }

    pub const fn prop_id_0(self) -> PropId {
        PropId(self.def_id(), DefPropTag(0))
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct MapKey {
    pub input: MapDef,
    pub output: MapDef,
    pub flags: MapFlags,
}

impl MapKey {
    pub fn def_ids(&self) -> [DefId; 2] {
        [self.input.def_id, self.output.def_id]
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct MapFlags: u8 {
        /// The mapping is a _pure_ but _partial_ version of an impure/lossy mapping that needs to access to a datastore.
        /// Used to implement translation of UPDATEs.
        const PURE_PARTIAL = 0b00000001;
    }
}

/// The direction of mapping.
#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub enum MapDirection {
    /// The down direction decreases domain order towards storage.
    /// It concretizes towards persisted model.
    Down,
    /// A mixed direction may appear when mapping is inferred from other
    /// mappings, for example for type unions.
    Mixed,
    /// The up direction increases domain order towards interface.
    /// It abstracts away from persisted model.
    Up,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct MapDef {
    pub def_id: DefId,
    pub flags: MapDefFlags,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct MapDefFlags: u8 {
        /// The type is a sequence rather than a single value
        const SEQUENCE        = 0b00000001;
    }
}

impl From<DefId> for MapDef {
    fn from(value: DefId) -> Self {
        Self {
            def_id: value,
            flags: MapDefFlags::empty(),
        }
    }
}

/// The tag of a relationship belonging to a specific Def.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct DefPropTag(pub u16);

impl ::std::fmt::Debug for DefPropTag {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "p@{}", self.0)
    }
}

/// The id of a property
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PropId(pub DefId, pub DefPropTag);

impl PropId {
    pub const fn open_data() -> Self {
        Self(OntolDefTag::RelationOpenData.def_id(), DefPropTag(0))
    }

    pub const fn data_store_address() -> Self {
        Self(
            OntolDefTag::RelationDataStoreAddress.def_id(),
            DefPropTag(0),
        )
    }

    pub fn tag(&self) -> DefPropTag {
        self.1
    }
}

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for PropId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "p@{}:{}:{}", self.0.0.0, self.0.1, self.1.0)
    }
}

impl ::std::fmt::Display for PropId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "p@{}:{}:{}", self.0.0.0, self.0.1, self.1.0)
    }
}

impl_ontol_debug!(PropId);

impl FromStr for PropId {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        s = s.strip_prefix("p@").ok_or(())?;

        let mut iterator = s.split(':');
        let domain_idx = DomainIndex(iterator.next().ok_or(())?.parse().map_err(|_| ())?);
        let def_idx: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;
        let def_rel_tag: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;

        if iterator.next().is_some() {
            return Err(());
        }

        Ok(PropId(DefId(domain_idx, def_idx), DefPropTag(def_rel_tag)))
    }
}

/// Sorted set of DefIds
#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct DefIdSet(SmallVec<DefId, 1>);

impl DefIdSet {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[DefId] {
        self.0.as_slice()
    }

    pub fn iter(&self) -> impl Iterator<Item = &DefId> {
        self.0.iter()
    }

    pub fn contains(&self, def_id: &DefId) -> bool {
        self.0.binary_search(def_id).is_ok()
    }

    pub fn insert(&mut self, def_id: DefId) {
        match self.0.binary_search(&def_id) {
            Ok(_pos) => {} // element already in vector @ `pos`
            Err(pos) => self.0.insert(pos, def_id),
        }
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl FromIterator<DefId> for DefIdSet {
    fn from_iter<T: IntoIterator<Item = DefId>>(iter: T) -> Self {
        let mut def_ids: SmallVec<DefId, 1> = iter.into_iter().collect();
        def_ids.sort();
        Self(def_ids)
    }
}

/// A BTreeSet is pre-sorted
impl From<BTreeSet<DefId>> for DefIdSet {
    fn from(value: BTreeSet<DefId>) -> Self {
        Self(value.into_iter().collect())
    }
}

pub type FnvIndexMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

#[cfg(test)]
mod tests {
    use crate::{DefId, DomainIndex};

    use super::DefIdSet;

    fn def(i: u16) -> DefId {
        DefId(DomainIndex::ontol(), i)
    }

    #[test]
    fn def_id_set_insert() {
        let mut set = DefIdSet::default();
        assert_eq!(0, set.len());

        set.insert(def(42));
        assert_eq!(1, set.len());

        set.insert(def(42));
        assert_eq!(1, set.len());

        set.insert(def(40));
        assert_eq!(2, set.len());

        set.insert(def(40));
        assert_eq!(2, set.len());

        set.insert(def(41));
        assert_eq!(3, set.len());
    }

    #[test]
    fn def_id_set_from_iterator() {
        let mut set = DefIdSet::from_iter([def(10), def(5), def(15)]);

        set.insert(def(5));
        assert_eq!(3, set.len());

        set.insert(def(10));
        assert_eq!(3, set.len());

        set.insert(def(15));
        assert_eq!(3, set.len());

        set.insert(def(20));
        assert_eq!(4, set.len());
    }
}

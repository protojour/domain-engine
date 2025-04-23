#![forbid(unsafe_code)]

use std::{collections::BTreeSet, fmt::Debug, str::FromStr};

use ::serde::{Deserialize, Serialize};
use fnv::FnvBuildHasher;
use indexmap::IndexMap;
pub use ontol_core::DomainId;
use ontol_core::impl_ontol_debug;
pub use ontol_core::tag::{DomainIndex, TagFlags};
use ontol_macros::OntolDebug;
use smallvec::SmallVec;

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
pub mod vm;

mod equality;

extern crate self as ontol_runtime;

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
        write!(f, "def@{}:{}", self.0.index(), self.1)
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
        Ok(DefId(
            DomainIndex::from_u16_and_mask(domain_idx, TagFlags::PKG_MASK),
            def_tag,
        ))
    }
}

impl_ontol_debug!(DefId);

impl DefId {
    #[inline]
    pub const fn unit() -> Self {
        DefId(DomainIndex::ontol(), OntolDefTag::Unit as u16)
    }
}

pub use ontol_core::tag::OntolDefTag;

pub trait OntolDefTagExt {
    fn def_id(self) -> DefId;

    fn prop_id_0(self) -> PropId;
}

impl OntolDefTagExt for OntolDefTag {
    fn def_id(self) -> DefId {
        DefId(DomainIndex::ontol(), self as u16)
    }

    fn prop_id_0(self) -> PropId {
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
    pub fn open_data() -> Self {
        Self(OntolDefTag::RelationOpenData.def_id(), DefPropTag(0))
    }

    pub fn data_store_address() -> Self {
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
        write!(f, "p@{}:{}:{}", self.0.0.index(), self.0.1, self.1.0)
    }
}

impl ::std::fmt::Display for PropId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "p@{}:{}:{}", self.0.0.index(), self.0.1, self.1.0)
    }
}

impl_ontol_debug!(PropId);

impl FromStr for PropId {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        s = s.strip_prefix("p@").ok_or(())?;

        let mut iterator = s.split(':');
        let domain_idx = DomainIndex::from_u16_and_mask(
            iterator.next().ok_or(())?.parse().map_err(|_| ())?,
            TagFlags::PKG_MASK,
        );
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

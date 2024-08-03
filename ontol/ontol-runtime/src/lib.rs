#![forbid(unsafe_code)]

use std::{collections::BTreeSet, fmt::Debug, str::FromStr};

use ::serde::{Deserialize, Serialize};
use ontol_macros::OntolDebug;
use smallvec::SmallVec;
use value::{TagFlags, ValueTagError};

pub mod attr;
pub mod cast;
pub mod debug;
pub mod format_utils;
pub mod interface;
pub mod ontology;
pub mod phf;
pub mod property;
pub mod query;
pub mod resolve_path;
pub mod rustdoc;
pub mod sequence;
pub mod tuple;
pub mod value;
pub mod var;
pub mod vm;

mod equality;

extern crate self as ontol_runtime;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PackageId(u16);

impl PackageId {
    pub const fn first() -> Self {
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

    pub const fn id(self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for PackageId {
    type Error = ValueTagError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::from_u16(value)
    }
}

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for PackageId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "PackageId({:?})", self.0)
    }
}

/// One definition inside some ONTOL domain.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct DefId(pub PackageId, pub u16);

impl DefId {
    pub fn package_id(&self) -> PackageId {
        self.0
    }
}

impl Debug for DefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "def@{}:{}", self.0 .0, self.1)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseDefIdError;

impl FromStr for DefId {
    type Err = ParseDefIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (package_id, def_id) = s
            .strip_prefix("def@")
            .and_then(|s| s.split_once(':'))
            .ok_or(ParseDefIdError)?;
        let package_id = package_id.parse::<u16>().map_err(|_| ParseDefIdError)?;
        let def_id = def_id.parse::<u16>().map_err(|_| ParseDefIdError)?;
        Ok(DefId(PackageId(package_id), def_id))
    }
}

impl_ontol_debug!(DefId);

impl DefId {
    #[inline]
    pub const fn unit() -> Self {
        DefId(PackageId(0), 0)
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
pub struct DefRelTag(pub u16);

impl DefRelTag {
    pub const fn order() -> Self {
        DefRelTag(65535)
    }

    pub const fn direction() -> Self {
        DefRelTag(65534)
    }

    pub const fn open_data() -> Self {
        DefRelTag(65533)
    }

    pub const fn flat_union() -> Self {
        DefRelTag(65532)
    }
}

impl ::std::fmt::Debug for DefRelTag {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "rel@{}", self.0)
    }
}

impl_ontol_debug!(DefRelTag);

impl FromStr for DefRelTag {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        s = s.strip_prefix("rel@").ok_or(())?;

        let def_rel_tag: u16 = s.parse().map_err(|_| ())?;

        Ok(DefRelTag(def_rel_tag))
    }
}

/// The ID of some relationship between ONTOL types.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct RelId(pub DefId, pub DefRelTag);

impl RelId {
    pub fn tag(&self) -> DefRelTag {
        self.1
    }
}

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for RelId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "rel@{}:{}:{}", self.0 .0 .0, self.0 .1, self.1 .0)
    }
}

impl ::std::fmt::Display for RelId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "rel@{}:{}:{}", self.0 .0 .0, self.0 .1, self.1 .0)
    }
}

impl_ontol_debug!(RelId);

impl FromStr for RelId {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        s = s.strip_prefix("rel@").ok_or(())?;

        let mut iterator = s.split(':');
        let package_id = PackageId(iterator.next().ok_or(())?.parse().map_err(|_| ())?);
        let def_idx: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;
        let def_rel_tag: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;

        if iterator.next().is_some() {
            return Err(());
        }

        Ok(RelId(DefId(package_id, def_idx), DefRelTag(def_rel_tag)))
    }
}

/// The ID of some relationship between ONTOL types.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub PackageId, pub u16);

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for EdgeId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "edge@{}:{}", self.0 .0, self.1)
    }
}

impl_ontol_debug!(EdgeId);

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

#[cfg(test)]
mod tests {
    use crate::{DefId, PackageId};

    use super::DefIdSet;

    fn def(i: u16) -> DefId {
        DefId(PackageId::first(), i)
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

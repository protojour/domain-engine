#![forbid(unsafe_code)]

use std::{fmt::Debug, str::FromStr};

use ::serde::{Deserialize, Serialize};
use ontol_macros::OntolDebug;
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

/// The ID of some relationship between ONTOL types.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct RelationshipId(pub DefId);

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for RelationshipId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "R:{}:{}", self.0 .0 .0, self.0 .1)
    }
}

impl ::std::fmt::Display for RelationshipId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "R:{}:{}", self.0 .0 .0, self.0 .1)
    }
}

impl_ontol_debug!(RelationshipId);

impl FromStr for RelationshipId {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        s = s.strip_prefix("R:").ok_or(())?;

        let mut iterator = s.split(':');
        let package_id = PackageId(iterator.next().ok_or(())?.parse().map_err(|_| ())?);
        let def_idx: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;

        if iterator.next().is_some() {
            return Err(());
        }

        Ok(RelationshipId(DefId(package_id, def_idx)))
    }
}

/// The ID of some relationship between ONTOL types.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub DefId);

/// This forces single-line output even when pretty-printed
impl ::std::fmt::Debug for EdgeId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "edge@{}:{}", self.0 .0 .0, self.0 .1)
    }
}

impl_ontol_debug!(EdgeId);

#![forbid(unsafe_code)]

use std::{fmt::Debug, str::FromStr};

use ::serde::{Deserialize, Serialize};
use ontol_macros::OntolDebug;

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
pub mod value;
pub mod var;
pub mod vm;

mod equality;

extern crate self as ontol_runtime;

/// Property mirroring is the old ONTOL property model,
/// trying to remove this by using edge cardinals instead
pub const MIRROR_PROP: bool = false;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PackageId(pub u16);

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
        write!(f, "RelationshipId({:?})", self.0)
    }
}

impl_ontol_debug!(RelationshipId);

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

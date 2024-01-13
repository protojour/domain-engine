#![forbid(unsafe_code)]

use std::fmt::Debug;

use ::serde::{Deserialize, Serialize};
use derive_debug_extras::DebugExtras;

pub mod cast;
pub mod condition;
pub mod config;
pub mod format_utils;
pub mod interface;
pub mod ontology;
pub mod resolve_path;
pub mod select;
pub mod sequence;
pub mod text_like_types;
pub mod text_pattern;
pub mod value;
pub mod value_generator;
pub mod var;
pub mod vm;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, DebugExtras,
)]
#[debug_single_tuple_inline]
pub struct PackageId(pub u16);

/// One definition inside some domain.
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

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct MapFlags: u8 {
        /// The mapping is a _pure_ but _partial_ version of an impure/lossy mapping that needs to access to a datastore.
        const PURE_PARTIAL    = 0b00000001;
    }
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub enum Role {
    Subject,
    Object,
}

impl Role {
    /// Get the role that is opposite to this role
    pub fn opposite(self) -> Self {
        match self {
            Self::Subject => Self::Object,
            Self::Object => Self::Subject,
        }
    }
}

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, DebugExtras,
)]
#[debug_single_tuple_inline]
pub struct RelationshipId(pub DefId);

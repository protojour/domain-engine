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
    pub def_id: DefId,
    pub seq: bool,
}

impl From<DefId> for MapKey {
    fn from(value: DefId) -> Self {
        Self {
            def_id: value,
            seq: false,
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

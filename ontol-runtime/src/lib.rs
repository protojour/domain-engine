use std::fmt::Debug;

use ::serde::{Deserialize, Serialize};
use derive_debug_extras::DebugExtras;
use value::Value;

pub mod cast;
pub mod config;
pub mod discriminator;
pub mod format_utils;
pub mod interface;
pub mod ontology;
pub mod query;
pub mod text_like_types;
pub mod text_pattern;
pub mod value;
pub mod value_generator;
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

bitflags::bitflags! {
    /// Modifier for (de)serializers.
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
    pub struct SerdeModifier: u32 {
        /// No modifiers
        const NONE           = 0b00000000;
        /// Make an array of the DefId
        const ARRAY          = 0b00000001;
        /// Include Repr union of DefId
        const UNION          = 0b00000010;
        /// Include Repr intersection of DefId
        const INTERSECTION   = 0b00000100;
        /// Include the primary ID of the entity
        const PRIMARY_ID     = 0b00001000;
        /// Include inherent properties
        const INHERENT_PROPS = 0b00010000;
        /// Apply GraphQL field renaming
        const GRAPHQL        = 0b00100000;
    }
}

impl SerdeModifier {
    pub fn cross_def_flags(self) -> Self {
        self & Self::cross_def_mask()
    }

    pub fn reset(self) -> Self {
        Self::json_default() | self.cross_def_flags()
    }

    pub fn json_default() -> Self {
        Self::UNION | Self::INTERSECTION | Self::PRIMARY_ID | Self::INHERENT_PROPS
    }

    pub fn graphql_default() -> Self {
        Self::json_default() | Self::GRAPHQL
    }

    /// Flags that apply across DefIds
    pub fn cross_def_mask() -> Self {
        Self::GRAPHQL
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct DefVariant {
    pub def_id: DefId,
    pub modifier: SerdeModifier,
}

impl DefVariant {
    pub const fn new(def_id: DefId, modifier: SerdeModifier) -> Self {
        Self { def_id, modifier }
    }

    pub fn with_def(self, def_id: DefId) -> Self {
        Self {
            def_id,
            modifier: self.modifier,
        }
    }

    pub fn remove_modifier(self, diff: SerdeModifier) -> Self {
        Self {
            def_id: self.def_id,
            modifier: self.modifier.difference(diff),
        }
    }
}

impl Debug for DefVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DefVariant({:?}, {:?})", self.def_id, self.modifier)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DefParamId(pub u32);

impl Debug for DefParamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DefParamId({})", self.0)
    }
}

pub enum DefParam {
    Def(DefId),
    Const(Value),
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

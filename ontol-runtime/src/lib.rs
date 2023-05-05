use std::fmt::Debug;

use derive_debug_extras::DebugExtras;
use value::Value;

pub mod cast;
pub mod discriminator;
pub mod env;
pub mod format_utils;
pub mod json_schema;
pub mod query;
pub mod query_translate;
pub mod serde;
pub mod string_pattern;
pub mod string_types;
pub mod value;
pub mod vm;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct PackageId(pub u16);

/// One definition inside some domain.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DefId(pub PackageId, pub u16);

impl DefId {
    pub fn package_id(&self) -> PackageId {
        self.0
    }
}

impl Debug for DefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DefId({}, {})", self.0 .0, self.1)
    }
}

impl DefId {
    #[inline]
    pub const fn unit() -> Self {
        DefId(PackageId(0), 0)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
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
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    pub struct DataModifier: u32 {
        const NONE           = 0b00000000;
        const ARRAY          = 0b00000001;
        const UNION          = 0b00000010;
        const INTERSECTION   = 0b00000100;
        const PRIMARY_ID     = 0b00001000;
        const INHERENT_PROPS = 0b00010000;
    }
}

impl Default for DataModifier {
    fn default() -> Self {
        Self::UNION | Self::INTERSECTION | Self::PRIMARY_ID | Self::INHERENT_PROPS
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DefVariant {
    pub def_id: DefId,
    pub modifier: DataModifier,
}

impl DefVariant {
    pub const fn new(def_id: DefId, modifier: DataModifier) -> Self {
        Self { def_id, modifier }
    }

    pub fn with_def(self, def_id: DefId) -> Self {
        Self {
            def_id,
            modifier: self.modifier,
        }
    }

    pub fn with_local_mod(self, local_mod: DataModifier) -> Self {
        Self {
            def_id: self.def_id,
            modifier: local_mod,
        }
    }

    pub fn remove_modifier(self, diff: DataModifier) -> Self {
        Self {
            def_id: self.def_id,
            modifier: self.modifier.difference(diff),
        }
    }
}

impl Debug for DefVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DefVariant({}, {}, {:?})",
            self.def_id.0 .0, self.def_id.1, self.modifier,
        )
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct RelationId(pub DefId);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Role {
    Subject,
    Object,
}

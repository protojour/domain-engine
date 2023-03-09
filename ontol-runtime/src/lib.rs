use std::fmt::Debug;

use derive_debug_extras::DebugExtras;

pub mod cast;
pub mod discriminator;
pub mod env;
pub mod format_utils;
pub mod json_schema;
pub mod proc;
pub mod property_probe;
pub mod serde;
pub mod string_pattern;
pub mod string_types;
pub mod translate;
pub mod value;

mod vm;

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

bitflags::bitflags! {
    /// Modifier for (de)serializers.
    pub struct DataModifier: u32 {
        const IDENTITY = 0b00000000;
        const ARRAY    = 0b00000001;
        const UNION    = 0b00000010;
        const ID       = 0b00000100;
        const PROPS    = 0b00001000;
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DefVariant {
    pub def_id: DefId,
    pub local_mod: DataModifier,
    pub global_mod: DataModifier,
}

impl DefVariant {
    pub const fn identity(def_id: DefId) -> Self {
        Self {
            def_id: def_id,
            local_mod: DataModifier::IDENTITY,
            global_mod: DataModifier::IDENTITY,
        }
    }

    pub fn new_def(self, def_id: DefId) -> Self {
        Self {
            def_id,
            local_mod: self.local_mod,
            global_mod: self.global_mod,
        }
    }

    pub fn new_local_mod(self, local_mod: DataModifier) -> Self {
        Self {
            def_id: self.def_id,
            local_mod,
            global_mod: self.global_mod,
        }
    }

    pub fn local_mod_difference(self, local_mod: DataModifier) -> Self {
        Self {
            def_id: self.def_id,
            local_mod: self.local_mod.difference(local_mod),
            global_mod: self.global_mod,
        }
    }
}

impl Debug for DefVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DefVariant({}, {}, {:?})",
            self.def_id.0 .0, self.def_id.1, self.local_mod,
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct RelationId(pub DefId);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Role {
    Subject,
    Object,
}

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
    pub struct DataVariant: u32 {
        const IDENTITY = 0b00000000;
        const ARRAY    = 0b00000001;
        const ID       = 0b00000010;
        const UNION    = 0b00000100;
        const PROPS    = 0b00001000;
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DefVariant(pub DefId, pub DataVariant);

impl DefVariant {
    pub const fn identity(def_id: DefId) -> Self {
        Self(def_id, DataVariant::IDENTITY)
    }

    pub const fn id(&self) -> DefId {
        self.0
    }

    pub const fn data_variant(&self) -> DataVariant {
        self.1
    }
}

impl Debug for DefVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DefVariant({}, {}, {:?})",
            self.0 .0 .0, self.0 .1, self.1,
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

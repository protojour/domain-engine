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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct RelationId(pub DefId);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Role {
    Subject,
    Object,
}

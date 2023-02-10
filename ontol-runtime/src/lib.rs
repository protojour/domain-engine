use derive_debug_extras::DebugExtras;

pub mod cast;
pub mod discriminator;
pub mod env;
pub mod format_utils;
pub mod proc;
pub mod property_probe;
pub mod serde;
pub mod translate;
pub mod value;

mod vm;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct PackageId(pub u32);

/// One definition inside some domain.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct DefId(pub u32);

impl DefId {
    pub const fn unit() -> Self {
        DefId(0)
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

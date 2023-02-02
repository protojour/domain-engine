use std::fmt::Debug;

use derive_debug_extras::DebugExtras;

pub mod discriminator;
pub mod env;
pub mod proc;
pub mod property_probe;
pub mod serde;
pub mod translate;
pub mod value;

mod format_utils;
mod vm;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct PackageId(pub u32);

/// One definition inside some domain.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct DefId(pub u32);

/// A domain-specific property of a type.
#[derive(Clone, Copy, Eq, PartialEq, Hash, DebugExtras)]
pub struct PropertyId(pub u32);

pub struct ProcId(u32);

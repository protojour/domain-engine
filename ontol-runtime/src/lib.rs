use std::fmt::Debug;

pub mod env;
pub mod serde;
pub mod translation;
pub mod value;
pub mod vm;

/// Identifies one "package" of ONTOL code.
/// One package represents a domain,
/// but one package can consist of internal subdomains (probably).
/// So this is called package id (for now) until we have fleshed out a full architecture..
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct PackageId(pub u32);

/// One definition inside some domain.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct DefId(pub u32);

/// A domain-specific property of a type.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PropertyId(pub u32);

impl Debug for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PropertyId({})", self.0)
    }
}

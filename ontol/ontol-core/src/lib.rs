use std::{
    fmt::{Debug, Display},
    str::FromStr,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub mod debug;
pub mod error;
pub mod property;
pub mod rustdoc;
pub mod span;
pub mod tag;
pub mod url;
pub mod vec_map;

extern crate self as ontol_core;

/// Global domain ID
#[derive(
    Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize,
)]
pub struct DomainId {
    /// Unique identifier for the top level domain
    pub ulid: Ulid,
    /// Subdomain tag
    pub subdomain: u32,
}

/// Domain ID as used in an ontology
#[derive(Clone, Serialize, Deserialize)]
pub struct OntologyDomainId {
    /// The ID
    pub id: DomainId,
    /// Whether the ULID is stable
    pub stable: bool,
}

/// An ordinal representing a domain's topological sort in the dependency graph for the ontology
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct TopologyGeneration(pub u32);

/// Thin wrapper around Arc<String> that implements AsRef<str>
pub struct ArcString(pub Arc<String>);

impl Display for DomainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}§{}", self.ulid, self.subdomain)
    }
}

impl FromStr for DomainId {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ulid, subdomain) = s.split_once('§').unwrap_or((s, "0"));
        Ok(Self {
            ulid: Ulid::from_string(ulid).map_err(|_| "badly formatted domain ulid")?,
            subdomain: u32::from_str(subdomain).map_err(|_| "badly formatted subdomain tag")?,
        })
    }
}

impl AsRef<str> for ArcString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A reference to an ontol log in memory.
///
/// This should never be serialized or persisted in any way, it's only used in code analysis and compilation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRef(pub u16);

impl Debug for LogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "l{}", self.0)
    }
}

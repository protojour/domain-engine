use std::sync::Arc;

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

#[derive(Clone, Serialize, Deserialize)]
pub struct DomainId {
    pub ulid: Ulid,
    pub stable: bool,
}

/// An ordinal representing a domain's topological sort in the dependency graph for the ontology
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct TopologyGeneration(pub u32);

/// Thin wrapper around Arc<String> that implements AsRef<str>
pub struct ArcString(pub Arc<String>);

impl AsRef<str> for ArcString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

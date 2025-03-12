//! The ONTOL model of a property

use std::fmt::Debug;

use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

pub type Cardinality = (PropertyCardinality, ValueCardinality);

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum PropertyCardinality {
    Optional,
    Mandatory,
}

impl PropertyCardinality {
    pub fn is_mandatory(&self) -> bool {
        matches!(self, Self::Mandatory)
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional)
    }
}

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum ValueCardinality {
    /// The property supports at most one defined value
    Unit,
    /// The property supports a set of values.
    ///
    /// The set is indexed, i.e. remembers the order of entries.
    IndexSet,
    /// The property supports a list of values, duplicates are allowed.
    List,
}

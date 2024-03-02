use std::fmt::Debug;

use ::serde::{Deserialize, Serialize};

use crate::DefId;

use self::operator::{SerdeOperatorAddr, StructOperator};

mod deserialize;
mod deserialize_id;
mod deserialize_matcher;
mod deserialize_option;
mod deserialize_patch;
mod deserialize_raw;
mod deserialize_struct;
mod serialize;
mod serialize_raw;
mod utils;

pub mod operator;
pub mod processor;

pub use deserialize_raw::deserialize_raw;
pub use serialize_raw::serialize_raw;

const EDGE_PROPERTY: &str = "_edge";

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SerdeDef {
    pub def_id: DefId,
    pub modifier: SerdeModifier,
}

impl SerdeDef {
    pub const fn new(def_id: DefId, modifier: SerdeModifier) -> Self {
        Self { def_id, modifier }
    }

    pub fn with_def(self, def_id: DefId) -> Self {
        Self {
            def_id,
            modifier: self.modifier,
        }
    }

    pub fn remove_modifier(self, diff: SerdeModifier) -> Self {
        Self {
            def_id: self.def_id,
            modifier: self.modifier.difference(diff),
        }
    }
}

impl Debug for SerdeDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerdeDef({:?}, {:?})", self.def_id, self.modifier)
    }
}

bitflags::bitflags! {
    /// Modifier for (de)serializers.
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
    pub struct SerdeModifier: u32 {
        /// No modifiers
        const NONE           = 0b00000000;
        /// Make an array of the DefId
        const ARRAY          = 0b00000001;
        /// Include Repr union of DefId
        const UNION          = 0b00000010;
        /// Include Repr intersection of DefId
        const INTERSECTION   = 0b00000100;
        /// Include the primary ID of the entity
        const PRIMARY_ID     = 0b00001000;
        /// Include inherent properties
        const INHERENT_PROPS = 0b00010000;
        /// Apply GraphQL field renaming
        const GRAPHQL        = 0b00100000;
    }
}

impl SerdeModifier {
    pub fn cross_def_flags(self) -> Self {
        self & Self::cross_def_mask()
    }

    pub fn reset(self) -> Self {
        Self::json_default() | self.cross_def_flags()
    }

    pub fn json_default() -> Self {
        Self::UNION | Self::INTERSECTION | Self::PRIMARY_ID | Self::INHERENT_PROPS
    }

    pub fn graphql_default() -> Self {
        Self::json_default() | Self::GRAPHQL
    }

    /// Flags that apply across DefIds
    pub fn cross_def_mask() -> Self {
        Self::GRAPHQL
    }
}

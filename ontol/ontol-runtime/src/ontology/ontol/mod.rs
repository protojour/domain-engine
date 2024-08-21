//! Various ontology models related to the fundamentals of the `ontol` domain

pub mod text_pattern;

mod text;
mod text_like_types;
mod value_generator;

use serde::{Deserialize, Serialize};
pub use text::TextConstant;
pub use text_like_types::{ParseError, TextLikeType};
pub use value_generator::ValueGenerator;

use crate::{DefId, PropId};

#[derive(Serialize, Deserialize)]
pub struct OntolDomainMeta {
    pub bool: DefId,
    pub i64: DefId,
    pub f64: DefId,
    pub text: DefId,
    pub ascending: DefId,
    pub descending: DefId,
    pub data_store_address: DefId,
    pub open_data_relationship: DefId,
    pub order_relationship: DefId,
    pub direction_relationship: DefId,

    /// Serde-related: The constant name of the "_edge" property used to deserialize relation parameters
    pub edge_property: String,
}

impl OntolDomainMeta {
    pub fn open_data_prop_id(&self) -> PropId {
        PropId(self.open_data_relationship, crate::DefPropTag(0))
    }
}

impl Default for OntolDomainMeta {
    fn default() -> Self {
        Self {
            bool: DefId::unit(),
            i64: DefId::unit(),
            f64: DefId::unit(),
            text: DefId::unit(),
            ascending: DefId::unit(),
            descending: DefId::unit(),
            data_store_address: DefId::unit(),
            open_data_relationship: DefId::unit(),
            order_relationship: DefId::unit(),
            direction_relationship: DefId::unit(),
            edge_property: String::new(),
        }
    }
}

//! Various ontology models related to the fundamentals of the `ontol` domain

pub mod text_pattern;

mod text;
mod text_like_types;
mod value_generator;

use serde::{Deserialize, Serialize};
pub use text::TextConstant;
pub use text_like_types::{ParseError, TextLikeType};
pub use value_generator::ValueGenerator;

use crate::{
    property::{PropertyId, Role},
    DefId, RelationshipId,
};

#[derive(Serialize, Deserialize)]
pub struct OntolDomainMeta {
    pub bool: DefId,
    pub i64: DefId,
    pub f64: DefId,
    pub text: DefId,
    pub ascending: DefId,
    pub descending: DefId,
    pub open_data_relationship: DefId,
    pub order_relationship: DefId,
    pub direction_relationship: DefId,
}

impl OntolDomainMeta {
    pub fn open_data_property_id(&self) -> PropertyId {
        PropertyId {
            role: Role::Subject,
            relationship_id: RelationshipId(self.open_data_relationship),
        }
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
            open_data_relationship: DefId::unit(),
            order_relationship: DefId::unit(),
            direction_relationship: DefId::unit(),
        }
    }
}

//! Various ontology models related to the fundamentals of the `ontol` domain

pub mod text_pattern;

mod text;
mod text_like_types;
mod value_generator;

use serde::{Deserialize, Serialize};
pub use text::TextConstant;
pub use text_like_types::{ParseError, TextLikeType};
pub use value_generator::ValueGenerator;

#[derive(Default, Serialize, Deserialize)]
pub struct OntolDomainMeta {
    /// Serde-related: The constant name of the "_edge" property used to deserialize relation parameters
    pub edge_property: String,
}

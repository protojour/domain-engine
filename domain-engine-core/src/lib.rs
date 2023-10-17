pub mod data_store;
pub mod domain_error;
pub mod system;

mod domain_engine;
mod entity_id_utils;
mod filter;
mod in_memory_store;
mod resolve_path;
mod select_data_flow;
mod value_generator;

pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};
use ontol_runtime::{ontology::Cardinality, select::EntitySelect};

pub struct Config {
    pub default_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { default_limit: 20 }
    }
}

#[derive(Debug)]
pub struct EntityQuery {
    pub cardinality: Cardinality,
    pub select: EntitySelect,
}

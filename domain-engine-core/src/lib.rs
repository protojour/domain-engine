pub mod data_store;
mod domain_engine;
pub mod domain_error;

mod entity_id_utils;
mod in_memory_store;
mod resolve_path;

pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};

pub struct Config {
    pub default_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { default_limit: 20 }
    }
}

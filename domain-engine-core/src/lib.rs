#![forbid(unsafe_code)]

pub mod data_store;
pub mod domain_error;
pub mod entity_id_utils;
pub mod match_utils;
pub mod system;

mod domain_engine;
mod filter;
mod in_memory_store;
mod resolve_path;
mod select_data_flow;
mod value_generator;

use std::{collections::HashMap, hash::BuildHasher};

pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};
use ontol_runtime::{
    condition::{CondTerm, Condition},
    ontology::Cardinality,
    select::EntitySelect,
    var::Var,
};

pub struct Config {
    pub default_limit: usize,
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

pub trait FindEntityQuery {
    fn find_query(&mut self, match_var: Var, condition: &Condition<CondTerm>) -> EntityQuery;
}

impl<A: BuildHasher> FindEntityQuery for HashMap<Var, EntityQuery, A> {
    fn find_query(&mut self, match_var: Var, _condition: &Condition<CondTerm>) -> EntityQuery {
        self.remove(&match_var).unwrap()
    }
}

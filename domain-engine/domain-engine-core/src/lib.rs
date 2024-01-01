#![forbid(unsafe_code)]

pub mod data_store;
pub mod domain_error;
pub mod entity_id_utils;
pub mod filter;
pub mod match_utils;
pub mod object_generator;
pub mod system;

mod domain_engine;
mod select_data_flow;

use std::{collections::HashMap, hash::BuildHasher};

pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};
use ontol_runtime::{
    condition::{CondTerm, Condition},
    select::EntitySelect,
    var::Var,
};

pub trait FindEntitySelect {
    fn find_select(&mut self, match_var: Var, condition: &Condition<CondTerm>) -> EntitySelect;
}

impl<A: BuildHasher> FindEntitySelect for HashMap<Var, EntitySelect, A> {
    fn find_select(&mut self, match_var: Var, _condition: &Condition<CondTerm>) -> EntitySelect {
        self.remove(&match_var).unwrap()
    }
}

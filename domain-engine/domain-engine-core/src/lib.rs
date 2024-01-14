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
mod update;

use std::any::Any;
use std::{collections::HashMap, hash::BuildHasher, sync::Arc};

pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};
use ontol_runtime::{
    condition::{CondTerm, Condition},
    select::EntitySelect,
    var::Var,
};

/// A session that's passed through the DomainEngine APIs into the data store layer.
#[derive(Clone)]
pub struct Session(pub Arc<dyn Any + Send + Sync>);

impl Default for Session {
    fn default() -> Self {
        Self(Arc::new(()))
    }
}

pub trait FindEntitySelect {
    fn find_select(&mut self, match_var: Var, condition: &Condition<CondTerm>) -> EntitySelect;
}

impl<A: BuildHasher> FindEntitySelect for HashMap<Var, EntitySelect, A> {
    fn find_select(&mut self, match_var: Var, _condition: &Condition<CondTerm>) -> EntitySelect {
        self.remove(&match_var).unwrap()
    }
}

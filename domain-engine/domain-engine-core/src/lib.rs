#![forbid(unsafe_code)]

pub mod data_store;
pub mod domain_error;
pub mod entity_id_utils;
pub mod filter;
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
    query::{condition::Condition, select::EntitySelect},
    var::Var,
    DefId,
};

/// A session that's passed through the DomainEngine APIs into the data store layer.
#[derive(Clone)]
pub struct Session(pub Arc<dyn Any + Send + Sync>);

impl Default for Session {
    fn default() -> Self {
        Self(Arc::new(()))
    }
}

/// Information about whether the domain engine needs to perform a selection.
///
/// A reason it might not be interested is that a GraphQL query has not selected any of its output properties.
pub enum MaybeSelect {
    Select(EntitySelect),
    Skip(DefId),
}

pub trait FindEntitySelect {
    fn find_select(&mut self, match_var: Var, condition: &Condition) -> MaybeSelect;
}

impl<A: BuildHasher> FindEntitySelect for HashMap<Var, EntitySelect, A> {
    fn find_select(&mut self, match_var: Var, condition: &Condition) -> MaybeSelect {
        match self.remove(&match_var) {
            Some(entity_select) => MaybeSelect::Select(entity_select),
            None => match condition.root_def_id() {
                Some(def_id) => MaybeSelect::Skip(def_id),
                None => panic!("No information about what to default-select"),
            },
        }
    }
}

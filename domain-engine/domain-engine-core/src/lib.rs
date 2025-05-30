#![forbid(unsafe_code)]

pub mod data_store;
pub mod domain_error;
pub mod domain_select;
pub mod entity_id_utils;
pub mod filter;
pub mod make_interfacable;
pub mod make_storable;
pub mod search;
pub mod system;
pub mod transact;

mod crdt_util;
mod domain_engine;
mod select_data_flow;
mod update;

use std::any::Any;
use std::{collections::HashMap, hash::BuildHasher, sync::Arc};

use automerge::ActorId;
pub use domain_engine::DomainEngine;
pub use domain_error::{DomainError, DomainResult};
use ontol_runtime::{
    DefId,
    query::{condition::Condition, select::EntitySelect},
    var::Var,
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

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
pub enum SelectMode {
    Dynamic(EntitySelect),
    Static(DefId),
}

pub trait FindEntitySelect {
    fn find_select(&mut self, match_var: Var, condition: &Condition) -> SelectMode;
}

impl<A: BuildHasher> FindEntitySelect for HashMap<Var, EntitySelect, A> {
    fn find_select(&mut self, match_var: Var, condition: &Condition) -> SelectMode {
        match self.remove(&match_var) {
            Some(entity_select) => SelectMode::Dynamic(entity_select),
            None => match condition.root_def_id() {
                Some(def_id) => SelectMode::Static(def_id),
                None => panic!("No information about what to default-select"),
            },
        }
    }
}

pub type VertexAddr = SmallVec<u8, 12>;

/// An actor used to track changes in CRDT documents
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CrdtActor {
    /// An ID that identifies the system user of this actor.
    ///
    /// This way the system should be able to look up user information.
    pub user_id: String,

    /// An actor id the distinguishes different actors belonging to the same user
    pub actor_id: Uuid,
}

impl From<CrdtActor> for automerge::ActorId {
    fn from(value: CrdtActor) -> Self {
        let buf = postcard::to_allocvec(&value).unwrap();
        ActorId::from(buf)
    }
}

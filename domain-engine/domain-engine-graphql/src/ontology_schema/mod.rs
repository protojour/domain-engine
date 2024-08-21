use std::{ops::Deref, sync::Arc};

use crate::juniper::{self, EmptyMutation, EmptySubscription, RootNode};

use domain_engine_core::{data_store::DataStore, DomainEngine, DomainResult, Session};
use ontol_runtime::ontology::Ontology;

use self::gql_query::Query;

mod gql_def;
mod gql_dictionary;
mod gql_domain;
mod gql_query;
mod gql_rel;
mod gql_vertex;

/// The Ctx is the source of information for all Ontology GraphQL interactions.
#[derive(Clone)]
pub struct Ctx {
    domain_engine: Arc<DomainEngine>,
    session: Session,
}

impl Ctx {
    pub fn new(domain_engine: Arc<DomainEngine>, session: Session) -> Self {
        Self {
            domain_engine,
            session,
        }
    }

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    pub(crate) fn data_store(&self) -> DomainResult<&DataStore> {
        self.domain_engine.get_data_store()
    }
}

impl Deref for Ctx {
    type Target = Ontology;

    fn deref(&self) -> &Self::Target {
        self.domain_engine.ontology()
    }
}

impl juniper::Context for Ctx {}

pub type OntologySchema = RootNode<'static, Query, EmptyMutation<Ctx>, EmptySubscription<Ctx>>;

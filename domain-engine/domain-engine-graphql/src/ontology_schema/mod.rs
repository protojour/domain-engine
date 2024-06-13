use std::{ops::Deref, sync::Arc};

use crate::juniper::{self, EmptyMutation, EmptySubscription, RootNode};

use ontol_runtime::ontology::Ontology;

use self::gql_query::Query;

mod gql_dictionary;
mod gql_domain;
mod gql_query;
mod gql_rel;

/// The Ctx is the source of information for all Ontology GraphQL interactions.
#[derive(Clone)]
pub struct Ctx {
    ontology: Arc<Ontology>,
}

impl From<Arc<Ontology>> for Ctx {
    fn from(value: Arc<Ontology>) -> Self {
        Self { ontology: value }
    }
}

impl Deref for Ctx {
    type Target = Ontology;

    fn deref(&self) -> &Self::Target {
        &self.ontology
    }
}

impl juniper::Context for Ctx {}

pub type OntologySchema = RootNode<'static, Query, EmptyMutation<Ctx>, EmptySubscription<Ctx>>;

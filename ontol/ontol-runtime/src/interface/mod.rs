use std::{fmt::Debug, sync::Arc};

use ::serde::{Deserialize, Serialize};

use crate::ontology::OntologyInit;

use self::graphql::schema::GraphqlSchema;

pub mod discriminator;
pub mod graphql;
pub mod json_schema;
pub mod serde;

#[derive(Serialize, Deserialize)]
pub enum DomainInterface {
    GraphQL(Arc<graphql::schema::GraphqlSchema>),
}

impl Debug for DomainInterface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GraphQL(_) => write!(f, "GraphQL"),
        }
    }
}

impl OntologyInit for DomainInterface {
    fn ontology_init(&mut self, ontology: &crate::ontology::Ontology) {
        match self {
            Self::GraphQL(arc) => {
                let mut schema = Arc::new(GraphqlSchema::empty());
                std::mem::swap(arc, &mut schema);

                let Ok(mut schema) = Arc::try_unwrap(schema) else {
                    panic!();
                };

                schema.ontology_init(ontology);

                *arc = Arc::new(schema);
            }
        }
    }
}

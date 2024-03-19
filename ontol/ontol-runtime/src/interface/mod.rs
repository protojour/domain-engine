use std::{fmt::Debug, sync::Arc};

use ::serde::{Deserialize, Serialize};

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

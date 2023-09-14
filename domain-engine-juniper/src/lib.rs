use std::sync::Arc;

use domain_engine_core::DomainEngine;
use gql_scalar::GqlScalar;
use ontol_runtime::{
    ontology::{DomainProtocol, Ontology},
    PackageId,
};
use schema_ctx::SchemaCtx;
use thiserror::Error;
use tracing::debug;

pub mod gql_scalar;

mod look_ahead_utils;
mod macros;
mod query_analyzer;
mod registry_ctx;
mod schema_ctx;
mod templates;

#[derive(Clone)]
pub struct GqlContext {
    pub domain_engine: Arc<DomainEngine>,
}

impl From<DomainEngine> for GqlContext {
    fn from(value: DomainEngine) -> Self {
        Self {
            domain_engine: Arc::new(value),
        }
    }
}

impl From<Arc<DomainEngine>> for GqlContext {
    fn from(value: Arc<DomainEngine>) -> Self {
        Self {
            domain_engine: value,
        }
    }
}

pub mod juniper {
    pub use ::juniper::*;
}

impl juniper::Context for GqlContext {}

pub type Schema = juniper::RootNode<
    'static,
    templates::query_type::QueryType,
    templates::mutation_type::MutationType,
    juniper::EmptySubscription<GqlContext>,
    GqlScalar,
>;

#[derive(Debug, Error)]
pub enum CreateSchemaError {
    #[error("GraphQL protocol not found for given package")]
    GraphqlProtocolNotFound,
}

pub fn create_graphql_schema(
    package_id: PackageId,
    ontology: Arc<Ontology>,
) -> Result<Schema, CreateSchemaError> {
    let ontol_protocol_schema = ontology
        .get_domain_protocols(package_id)
        .ok_or(CreateSchemaError::GraphqlProtocolNotFound)?
        .iter()
        .find_map(|domain_protocol| match domain_protocol {
            DomainProtocol::GraphQL(schema) => Some(schema),
        })
        .ok_or(CreateSchemaError::GraphqlProtocolNotFound)?;

    let schema_ctx = Arc::new(SchemaCtx {
        schema: ontol_protocol_schema.clone(),
        ontology,
    });

    let juniper_schema = Schema::new_with_info(
        templates::query_type::QueryType,
        templates::mutation_type::MutationType,
        juniper::EmptySubscription::new(),
        schema_ctx.query_type_info(),
        schema_ctx.mutation_type_info(),
        (),
    );

    debug!("Created schema \n{}", juniper_schema.as_schema_language());

    Ok(juniper_schema)
}

/// Just some test code to be able to look at some macro expansions
#[cfg(test)]
mod test_derivations {
    #[derive(juniper::GraphQLInputObject)]
    struct TestInputObject {
        a: i32,
        b: String,
    }

    struct TestMutation;

    #[juniper::graphql_object]
    impl TestMutation {
        fn update_input_object(_obj: TestInputObject) -> f64 {
            42.0
        }

        fn option_ret(_obj: TestInputObject) -> Option<f64> {
            Some(42.0)
        }
    }

    #[juniper::graphql_union]
    trait TestUnion {
        fn as_mutation(&self) -> Option<&TestMutation>;
    }
}

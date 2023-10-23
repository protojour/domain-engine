#![forbid(unsafe_code)]

use std::sync::Arc;

use context::{SchemaCtx, ServiceCtx};
use gql_scalar::GqlScalar;
use ontol_runtime::{interface::DomainInterface, ontology::Ontology, PackageId};
use thiserror::Error;
use tracing::debug;

pub mod context;
pub mod gql_scalar;

mod look_ahead_utils;
mod macros;
mod registry_ctx;
mod select_analyzer;
mod templates;

pub mod juniper {
    pub use ::juniper::*;
}

pub type Schema = juniper::RootNode<
    'static,
    templates::query_type::QueryType,
    templates::mutation_type::MutationType,
    juniper::EmptySubscription<ServiceCtx>,
    GqlScalar,
>;

#[derive(Debug, Error)]
pub enum CreateSchemaError {
    #[error("GraphQL interface not found for given package")]
    GraphqlInterfaceNotFound,
}

pub fn create_graphql_schema(
    package_id: PackageId,
    ontology: Arc<Ontology>,
) -> Result<Schema, CreateSchemaError> {
    let ontol_interface_schema = ontology
        .domain_interfaces(package_id)
        .iter()
        .map(|interface| match interface {
            DomainInterface::GraphQL(schema) => schema,
        })
        .next()
        .ok_or(CreateSchemaError::GraphqlInterfaceNotFound)?;

    let schema_ctx = Arc::new(SchemaCtx {
        schema: ontol_interface_schema.clone(),
        ontology,
    });

    let juniper_schema = Schema::new_with_info(
        templates::query_type::QueryType,
        templates::mutation_type::MutationType,
        juniper::EmptySubscription::new(),
        schema_ctx.query_schema_type(),
        schema_ctx.mutation_schema_type(),
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

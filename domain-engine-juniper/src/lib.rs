use std::sync::Arc;

use domain_engine_core::DomainEngine;
use gql_scalar::GqlScalar;
use ontol_runtime::{ontology::Ontology, PackageId};
use thiserror::Error;
use tracing::debug;

pub mod gql_scalar;
mod virtual_schema;

mod look_ahead_utils;
mod macros;
mod query_analyzer;
mod templates;
mod virtual_registry;

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
pub enum SchemaBuildError {
    #[error("unknown package")]
    UnknownPackage,
}

pub fn create_graphql_schema(
    package_id: PackageId,
    ontology: Arc<Ontology>,
) -> Result<Schema, SchemaBuildError> {
    let virtual_schema = Arc::new(virtual_schema::VirtualSchema::build(package_id, ontology)?);

    let schema = Schema::new_with_info(
        templates::query_type::QueryType,
        templates::mutation_type::MutationType,
        juniper::EmptySubscription::new(),
        virtual_schema.query_type_info(),
        virtual_schema.mutation_type_info(),
        (),
    );

    debug!("Created schema \n{}", schema.as_schema_language());

    Ok(schema)
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

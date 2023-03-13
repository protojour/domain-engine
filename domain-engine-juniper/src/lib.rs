use std::sync::Arc;

use gql_scalar::GqlScalar;
use ontol_runtime::{env::Env, PackageId};
use tracing::debug;

pub mod gql_scalar;
mod virtual_schema;

mod input_value_deserializer;
mod macros;
mod query_analyzer;
mod templates;
mod type_info;
mod virtual_registry;

pub struct GqlContext;

impl juniper::Context for GqlContext {}

pub type Schema = juniper::RootNode<
    'static,
    templates::query_type::QueryType,
    templates::mutation_type::MutationType,
    juniper::EmptySubscription<GqlContext>,
    GqlScalar,
>;

#[derive(Debug)]
pub enum SchemaBuildError {
    UnknownPackage,
}

pub fn create_graphql_schema(
    package_id: PackageId,
    env: Arc<Env>,
    config: Arc<domain_engine_core::Config>,
) -> Result<Schema, SchemaBuildError> {
    let virtual_schema = Arc::new(virtual_schema::VirtualSchema::build(
        package_id, env, config,
    )?);

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
    /*
    struct TestScalar(String);

    #[juniper::graphql_scalar]
    impl<S> juniper::GraphQLScalar for TestScalar
    where
        S: juniper::ScalarValue,
    {
        fn resolve(&self) -> juniper::Value {
            juniper::Value::scalar(self.0.to_owned())
        }

        fn from_input_value(_value: &juniper::InputValue) -> Option<TestScalar> {
            panic!()
        }

        fn from_str<'a>(value: juniper::ScalarToken<'a>) -> juniper::ParseScalarResult<'a, S> {
            <String as juniper::ParseScalarValue<S>>::from_str(value)
        }
    }
    */

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

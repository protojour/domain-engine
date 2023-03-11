use std::sync::Arc;

use gql_scalar::GqlScalar;
use ontol_runtime::{env::Env, PackageId};

pub mod adapter;
pub mod gql_scalar;
mod virtual_schema;

mod input_value_deserializer;
mod macros;
mod new_templates;
mod registry_wrapper;
mod templates;
mod type_info;
mod virtual_registry;

pub struct GqlContext;

impl juniper::Context for GqlContext {}

pub type Schema = juniper::RootNode<
    'static,
    templates::query::Query,
    templates::mutation::Mutation,
    juniper::EmptySubscription<GqlContext>,
    GqlScalar,
>;

#[derive(Debug)]
pub enum SchemaBuildError {
    UnknownPackage,
}

pub fn create_graphql_schema(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<Schema, SchemaBuildError> {
    let adapter = adapter::adapt::adapt_domain(env.clone(), package_id)?;

    let _ = virtual_schema::VirtualSchema::build(env, package_id).expect("BUG");

    Ok(Schema::new_with_info(
        templates::query::Query,
        templates::mutation::Mutation,
        juniper::EmptySubscription::new(),
        templates::query::QueryTypeInfo(adapter.clone()),
        templates::mutation::MutationTypeInfo(adapter),
        (),
    ))
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

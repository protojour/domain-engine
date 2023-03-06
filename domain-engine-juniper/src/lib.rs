use std::sync::Arc;

use adapter::adapt_domain;
use gql_scalar::GqlScalar;
use ontol_runtime::{env::Env, PackageId};

pub mod adapter;
pub mod gql_scalar;

mod macros;
mod templates;
mod type_info;

pub struct GqlContext;

impl juniper::Context for GqlContext {}

pub type Schema = juniper::RootNode<
    'static,
    templates::query::Query,
    juniper::EmptyMutation<GqlContext>,
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
    let adapter = Arc::new(adapt_domain(env, package_id)?);

    Ok(Schema::new_with_info(
        templates::query::Query,
        juniper::EmptyMutation::new(),
        juniper::EmptySubscription::new(),
        templates::query::QueryTypeInfo(adapter),
        (),
        (),
    ))
}

/// Just some test code to be able to look at some macro expansions
#[cfg(test)]
mod test_derivations {
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

    #[derive(juniper::GraphQLInputObject)]
    struct TestInputObject {
        a: i32,
        b: String,
    }
}

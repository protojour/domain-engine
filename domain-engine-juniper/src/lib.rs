pub mod adapter;

mod gql_scalar;
mod macros;
mod templates;
mod type_info;

pub struct GqlContext;

impl juniper::Context for GqlContext {}

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

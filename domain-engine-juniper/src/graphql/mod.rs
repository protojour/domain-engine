pub mod adapter;

mod macros;
mod scalar;
mod templates;
mod type_info;

pub struct GqlContext;

impl juniper::Context for GqlContext {}

struct Lolle(String);

#[juniper::graphql_scalar]
impl<S> juniper::GraphQLScalar for Lolle
where
    S: juniper::ScalarValue,
{
    fn resolve(&self) -> juniper::Value {
        juniper::Value::scalar(self.0.to_owned())
    }

    fn from_input_value(_value: &juniper::InputValue) -> Option<Lolle> {
        panic!()
    }

    fn from_str<'a>(value: juniper::ScalarToken<'a>) -> juniper::ParseScalarResult<'a, S> {
        <String as juniper::ParseScalarValue<S>>::from_str(value)
    }
}

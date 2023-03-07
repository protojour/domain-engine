use crate::{
    adapter::{ScalarKind, TypeAdapter},
    gql_scalar::GqlScalar,
    type_info::GraphqlTypeName,
    GqlContext,
};

pub struct CustomScalar {
    pub json_value: serde_json::Value,
}

pub struct CustomScalarTypeInfo(pub TypeAdapter<ScalarKind>);

impl GraphqlTypeName for CustomScalarTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.type_data().type_name
    }
}

impl juniper::GraphQLValue<GqlScalar> for CustomScalar {
    type Context = GqlContext;
    type TypeInfo = CustomScalarTypeInfo;

    fn type_name<'i>(&self, info: &'i CustomScalarTypeInfo) -> Option<&'i str> {
        Some(info.graphql_type_name())
    }

    fn resolve(
        &self,
        _info: &CustomScalarTypeInfo,
        _selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        _executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        todo!("serialize")
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for CustomScalar {
    fn resolve_async<'a>(
        &'a self,
        info: &'a CustomScalarTypeInfo,
        selection_set: Option<&'a [juniper::Selection<GqlScalar>]>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        let v = juniper::GraphQLValue::resolve(&self, info, selection_set, executor);
        Box::pin(async { v })
    }
}

impl juniper::GraphQLType<GqlScalar> for CustomScalar {
    fn name(info: &CustomScalarTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        registry.build_scalar_type::<Self>(info).into_meta()
    }
}

impl juniper::marker::IsInputType<GqlScalar> for CustomScalar {}
impl juniper::marker::IsOutputType<GqlScalar> for CustomScalar {}

impl juniper::ToInputValue<GqlScalar> for CustomScalar {
    fn to_input_value(&self) -> juniper::InputValue<GqlScalar> {
        let v = juniper::Value::scalar(42);
        juniper::ToInputValue::to_input_value(&v)
    }
}

impl juniper::FromInputValue<GqlScalar> for CustomScalar {
    type Error = String;

    fn from_input_value(_v: &juniper::InputValue<GqlScalar>) -> Result<Self, Self::Error> {
        Err("BUG: This also panics inside the official juniper scalar macro".into())
    }
}

impl juniper::ParseScalarValue<GqlScalar> for CustomScalar {
    fn from_str(_value: juniper::parser::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        panic!("TODO: this won't work")
    }
}

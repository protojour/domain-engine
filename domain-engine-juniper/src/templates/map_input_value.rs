use ontol_runtime::serde::SerdeOperator;

use crate::{
    adapter::{NodeKind, TypeAdapter},
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
};

pub struct MapInputValue {
    pub input_value: juniper::InputValue<GqlScalar>,
}

pub struct MapInputValueTypeInfo(pub TypeAdapter<NodeKind>);

impl GraphqlTypeName for MapInputValueTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.type_data().input_type_name
    }
}

impl_graphql_value!(MapInputValue, TypeInfo = MapInputValueTypeInfo);

impl juniper::GraphQLType<GqlScalar> for MapInputValue {
    fn name(info: &MapInputValueTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let type_data = &info.0.data().type_data;
        let env = &info.0.domain_data.env;
        let mut reg = RegistryWrapper::new(registry, &info.0.domain_data);
        let operator = env.get_serde_operator(type_data.operator_id);

        let mut arguments = vec![];

        if let SerdeOperator::MapType(map_type) = operator {
            for (name, property) in &map_type.properties {
                arguments.push(reg.register_operator_argument(name, property.value_operator_id));
            }
        } else {
            panic!()
        }

        registry
            .build_input_object_type::<Self>(info, &arguments)
            .into_meta()
    }
}

impl juniper::ToInputValue<GqlScalar> for MapInputValue {
    fn to_input_value(&self) -> juniper::InputValue<GqlScalar> {
        let v = juniper::Value::scalar(42);
        juniper::ToInputValue::to_input_value(&v)
    }
}

impl juniper::FromInputValue<GqlScalar> for MapInputValue {
    type Error = String;

    fn from_input_value(_: &juniper::InputValue<GqlScalar>) -> Result<Self, Self::Error> {
        unimplemented!("This is not actually used");
    }
}

impl juniper::ParseScalarValue<GqlScalar> for MapInputValue {
    fn from_str(_value: juniper::parser::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        panic!("TODO: this won't work")
    }
}

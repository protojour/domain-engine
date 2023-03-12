use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{
        data::{ObjectData, ObjectKind, TypeKind},
        VirtualIndexedTypeInfo,
    },
};

pub struct IndexedInputValue {
    pub input_value: juniper::InputValue<GqlScalar>,
}

impl_graphql_value!(IndexedInputValue, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for IndexedInputValue {
    fn name(info: &VirtualIndexedTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = VirtualRegistry::new(&info.virtual_schema, registry);
        match &info.type_data().kind {
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Node(node_data),
                ..
            }) => {
                let mut arguments = vec![];
                reg.collect_operator_arguments(
                    node_data.generic_operator_id,
                    &mut arguments,
                    info.mode,
                    info.level,
                );
                registry
                    .build_input_object_type::<Self>(info, &arguments)
                    .into_meta()
            }
            TypeKind::Union(_union_data) => {
                panic!("Unions can't be part of input values")
            }
            TypeKind::CustomScalar(_) => todo!(),
            TypeKind::Object(_) => panic!("Invalid Object input data"),
        }
    }
}

impl juniper::ToInputValue<GqlScalar> for IndexedInputValue {
    fn to_input_value(&self) -> juniper::InputValue<GqlScalar> {
        let v = juniper::Value::scalar(42);
        juniper::ToInputValue::to_input_value(&v)
    }
}

impl juniper::FromInputValue<GqlScalar> for IndexedInputValue {
    type Error = String;

    fn from_input_value(_: &juniper::InputValue<GqlScalar>) -> Result<Self, Self::Error> {
        unimplemented!("This is not actually used");
    }
}

impl juniper::ParseScalarValue<GqlScalar> for IndexedInputValue {
    fn from_str(_value: juniper::parser::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        panic!("TODO: this won't work")
    }
}

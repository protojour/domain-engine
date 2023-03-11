use ontol_runtime::serde::SerdeOperator;

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

pub struct IndexedInputValueTypeInfo(pub VirtualIndexedTypeInfo);

impl GraphqlTypeName for IndexedInputValueTypeInfo {
    fn graphql_type_name(&self) -> &str {
        match &self.0.type_data().kind {
            TypeKind::Object(obj) => match &obj.kind {
                ObjectKind::Node(node) => &node.input_type_name,
                _ => panic!(),
            },
            _ => panic!(),
        }
    }
}

impl_graphql_value!(IndexedInputValue, TypeInfo = IndexedInputValueTypeInfo);

impl juniper::GraphQLType<GqlScalar> for IndexedInputValue {
    fn name(info: &IndexedInputValueTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let reg = VirtualRegistry::new(&info.0.virtual_schema, registry);
        match &info.0.type_data().kind {
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Node(node_data),
                ..
            }) => {
                let fields = vec![];
                let serde_operator = reg
                    .virtual_schema
                    .env()
                    .get_serde_operator(node_data.operator_id);

                if let SerdeOperator::MapType(_map_type) = serde_operator {
                } else {
                    panic!();
                }

                registry
                    .build_object_type::<Self>(info, &fields)
                    .into_meta()
            }
            TypeKind::Union(_union_data) => {
                todo!()
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

use tracing::{debug, warn};

use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    virtual_registry::VirtualRegistry,
    virtual_schema::{
        data::{EdgeData, ObjectData, ObjectKind, TypeKind, TypeRef},
        VirtualIndexedTypeInfo,
    },
};

pub struct IndexedInputValue;

impl_graphql_value!(IndexedInputValue);

impl juniper::GraphQLType<GqlScalar> for IndexedInputValue {
    fn name(info: &VirtualIndexedTypeInfo) -> Option<&str> {
        Some(info.typename())
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
                    node_data.operator_id,
                    &mut arguments,
                    info.typing_purpose,
                );
                registry
                    .build_input_object_type::<Self>(info, &arguments)
                    .into_meta()
            }
            TypeKind::Object(ObjectData {
                kind:
                    ObjectKind::Edge(EdgeData {
                        node_operator_id,
                        rel_edge_ref: rel_ref,
                    }),
                ..
            }) => {
                debug!("collect edge arguments");

                let mut arguments = vec![];
                reg.collect_operator_arguments(
                    *node_operator_id,
                    &mut arguments,
                    info.typing_purpose,
                );

                if let Some(rel_ref) = rel_ref {
                    arguments.push(juniper::meta::Argument::new(
                        "_edge",
                        reg.get_type::<IndexedInputValue>(
                            TypeRef::mandatory(*rel_ref),
                            info.typing_purpose,
                        ),
                    ));
                }

                // let arguments = reg.convert_fields_to_arguments(fields, info.typing_purpose);
                registry
                    .build_input_object_type::<Self>(info, &arguments)
                    .into_meta()
            }
            TypeKind::Union(_union_data) => {
                warn!("No support for unions as input values yet");
                registry
                    .build_input_object_type::<Self>(info, &[])
                    .into_meta()
            }
            TypeKind::CustomScalar(_) => registry.build_scalar_type::<Self>(info).into_meta(),
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
        debug!("No way to validate this here without TypeInfo/SerdeOperatorId");
        Ok(Self)
    }
}

impl juniper::ParseScalarValue<GqlScalar> for IndexedInputValue {
    fn from_str(_value: juniper::parser::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        panic!("TODO: this won't work")
    }
}

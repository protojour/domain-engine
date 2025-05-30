use std::str::FromStr;

use arcstr::{ArcStr, literal};
use juniper::{ParseError, ScalarToken};
use ontol_runtime::interface::graphql::{
    data::{
        ConnectionData, EdgeData, ObjectData, ObjectKind, Optionality, TypeKind, TypeModifier,
        TypeRef,
    },
    schema::TypingPurpose,
};
use tracing::{debug, trace_span};

use crate::{
    domain::{
        context::SchemaType,
        macros::impl_graphql_value,
        registry_ctx::{ArgumentFilter, RegistryCtx},
    },
    gql_scalar::GqlScalar,
};

/// GraphQL input types for things in the SchemaCtx.
/// i.e. object types and custom scalars.
///
/// Native scalars are supported by juniper directly.
pub struct InputType;

impl_graphql_value!(InputType);

impl juniper::GraphQLType<GqlScalar> for InputType {
    fn name(info: &SchemaType) -> Option<ArcStr> {
        Some(info.typename())
    }

    fn meta(
        info: &SchemaType,
        registry: &mut juniper::Registry<GqlScalar>,
    ) -> juniper::meta::MetaType<GqlScalar> {
        let _entered = trace_span!("input", name = ?info.typename()).entered();

        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        match &info.type_data().kind {
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Node(node_data),
                ..
            }) => {
                let mut arguments = vec![];

                let _ = reg
                    .collect_operator_arguments(
                        node_data.operator_addr,
                        &mut arguments,
                        info.typing_purpose,
                        ArgumentFilter::default(),
                    )
                    .unwrap();

                reg.build_input_object_meta_type(info, &arguments)
            }
            TypeKind::Object(ObjectData {
                kind:
                    ObjectKind::Edge(EdgeData {
                        node_operator_addr,
                        rel_edge_ref: rel_ref,
                        ..
                    }),
                ..
            }) => {
                let mut arguments = vec![];
                let _ = reg
                    .collect_operator_arguments(
                        *node_operator_addr,
                        &mut arguments,
                        info.typing_purpose,
                        ArgumentFilter::default(),
                    )
                    .unwrap();

                if let Some(rel_ref) = rel_ref {
                    arguments.push(juniper::meta::Argument::new(
                        literal!("_edge"),
                        reg.get_type::<InputType>(
                            TypeRef::mandatory(*rel_ref),
                            info.typing_purpose,
                        ),
                    ));
                }

                reg.build_input_object_meta_type(info, &arguments)
            }
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Connection(ConnectionData { node_type_ref }),
                ..
            }) => {
                let arguments = vec![
                    reg.get_arg::<InputType>(
                        literal!("add"),
                        TypeRef {
                            modifier: TypeModifier::Array {
                                array: Optionality::Optional,
                                element: Optionality::Mandatory,
                            },
                            unit: *node_type_ref,
                        },
                        TypingPurpose::InputOrReference,
                    ),
                    reg.get_arg::<InputType>(
                        literal!("update"),
                        TypeRef {
                            modifier: TypeModifier::Array {
                                array: Optionality::Optional,
                                element: Optionality::Mandatory,
                            },
                            unit: *node_type_ref,
                        },
                        TypingPurpose::PartialInput,
                    ),
                    reg.get_arg::<InputType>(
                        literal!("remove"),
                        TypeRef {
                            modifier: TypeModifier::Array {
                                array: Optionality::Optional,
                                element: Optionality::Mandatory,
                            },
                            unit: *node_type_ref,
                        },
                        TypingPurpose::PartialInput,
                    ),
                ];

                reg.build_input_object_meta_type(info, &arguments)
            }
            TypeKind::Union(union_data) => {
                let mut arguments = vec![];
                let _ = reg
                    .collect_operator_arguments(
                        union_data.operator_addr,
                        &mut arguments,
                        info.typing_purpose,
                        ArgumentFilter::default(),
                    )
                    .unwrap();

                reg.build_input_object_meta_type(info, &arguments)
            }
            TypeKind::CustomScalar(_) => registry.build_scalar_type::<Self>(info).into_meta(),
            TypeKind::Object(_) => panic!("Invalid Object input data"),
        }
    }
}

impl juniper::ToInputValue<GqlScalar> for InputType {
    fn to_input_value(&self) -> juniper::InputValue<GqlScalar> {
        let v = juniper::Value::scalar(42);
        juniper::ToInputValue::to_input_value(&v)
    }
}

impl juniper::FromInputValue<GqlScalar> for InputType {
    type Error = String;

    fn from_input_value(_: &juniper::InputValue<GqlScalar>) -> Result<Self, Self::Error> {
        debug!("No way to validate this here without TypeInfo/SerdeOperatorAddr");
        Ok(Self)
    }
}

impl juniper::ParseScalarValue<GqlScalar> for InputType {
    fn from_str(value: juniper::parser::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        match value {
            ScalarToken::String(str) => Ok(GqlScalar::String(str.into())),
            ScalarToken::Float(str) => <f64 as FromStr>::from_str(str)
                .map_err(|err| ParseError::UnexpectedToken(format!("{err}").into()))
                .map(GqlScalar::F64),
            ScalarToken::Int(str) => {
                <i32 as FromStr>::from_str(str)
                    .map(GqlScalar::I32)
                    .or_else(|_| {
                        <i64 as FromStr>::from_str(str)
                            .map(GqlScalar::I64)
                            .map_err(|err| ParseError::UnexpectedToken(format!("{err}").into()))
                    })
            }
        }
    }
}

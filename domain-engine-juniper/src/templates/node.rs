use ontol_runtime::serde::SerdeOperator;

use crate::{
    adapter::{data::FieldKind, EdgeAdapter, NodeKind, TypeAdapter},
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
};

use super::connection::{Connection, ConnectionTypeInfo};

pub struct Node;

#[derive(Clone)]
pub struct NodeTypeInfo(pub TypeAdapter<NodeKind>);

impl GraphqlTypeName for NodeTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.type_data().type_name
    }
}

impl_graphql_value!(Node, TypeInfo = NodeTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Node {
    fn name(info: &NodeTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &NodeTypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let env = info.0.domain_data.env.as_ref();
        let node_ref = info.0.data();

        let mut fields = vec![];

        for (field_name, field) in &node_ref.type_data.fields {
            match &field.kind {
                FieldKind::Scalar(serde_operator_id) => {
                    match env.get_serde_operator(*serde_operator_id) {
                        SerdeOperator::Unit => {
                            todo!("unit fields");
                        }
                        SerdeOperator::Int(_) => {
                            fields.push(
                                registry.field_convert::<i32, _, Self::Context>(field_name, &()),
                            );
                        }
                        SerdeOperator::Number(_) => {
                            todo!("number fields");
                        }
                        SerdeOperator::String(_)
                        | SerdeOperator::StringConstant(..)
                        | SerdeOperator::StringPattern(_)
                        | SerdeOperator::CapturingStringPattern(_) => {
                            fields.push(
                                registry.field_convert::<std::string::String, _, Self::Context>(
                                    field_name,
                                    &(),
                                ),
                            );
                        }
                        _ => {}
                    }
                }
                FieldKind::Node { .. } => {}
                FieldKind::EntityRelationship { node, .. } => {
                    fields.push(
                        registry.field_convert::<Option<Connection>, _, Self::Context>(
                            field_name,
                            &ConnectionTypeInfo(EdgeAdapter {
                                domain_data: info.0.domain_data.clone(),
                                subject: Some(node_ref.type_data.def_id),
                                node_operator_id: *node,
                            }),
                        ),
                    );
                }
            }
        }

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

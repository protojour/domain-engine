use juniper::{meta::Field, Registry};

use crate::{
    adapter::{DynamicKind, DynamicRef, TypeAdapter},
    gql_scalar::GqlScalar,
    templates::node::{Node, NodeTypeInfo},
    GqlContext,
};

pub fn domain_field<'r>(
    field_name: &str,
    adapter: TypeAdapter<DynamicKind>,
    registry: &mut Registry<'r, GqlScalar>,
) -> Field<'r, GqlScalar> {
    match adapter.data() {
        DynamicRef::Node(node_ref) => registry.field_convert::<Node, _, GqlContext>(
            field_name,
            &NodeTypeInfo(TypeAdapter {
                domain_data: adapter.domain_data.clone(),
                operator_id: node_ref.type_data.operator_id,
                _kind: std::marker::PhantomData,
            }),
        ),
        DynamicRef::Entity(entity_ref) => registry.field_convert::<Node, _, GqlContext>(
            field_name,
            &NodeTypeInfo(TypeAdapter {
                domain_data: adapter.domain_data.clone(),
                operator_id: entity_ref.type_data.operator_id,
                _kind: std::marker::PhantomData,
            }),
        ),
        DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
    }
}

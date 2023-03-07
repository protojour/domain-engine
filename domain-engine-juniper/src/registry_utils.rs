use juniper::{meta::Field, Registry};

use crate::{
    adapter::{DynamicKind, DynamicRef, TypeAdapter, UnionAdapter},
    gql_scalar::GqlScalar,
    templates::{
        node::{Node, NodeTypeInfo},
        union::{Union, UnionTypeInfo},
    },
    GqlContext,
};

pub fn domain_field<'r>(
    field_name: &str,
    adapter: TypeAdapter<DynamicKind>,
    registry: &mut Registry<'r, GqlScalar>,
) -> Field<'r, GqlScalar> {
    let domain_data = adapter.domain_data.clone();
    match adapter.data() {
        DynamicRef::Node(node_ref) => registry.field_convert::<Node, _, GqlContext>(
            field_name,
            &NodeTypeInfo(TypeAdapter {
                domain_data,
                operator_id: node_ref.type_data.operator_id,
                _kind: std::marker::PhantomData,
            }),
        ),
        DynamicRef::Entity(entity_ref) => registry.field_convert::<Node, _, GqlContext>(
            field_name,
            &NodeTypeInfo(TypeAdapter {
                domain_data,
                operator_id: entity_ref.type_data.operator_id,
                _kind: std::marker::PhantomData,
            }),
        ),
        DynamicRef::Union(union_ref) => registry.field_convert::<Union, _, GqlContext>(
            field_name,
            &UnionTypeInfo(UnionAdapter {
                domain_data,
                operator_id: union_ref.union_data.operator_id,
            }),
        ),
        DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
    }
}

pub fn get_domain_type<'r>(
    adapter: TypeAdapter<DynamicKind>,
    registry: &mut Registry<'r, GqlScalar>,
) -> juniper::Type<'r> {
    let domain_data = adapter.domain_data.clone();
    match adapter.data() {
        DynamicRef::Node(node_ref) => registry.get_type::<Node>(&NodeTypeInfo(TypeAdapter {
            domain_data,
            operator_id: node_ref.type_data.operator_id,
            _kind: std::marker::PhantomData,
        })),
        DynamicRef::Entity(entity_ref) => registry.get_type::<Node>(&NodeTypeInfo(TypeAdapter {
            domain_data,
            operator_id: entity_ref.type_data.operator_id,
            _kind: std::marker::PhantomData,
        })),
        DynamicRef::Union(union_ref) => registry.get_type::<Union>(&UnionTypeInfo(UnionAdapter {
            domain_data,
            operator_id: union_ref.union_data.operator_id,
        })),
        DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
    }
}

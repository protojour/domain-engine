use crate::{
    adapter::{
        data::{Field, FieldCardinality, FieldKind},
        EdgeAdapter,
    },
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
};

pub struct Edge;

#[derive(Clone)]
pub struct EdgeTypeInfo(pub EdgeAdapter);

impl GraphqlTypeName for EdgeTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.data().edge_type_name
    }
}

impl_graphql_value!(Edge, TypeInfo = EdgeTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Edge {
    fn name(info: &EdgeTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &EdgeTypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = RegistryWrapper::new(registry, &info.0.domain_data);
        let fields = [
            // TODO: edge rel params, cursor
            reg.field_convert::<std::string::String, _, Self::Context>("cursor", &()),
            reg.register_domain_field(
                "node",
                &Field {
                    cardinality: FieldCardinality::UnitMandatory,
                    kind: FieldKind::Node(info.0.node_operator_id),
                },
            ),
        ];

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

use crate::{
    adapter::{NodeKind, TypeAdapter},
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
};

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
        let mut reg = RegistryWrapper::new(registry, &info.0.domain_data);
        let fields: Vec<_> = info
            .0
            .data()
            .type_data
            .fields
            .iter()
            .map(|(field_name, field)| reg.register_domain_field(field_name, field))
            .collect();

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

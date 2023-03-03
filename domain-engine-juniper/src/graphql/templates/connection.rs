use crate::graphql::{
    adapter::{EntityKind, TypeAdapter},
    macros::impl_graphql_value,
    scalar::GqlScalar,
    type_info::GraphqlTypeName,
};

pub struct Connection {}

#[derive(Clone)]
pub struct ConnectionTypeInfo(pub TypeAdapter<EntityKind>);

impl GraphqlTypeName for ConnectionTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.get_kind().entity_data.connection_type_name
    }
}

impl_graphql_value!(Connection, TypeInfo = ConnectionTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Connection {
    fn name(type_info: &Self::TypeInfo) -> Option<&str> {
        Some(type_info.graphql_type_name())
    }

    fn meta<'r>(
        type_info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        panic!()
    }
}

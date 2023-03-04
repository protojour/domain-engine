use crate::{
    adapter::EdgeAdapter, gql_scalar::GqlScalar, macros::impl_graphql_value,
    type_info::GraphqlTypeName,
};

use super::edge::{Edge, EdgeTypeInfo};

pub struct Connection {}

#[derive(Clone)]
pub struct ConnectionTypeInfo(pub EdgeAdapter);

impl GraphqlTypeName for ConnectionTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.data().connection_type_name
    }
}

impl_graphql_value!(Connection, TypeInfo = ConnectionTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Connection {
    fn name(info: &ConnectionTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &ConnectionTypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let fields = [
            // TODO: page info
            registry.field_convert::<Vec<Edge>, _, Self::Context>(
                "edges",
                &EdgeTypeInfo(info.0.clone()),
            ),
        ];

        registry
            .build_object_type::<Connection>(info, &fields)
            .into_meta()
    }
}

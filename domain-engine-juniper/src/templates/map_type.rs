use crate::{
    adapter::{TypeAdapter, TypeKind},
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
};

pub struct MapType;

#[derive(Clone)]
pub struct MapTypeInfo(pub TypeAdapter<TypeKind>);

impl GraphqlTypeName for MapTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.get_type_data().type_name
    }
}

impl_graphql_value!(MapType, TypeInfo = MapTypeInfo);

impl juniper::GraphQLType<GqlScalar> for MapType {
    fn name(type_info: &Self::TypeInfo) -> Option<&str> {
        Some(type_info.graphql_type_name())
    }

    fn meta<'r>(
        _type_info: &Self::TypeInfo,
        _registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        panic!()
    }
}

use crate::{
    adapter::{DynamicTypeKind, TypeAdapter},
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
};

pub struct MapType;

#[derive(Clone)]
pub struct MapTypeInfo(pub TypeAdapter<DynamicTypeKind>);

impl GraphqlTypeName for MapTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.type_data().type_name
    }
}

impl_graphql_value!(MapType, TypeInfo = MapTypeInfo);

impl juniper::GraphQLType<GqlScalar> for MapType {
    fn name(info: &MapTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &MapTypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let _serde_operator = info.0.serde_operator();

        let fields = [];

        registry
            .build_object_type::<MapType>(info, &fields)
            .into_meta()
    }
}

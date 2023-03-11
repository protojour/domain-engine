use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{data::TypeKind, VirtualIndexedTypeInfo},
};

pub struct IndexedType;

impl_graphql_value!(IndexedType, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for IndexedType {
    fn name(info: &Self::TypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = VirtualRegistry::new(&info.virtual_schema, registry);
        match &info.type_data().kind {
            TypeKind::Object(_) => {
                let fields = reg.get_fields(info.type_index);

                registry
                    .build_object_type::<Self>(info, &fields)
                    .into_meta()
            }
            TypeKind::Union(union_data) => {
                let types: Vec<_> = union_data
                    .variants
                    .iter()
                    .map(|type_index| {
                        reg.registry
                            .get_type::<IndexedType>(&VirtualIndexedTypeInfo {
                                virtual_schema: info.virtual_schema.clone(),
                                type_index: *type_index,
                            })
                    })
                    .collect();

                registry.build_union_type::<Self>(info, &types).into_meta()
            }
            TypeKind::CustomScalar(_) => todo!(),
        }
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for IndexedType {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        _info: &'a Self::TypeInfo,
        _field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move { Ok(juniper::Value::Null) })
    }
}

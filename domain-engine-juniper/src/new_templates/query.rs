use juniper::graphql_value;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar, macros::impl_graphql_value, type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry, virtual_schema::VirtualIndexedTypeInfo,
};

pub struct Query;

impl_graphql_value!(Query, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Query {
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
        let fields = reg.get_fields(info.type_index);

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for Query {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a Self::TypeInfo,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let _query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            debug!("Executing query {field_name}");

            Ok(graphql_value!({ "edges": None }))
        })
    }
}

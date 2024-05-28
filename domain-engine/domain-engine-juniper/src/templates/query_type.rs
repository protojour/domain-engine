use tracing::{debug_span, Instrument};

use crate::{
    context::SchemaType, gql_scalar::GqlScalar, macros::impl_graphql_value,
    registry_ctx::RegistryCtx,
};

pub struct QueryType;

impl_graphql_value!(QueryType);

impl juniper::GraphQLType<GqlScalar> for QueryType {
    fn name(info: &SchemaType) -> Option<&str> {
        Some(info.typename())
    }

    fn meta<'r>(
        info: &SchemaType,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        // Hack to avoid some juniper errors.
        // The Int type needs to exist unconditionally, even if our generated schema does not request it.
        registry.get_type::<i32>(&());

        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        let fields = reg.get_fields(info.type_addr);

        let mut builder = registry.build_object_type::<Self>(info, &fields);

        if let Some(docs) = info.docs_str() {
            builder = builder.description(docs);
        }

        builder.into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for QueryType {
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a SchemaType,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(
            crate::query(info, field_name, executor)
                .instrument(debug_span!("query", name = %field_name)),
        )
    }
}

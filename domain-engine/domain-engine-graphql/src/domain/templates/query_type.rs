use arcstr::ArcStr;
use tracing::{Instrument, debug_span};

use crate::{
    domain::{context::SchemaType, macros::impl_graphql_value, registry_ctx::RegistryCtx},
    gql_scalar::GqlScalar,
};

pub struct QueryType;

impl_graphql_value!(QueryType);

impl juniper::GraphQLType<GqlScalar> for QueryType {
    fn name(info: &SchemaType) -> Option<ArcStr> {
        Some(info.typename())
    }

    fn meta(
        info: &SchemaType,
        registry: &mut juniper::Registry<GqlScalar>,
    ) -> juniper::meta::MetaType<GqlScalar> {
        // Hack to avoid some juniper errors.
        // The Int type needs to exist unconditionally, even if our generated schema does not request it.
        registry.get_type::<i32>(&());

        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        let fields = reg.get_fields(info.type_addr);

        let mut builder = registry.build_object_type::<Self>(info, &fields);

        if let Some(docs) = info.docs() {
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
            crate::domain::query(info, field_name, executor)
                .instrument(debug_span!("query", name = %field_name)),
        )
    }
}

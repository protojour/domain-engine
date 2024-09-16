use ontol_runtime::interface::graphql::data::TypeKind;
use tracing::{debug_span, Instrument};

use crate::{
    domain::{context::SchemaType, macros::impl_graphql_value, registry_ctx::RegistryCtx},
    gql_scalar::GqlScalar,
};

pub struct MutationType;

impl_graphql_value!(MutationType);

impl juniper::GraphQLType<GqlScalar> for MutationType {
    fn name(info: &SchemaType) -> Option<&str> {
        let is_empty = match &info.type_data().kind {
            TypeKind::Object(object_data) => object_data.fields.is_empty(),
            _ => true,
        };

        if is_empty {
            // This is a magic string that juniper recognizes in order
            // to make a schema without a mutation type
            Some("_EmptyMutation")
        } else {
            Some(info.typename())
        }
    }

    fn meta<'r>(
        info: &SchemaType,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        let fields = reg.get_fields(info.type_addr);

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for MutationType {
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a SchemaType,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(
            crate::domain::mutation(info, field_name, executor)
                .instrument(debug_span!("mutation", name = %field_name)),
        )
    }
}

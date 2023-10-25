use domain_engine_core::DomainError;
use ontol_runtime::{
    interface::graphql::{
        data::{FieldData, FieldKind, Optionality, TypeModifier, TypeRef},
        schema::TypingPurpose,
    },
    value::ValueDebug,
};
use tracing::{debug_span, trace, Instrument};

use crate::{
    context::SchemaType,
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_ctx::RegistryCtx,
    select_analyzer::{AnalyzedQuery, SelectAnalyzer},
    templates::{attribute_type::AttributeType, resolve_schema_type_field},
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
        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        let fields = reg.get_fields(info.type_addr);

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for QueryType {
    /// TODO: Might implement resolve_async instead, so we can have just one engine_api call?
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a SchemaType,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let debug_span = debug_span!("query", "name" = field_name);

            let schema_ctx = &info.schema_ctx;
            let query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            let output = {
                let AnalyzedQuery {
                    map_key,
                    input,
                    mut selects,
                } = debug_span.in_scope(|| {
                    SelectAnalyzer::new(schema_ctx, executor.context())
                        .analyze_look_ahead(&executor.look_ahead(), query_field)
                })?;

                executor
                    .context()
                    .domain_engine
                    .exec_map(map_key, input, &mut selects)
                    .instrument(debug_span.clone())
                    .await?
            };

            debug_span.in_scope(|| {
                trace!("query result: {}", ValueDebug(&output));

                if output.is_unit()
                    && matches!(
                        query_field,
                        FieldData {
                            kind: FieldKind::MapFind { .. },
                            field_type: TypeRef {
                                modifier: TypeModifier::Unit(Optionality::Mandatory),
                                ..
                            }
                        }
                    )
                {
                    return Err(DomainError::EntityNotFound.into());
                }

                resolve_schema_type_field(
                    AttributeType {
                        attr: &output.into(),
                    },
                    schema_ctx
                        .find_schema_type_by_unit(
                            query_field.field_type.unit,
                            TypingPurpose::Selection,
                        )
                        .unwrap(),
                    executor,
                )
            })
        })
    }
}

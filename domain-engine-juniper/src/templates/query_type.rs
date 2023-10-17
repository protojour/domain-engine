use ontol_runtime::{
    interface::graphql::schema::TypingPurpose,
    value::{Attribute, Data, Value, ValueDebug},
    DefId,
};
use tracing::debug;

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
        let fields = reg.get_fields(info.type_index);

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
            let schema_ctx = &info.schema_ctx;
            let query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            let analyzed_query = SelectAnalyzer::new(schema_ctx, executor.context())
                .analyze_look_ahead(&executor.look_ahead(), query_field)?;

            debug!("Executing query `{field_name}`");

            let attribute = match analyzed_query {
                AnalyzedQuery::NamedMap {
                    key,
                    input,
                    queries,
                } => {
                    executor
                        .context()
                        .domain_engine
                        .call_map(key, input, queries)
                        .await?
                }
                AnalyzedQuery::ClassicConnection(entity_query) => {
                    let entity_attributes = executor
                        .context()
                        .domain_engine
                        .query_entities(entity_query)
                        .await?;

                    Attribute {
                        value: Value::new(Data::Sequence(entity_attributes), DefId::unit()),
                        rel_params: Value::unit(),
                    }
                }
            };

            debug!("query result: {}", ValueDebug(&attribute.value));

            resolve_schema_type_field(
                AttributeType { attr: &attribute },
                schema_ctx
                    .find_schema_type_by_unit(query_field.field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            )
        })
    }
}

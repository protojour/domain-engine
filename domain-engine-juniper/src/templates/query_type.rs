use ontol_runtime::{
    graphql::TypingPurpose,
    value::{Attribute, Data, Value, ValueDebug},
    DefId,
};
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    query_analyzer::QueryAnalyzer,
    registry_ctx::RegistryCtx,
    templates::{attribute_type::AttributeType, resolve_indexed_schema_field},
};

pub struct QueryType;

impl_graphql_value!(QueryType);

impl juniper::GraphQLType<GqlScalar> for QueryType {
    fn name(info: &Self::TypeInfo) -> Option<&str> {
        Some(info.typename())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
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
        info: &'a Self::TypeInfo,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let schema_ctx = &info.schema_ctx;
            let query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            let entity_query = QueryAnalyzer::new(schema_ctx, executor.context())
                .analyze_entity_query(&executor.look_ahead(), query_field);

            debug!("Executing query {field_name}: {entity_query:#?}");

            let entity_attributes = executor
                .context()
                .domain_engine
                .query_entities(entity_query)
                .await?;

            let attribute = Attribute {
                value: Value::new(Data::Sequence(entity_attributes), DefId::unit()),
                rel_params: Value::unit(),
            };

            debug!("query result: {}", ValueDebug(&attribute.value));

            resolve_indexed_schema_field(
                AttributeType { attr: &attribute },
                schema_ctx
                    .indexed_type_info_by_unit(
                        query_field.field_type.unit,
                        TypingPurpose::Selection,
                    )
                    .unwrap(),
                executor,
            )
        })
    }
}

use ontol_runtime::{
    interface::graphql::{data::TypeModifier, schema::TypingPurpose},
    value::{Attribute, Data, Value, ValueDebug},
    DefId,
};
use tracing::{debug, debug_span, Instrument};

use crate::{
    context::SchemaType,
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_ctx::RegistryCtx,
    select_analyzer::{AnalyzedQuery, SelectAnalyzer},
    templates::{
        attribute_type::AttributeType, resolve_schema_type_field, sequence_type::SequenceType,
    },
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

            let analyzed_query = debug_span.in_scope(|| {
                SelectAnalyzer::new(schema_ctx, executor.context())
                    .analyze_look_ahead(&executor.look_ahead(), query_field)
            })?;

            let schema_type = schema_ctx
                .find_schema_type_by_unit(query_field.field_type.unit, TypingPurpose::Selection)
                .unwrap();

            match analyzed_query {
                AnalyzedQuery::NamedMap {
                    key,
                    input,
                    queries,
                    field_type,
                } => {
                    let attribute = executor
                        .context()
                        .domain_engine
                        .exec_map(key, input, queries)
                        .instrument(debug_span.clone())
                        .await?;

                    debug_span.in_scope(|| {
                        debug!("query result: {}", ValueDebug(&attribute.value));

                        if matches!(field_type.modifier, TypeModifier::Array(..)) {
                            let Data::Sequence(seq) = &attribute.value.data else {
                                panic!("Not a sequence")
                            };
                            resolve_schema_type_field(SequenceType { seq }, schema_type, executor)
                        } else {
                            resolve_schema_type_field(
                                AttributeType { attr: &attribute },
                                schema_type,
                                executor,
                            )
                        }
                    })
                }
                AnalyzedQuery::ClassicConnection(entity_query) => {
                    let entity_sequence = executor
                        .context()
                        .domain_engine
                        .query_entities(entity_query)
                        .instrument(debug_span.clone())
                        .await?;

                    let attribute = Attribute {
                        value: Value::new(Data::Sequence(entity_sequence), DefId::unit()),
                        rel_params: Value::unit(),
                    };

                    debug_span.in_scope(|| {
                        debug!("query result: {}", ValueDebug(&attribute.value));

                        resolve_schema_type_field(
                            AttributeType { attr: &attribute },
                            schema_type,
                            executor,
                        )
                    })
                }
            }
        })
    }
}

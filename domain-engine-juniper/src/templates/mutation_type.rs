use juniper::LookAheadMethods;
use ontol_runtime::interface::graphql::data::FieldKind;
use ontol_runtime::interface::graphql::schema::TypingPurpose;
use ontol_runtime::select::{Select, StructOrUnionSelect};
use ontol_runtime::value::ValueDebug;
use tracing::trace;

use crate::context::SchemaType;
use crate::{
    gql_scalar::GqlScalar,
    look_ahead_utils::ArgsWrapper,
    macros::impl_graphql_value,
    registry_ctx::RegistryCtx,
    select_analyzer::SelectAnalyzer,
    templates::{attribute_type::AttributeType, resolve_schema_type_field},
};

pub struct MutationType;

impl_graphql_value!(MutationType);

impl juniper::GraphQLType<GqlScalar> for MutationType {
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

impl juniper::GraphQLValueAsync<GqlScalar> for MutationType {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a SchemaType,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let schema_ctx = &info.schema_ctx;
            let look_ahead = executor.look_ahead();
            let args_wrapper = ArgsWrapper::new(look_ahead.arguments());
            let query_analyzer = SelectAnalyzer::new(schema_ctx, executor.context());

            let field_data = info.type_data().fields().unwrap().get(field_name).unwrap();

            match &field_data.kind {
                FieldKind::CreateMutation { input } => {
                    let input_attribute = args_wrapper
                        .deserialize_domain_field_arg_as_attribute(input, info.ontology())?;
                    let struct_query =
                        query_analyzer.analyze_struct_select(&look_ahead, field_data);
                    let query = match struct_query {
                        StructOrUnionSelect::Struct(struct_query) => Select::Struct(struct_query),
                        StructOrUnionSelect::Union(def_id, structs) => {
                            Select::StructUnion(def_id, structs)
                        }
                    };

                    trace!(
                        "CREATE {} -> {query:#?}",
                        ValueDebug(&input_attribute.value)
                    );

                    let value = executor
                        .context()
                        .domain_engine
                        .store_new_entity(input_attribute.value, query)
                        .await?;

                    resolve_schema_type_field(
                        AttributeType {
                            attr: &value.into(),
                        },
                        schema_ctx
                            .find_schema_type_by_unit(
                                field_data.field_type.unit,
                                TypingPurpose::Selection,
                            )
                            .unwrap(),
                        executor,
                    )
                }
                FieldKind::UpdateMutation { id, input } => {
                    let id_attribute = args_wrapper
                        .deserialize_domain_field_arg_as_attribute(id, info.ontology())?;
                    let input_attribute = args_wrapper
                        .deserialize_domain_field_arg_as_attribute(input, info.ontology())?;

                    let query = query_analyzer.analyze_struct_select(&look_ahead, field_data);

                    trace!(
                        "UPDATE {} -> {} -> {query:#?}",
                        ValueDebug(&id_attribute.value),
                        ValueDebug(&input_attribute.value)
                    );
                    Ok(juniper::Value::Null)
                }
                FieldKind::DeleteMutation { id } => {
                    let id_value = args_wrapper
                        .deserialize_domain_field_arg_as_attribute(id, info.ontology())?;

                    trace!("DELETE {id_value:?}");

                    Ok(juniper::Value::Null)
                }
                _ => panic!(),
            }
        })
    }
}

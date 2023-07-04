use juniper::LookAheadMethods;
use ontol_runtime::query::{Query, StructOrUnionQuery};
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    look_ahead_utils::ArgsWrapper,
    macros::impl_graphql_value,
    query_analyzer::QueryAnalyzer,
    templates::{attribute_type::AttributeType, resolve_virtual_schema_field},
    virtual_registry::VirtualRegistry,
    virtual_schema::{data::FieldKind, TypingPurpose},
};

pub struct MutationType;

impl_graphql_value!(MutationType);

impl juniper::GraphQLType<GqlScalar> for MutationType {
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
        let mut reg = VirtualRegistry::new(&info.virtual_schema, registry);
        let fields = reg.get_fields(info.type_index);

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for MutationType {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a Self::TypeInfo,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let virtual_schema = &info.virtual_schema;
            let look_ahead = executor.look_ahead();
            let args_wrapper = ArgsWrapper::new(look_ahead.arguments());
            let query_analyzer = QueryAnalyzer::new(virtual_schema, executor.context());

            let field_data = info.type_data().fields().unwrap().get(field_name).unwrap();

            match &field_data.kind {
                FieldKind::CreateMutation { input } => {
                    let input_attribute = args_wrapper.deserialize_attribute(input, info.env())?;
                    let struct_query = query_analyzer.analyze_struct_query(&look_ahead, field_data);
                    let query = match struct_query {
                        StructOrUnionQuery::Struct(struct_query) => Query::Struct(struct_query),
                        StructOrUnionQuery::Union(def_id, structs) => {
                            Query::StructUnion(def_id, structs)
                        }
                    };

                    debug!("CREATE {input_attribute:#?} -> {query:#?}");

                    let value = executor
                        .context()
                        .domain_engine
                        .store_new_entity(input_attribute.value, query)
                        .await?;

                    resolve_virtual_schema_field(
                        AttributeType {
                            attr: &value.into(),
                        },
                        virtual_schema
                            .indexed_type_info_by_unit(
                                field_data.field_type.unit,
                                TypingPurpose::Selection,
                            )
                            .unwrap(),
                        executor,
                    )
                }
                FieldKind::UpdateMutation { id, input } => {
                    let id_attribute = args_wrapper.deserialize_attribute(id, info.env())?;
                    let input_attribute = args_wrapper.deserialize_attribute(input, info.env())?;

                    let query = query_analyzer.analyze_struct_query(&look_ahead, field_data);

                    debug!("UPDATE {id_attribute:?} -> {input_attribute:#?} -> {query:#?}");
                    Ok(juniper::Value::Null)
                }
                FieldKind::DeleteMutation { id } => {
                    let id_value = args_wrapper.deserialize_attribute(id, info.env())?;

                    debug!("DELETE {id_value:?}");

                    Ok(juniper::Value::Null)
                }
                _ => panic!(),
            }
        })
    }
}

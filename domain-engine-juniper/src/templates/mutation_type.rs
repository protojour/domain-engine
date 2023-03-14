use juniper::LookAheadMethods;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    look_ahead_utils::ArgsWrapper,
    macros::impl_graphql_value,
    query_analyzer,
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{data::FieldKind, VirtualIndexedTypeInfo},
};

pub struct MutationType;

impl_graphql_value!(MutationType, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for MutationType {
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
            let look_ahead = executor.look_ahead();
            let args_wrapper = ArgsWrapper::new(look_ahead.arguments());
            let field_data = info.type_data().fields().unwrap().get(field_name).unwrap();

            match &field_data.kind {
                FieldKind::CreateMutation { input } => {
                    let input_value = args_wrapper.deserialize_attribute(input, info.env())?;

                    let query = query_analyzer::analyze_map_query(
                        &look_ahead,
                        field_data,
                        &info.virtual_schema,
                    );

                    debug!("CREATE {input_value:#?} -> {query:#?}");
                }
                FieldKind::UpdateMutation { id, input } => {
                    let id_value = args_wrapper.deserialize_attribute(id, info.env())?;
                    let input_value = args_wrapper.deserialize_attribute(input, info.env())?;

                    let query = query_analyzer::analyze_map_query(
                        &look_ahead,
                        field_data,
                        &info.virtual_schema,
                    );

                    debug!("UPDATE {id_value:?} -> {input_value:#?} -> {query:#?}");
                }
                FieldKind::DeleteMutation { id } => {
                    let id_value = args_wrapper.deserialize_attribute(id, info.env())?;

                    debug!("DELETE {id_value:?}");
                }
                _ => panic!(),
            }

            Ok(juniper::Value::Null)
        })
    }
}

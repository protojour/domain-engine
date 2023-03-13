use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    input_value_deserializer::deserialize_argument,
    macros::impl_graphql_value,
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
        arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let mutation_field = info.type_data().fields().unwrap().get(field_name).unwrap();
            match &mutation_field.kind {
                FieldKind::CreateMutation { input } => {
                    let input_value = deserialize_argument(input, arguments, info.env())?;

                    debug!("CREATE {input_value:?}");
                }
                FieldKind::UpdateMutation { id, input } => {
                    let id_value = deserialize_argument(id, arguments, info.env())?;
                    let input_value = deserialize_argument(input, arguments, info.env())?;

                    debug!("UPDATE {id_value:?}: {input_value:?}");
                }
                FieldKind::DeleteMutation { id } => {
                    let id_value = deserialize_argument(id, arguments, info.env())?;

                    debug!("DELETE {id_value:?}");
                }
                _ => panic!(),
            }

            Ok(juniper::Value::Null)
        })
    }
}

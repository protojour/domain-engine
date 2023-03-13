use juniper::graphql_value;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    selection_analyzer::analyze_selection,
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{data::UnitTypeRef, VirtualIndexedTypeInfo},
};

pub struct Query;

impl_graphql_value!(Query, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Query {
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

impl juniper::GraphQLValueAsync<GqlScalar> for Query {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a Self::TypeInfo,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            let connection_type_data = match query_field.field_type.unit {
                UnitTypeRef::Indexed(type_index) => info.virtual_schema.type_data(type_index),
                _ => panic!("Invalid connection"),
            };

            let naive_selection = analyze_selection(
                &executor.look_ahead(),
                connection_type_data,
                &info.virtual_schema,
            );

            // info.virtual_schema.indexed_type_info(type_index, typing_purpose);

            // query_field.field_type

            debug!("Executing query {field_name} selection: {naive_selection:?}");

            Ok(graphql_value!({ "edges": None }))
        })
    }
}

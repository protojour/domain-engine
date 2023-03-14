use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    query_analyzer,
    resolve::resolve_value,
    templates::indexed_type::IndexedType,
    type_info::GraphqlTypeName,
    value_serializer::ValueSerializer,
    virtual_registry::VirtualRegistry,
    virtual_schema::{TypingPurpose, VirtualIndexedTypeInfo},
};

pub struct QueryType;

impl_graphql_value!(QueryType, TypeInfo = VirtualIndexedTypeInfo);

impl juniper::GraphQLType<GqlScalar> for QueryType {
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

impl juniper::GraphQLValueAsync<GqlScalar> for QueryType {
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
            let query_field = info.type_data().fields().unwrap().get(field_name).unwrap();

            let entity_query = query_analyzer::analyze_entity_query(
                &executor.look_ahead(),
                query_field,
                virtual_schema,
            );

            debug!("Executing query {field_name}: {entity_query:#?}");

            let entities = virtual_schema
                .domain_api()
                .query_entities(virtual_schema.package_id(), entity_query)
                .await?;

            let value_serializer = ValueSerializer::new(virtual_schema);
            let operator_id = value_serializer
                .find_operator_id(query_field)
                .expect("found no operator");
            let serialized_value = value_serializer.serialize_values(entities, operator_id);

            match virtual_schema.lookup_type_index(query_field.field_type.unit) {
                Ok(type_index) => resolve_value(
                    IndexedType {
                        value: &serialized_value,
                    },
                    virtual_schema.indexed_type_info(type_index, TypingPurpose::Selection),
                    executor,
                ),
                Err(_) => panic!(),
            }
        })
    }
}

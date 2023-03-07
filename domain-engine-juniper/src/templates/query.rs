use crate::{
    adapter::DomainAdapter, gql_scalar::GqlScalar, macros::impl_graphql_value,
    type_info::GraphqlTypeName, GqlContext,
};

use super::connection::{Connection, ConnectionTypeInfo};

pub struct Query;

#[derive(Clone)]
pub struct QueryTypeInfo(pub DomainAdapter);

impl GraphqlTypeName for QueryTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.domain_data.query_type_name
    }
}

impl_graphql_value!(Query, TypeInfo = QueryTypeInfo);

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
        let fields: Vec<_> = info
            .0
            .domain_data
            .queries
            .iter()
            .map(|(name, operator_id)| {
                let entity_ref = info.0.entity_ref(*operator_id);

                registry.field_convert::<Option<Connection>, _, GqlContext>(
                    name,
                    &ConnectionTypeInfo(info.0.root_edge_adapter(&entity_ref)),
                )
                // .argument(registry.arg::<Option<i32>>("skip", &()))
                // .argument(registry.arg::<Option<i32>>("limit", &()))
                // .argument(registry.arg::<Option<String>>("sort", &()))
                // .argument(registry.arg::<Option<Vec<String>>>("filter", &()))
                // .argument(registry.arg::<Option<String>>("search", &()))
            })
            .collect();

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
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let _operator_id = info
                .0
                .domain_data
                .queries
                .get(field_name)
                .expect("BUG: Query not found");

            Ok(juniper::Value::Null)
        })
    }
}

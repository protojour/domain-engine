use std::sync::Arc;

use crate::{
    adapter::DomainAdapter, gql_scalar::GqlScalar, macros::impl_graphql_value,
    type_info::GraphqlTypeName, GqlContext,
};

use super::connection::{Connection, ConnectionTypeInfo};

pub struct Query;

#[derive(Clone)]
pub struct QueryTypeInfo(pub Arc<DomainAdapter>);

impl GraphqlTypeName for QueryTypeInfo {
    fn graphql_type_name(&self) -> &str {
        "Query"
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
            .iter_entities()
            .map(|entity_ref| {
                registry.field_convert::<Option<Connection>, _, GqlContext>(
                    &entity_ref.entity_data.query_field_name,
                    &ConnectionTypeInfo(info.0.root_edge_adapter(&entity_ref)),
                )
                // .argument(registry.arg::<Option<i32>>("skip", &()))
                // .argument(registry.arg::<Option<i32>>("limit", &()))
                // .argument(registry.arg::<Option<String>>("sort", &()))
                // .argument(registry.arg::<Option<Vec<String>>>("filter", &()))
                // .argument(registry.arg::<Option<String>>("search", &()))
            })
            .collect();

        let meta = registry.build_object_type::<Query>(info, &fields);
        meta.into_meta()
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
            let _entity_ref = info
                .0
                .iter_entities()
                .find(|entity_ref| entity_ref.entity_data.query_field_name == field_name)
                .expect("not found");

            Ok(juniper::Value::Null)
        })
    }
}

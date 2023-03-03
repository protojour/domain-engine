use std::sync::Arc;

use crate::graphql::{
    adapter::{DomainAdapter, EntityRef},
    macros::impl_graphql_value,
    scalar::GqlScalar,
    type_info::GraphqlTypeName,
    GqlContext,
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
    fn name(type_info: &Self::TypeInfo) -> Option<&str> {
        Some(type_info.graphql_type_name())
    }

    fn meta<'r>(
        type_info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let fields: Vec<_> = type_info
            .0
            .iter_entities()
            .map(|entity_ref| query_field(&type_info.0, entity_ref, registry))
            .collect();

        let meta = registry.build_object_type::<Query>(type_info, &fields);
        meta.into_meta()
    }
}

pub fn query_field<'r>(
    domain_adapter: &Arc<DomainAdapter>,
    entity_ref: EntityRef,
    registry: &mut juniper::Registry<'r, GqlScalar>,
) -> juniper::meta::Field<'r, GqlScalar> {
    registry.field_convert::<Option<Connection>, _, GqlContext>(
        &entity_ref.type_data.type_name,
        &ConnectionTypeInfo(domain_adapter.entity_adapter(&entity_ref)),
    )
    // .argument(registry.arg::<Option<i32>>("skip", &()))
    // .argument(registry.arg::<Option<i32>>("limit", &()))
    // .argument(registry.arg::<Option<String>>("sort", &()))
    // .argument(registry.arg::<Option<Vec<String>>>("filter", &()))
    // .argument(registry.arg::<Option<String>>("search", &()))
}

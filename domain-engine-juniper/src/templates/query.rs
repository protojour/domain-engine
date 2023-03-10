use crate::{
    adapter::{
        data::{Field, FieldCardinality, FieldKind},
        DomainAdapter,
    },
    gql_scalar::GqlScalar,
    macros::impl_graphql_value,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
};

pub struct Query;

#[derive(Clone)]
pub struct QueryTypeInfo(pub DomainAdapter);

impl GraphqlTypeName for QueryTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.query_type_name
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
        let mut reg = RegistryWrapper::new(registry, &info.0);

        let fields: Vec<_> = info
            .0
            .queries
            .iter()
            .map(|(name, def_id)| {
                reg.register_domain_field(
                    name,
                    &Field {
                        cardinality: FieldCardinality::ManyMandatory,
                        kind: FieldKind::EntityRelationship {
                            subject_id: None,
                            node_id: *def_id,
                            rel_id: None,
                        },
                    },
                )
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
            let _def_id = info
                .0
                .queries
                .get(field_name)
                .expect("BUG: Query not found");

            Ok(juniper::Value::Null)
        })
    }
}

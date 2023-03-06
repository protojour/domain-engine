use std::sync::Arc;

use crate::{
    adapter::DomainAdapter, gql_scalar::GqlScalar, macros::impl_graphql_value,
    type_info::GraphqlTypeName, GqlContext,
};

use super::node::{Node, NodeTypeInfo};

pub struct Mutation;

#[derive(Clone)]
pub struct MutationTypeInfo(pub Arc<DomainAdapter>);

impl GraphqlTypeName for MutationTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.domain_data.mutation_type_name
    }
}

impl_graphql_value!(Mutation, TypeInfo = MutationTypeInfo);

impl juniper::GraphQLType<GqlScalar> for Mutation {
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
            .flat_map(|entity_ref| {
                let type_name = entity_ref.type_data.type_name.as_str();

                [
                    // CREATE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.create_mutation_field_name,
                            &NodeTypeInfo(
                                info.0
                                    .dynamic_type_adapter(entity_ref.type_data.operator_id),
                            ),
                        )
                        .argument(
                            registry
                                .arg::<i32>("input", &())
                                .description(&format!("Input data for new {type_name}")),
                        ),
                    // UPDATE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.update_mutation_field_name,
                            &NodeTypeInfo(
                                info.0
                                    .dynamic_type_adapter(entity_ref.type_data.operator_id),
                            ),
                        )
                        .argument(
                            registry
                                .arg::<i32>("_id", &())
                                .description(&format!("Identifier for the {type_name} object")),
                        )
                        .argument(
                            registry
                                .arg::<i32>("input", &())
                                .description(&format!("Input data for the {type_name} update")),
                        ),
                    // DELETE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.delete_mutation_field_name,
                            &NodeTypeInfo(
                                info.0
                                    .dynamic_type_adapter(entity_ref.type_data.operator_id),
                            ),
                        )
                        .argument(
                            registry
                                .arg::<i32>("_id", &())
                                .description(&format!("Identifier for the {type_name} object")),
                        ),
                ]
            })
            .collect();

        registry
            .build_object_type::<Self>(info, &fields)
            .into_meta()
    }
}

impl juniper::GraphQLValueAsync<GqlScalar> for Mutation {
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a Self::TypeInfo,
        field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let (_mutation_kind, _operator_id) = info
                .0
                .domain_data
                .mutations
                .get(field_name)
                .expect("No such field");

            Ok(juniper::Value::null())
        })
    }
}

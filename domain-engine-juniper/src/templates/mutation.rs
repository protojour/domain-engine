use std::sync::Arc;

use crate::{
    adapter::{data::MutationKind, DomainAdapter},
    gql_scalar::GqlScalar,
    input::{deserialize_argument, register_domain_argument},
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
    GqlContext,
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
        let domain_adapter = DomainAdapter {
            domain_data: info.0.domain_data.clone(),
        };

        let fields: Vec<_> = info
            .0
            .iter_entities()
            .flat_map(|entity_ref| {
                let input_operator_id = entity_ref.type_data.operator_id;
                let id_operator_id = entity_ref.entity_data.id_operator_id;
                let type_name = entity_ref.type_data.type_name.as_str();

                [
                    // CREATE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.create_mutation_field_name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument(
                                "input",
                                input_operator_id,
                                &domain_adapter,
                                registry,
                            )
                            .description(&format!("Input data for new {type_name}")),
                        ),
                    // UPDATE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.update_mutation_field_name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument(
                                "_id",
                                id_operator_id,
                                &domain_adapter,
                                registry,
                            )
                            .description(&format!("Identifier for the {type_name} object")),
                        )
                        .argument(
                            register_domain_argument(
                                "input",
                                input_operator_id,
                                &domain_adapter,
                                registry,
                            )
                            .description(&format!("Input data for the {type_name} update")),
                        ),
                    // DELETE
                    registry
                        .field_convert::<Node, _, GqlContext>(
                            &entity_ref.entity_data.delete_mutation_field_name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument(
                                "_id",
                                id_operator_id,
                                &domain_adapter,
                                registry,
                            )
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
        arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            let env = info.0.domain_data.env.as_ref();
            let mutation_kind = info
                .0
                .domain_data
                .mutations
                .get(field_name)
                .expect("No such field");

            match mutation_kind {
                MutationKind::Create { input_operator_id } => {
                    let _input_value =
                        deserialize_argument(arguments, "input", *input_operator_id, env)?;
                }
                MutationKind::Update {
                    id_operator_id,
                    input_operator_id,
                } => {
                    let _id_value = deserialize_argument(arguments, "_id", *id_operator_id, env)?;
                    let _input_value =
                        deserialize_argument(arguments, "input", *input_operator_id, env)?;
                }
                MutationKind::Delete { id_operator_id: id } => {
                    let _id_value = deserialize_argument(arguments, "_id", *id, env)?;
                }
            };

            Ok(juniper::Value::null())
        })
    }
}

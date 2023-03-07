use tracing::debug;

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
pub struct MutationTypeInfo(pub DomainAdapter);

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
            .domain_data
            .mutations
            .iter()
            .map(|(name, mutation_data)| {
                let entity_ref = domain_adapter.entity_ref(mutation_data.entity_operator_id);
                let type_name = &entity_ref.type_data.type_name;

                match mutation_data.kind {
                    MutationKind::Create { input } => registry
                        .field_convert::<Node, _, GqlContext>(
                            name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument("input", input, &domain_adapter, registry)
                                .description(&format!("Input data for new {type_name}")),
                        ),
                    MutationKind::Update { input, id } => registry
                        .field_convert::<Node, _, GqlContext>(
                            name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument("_id", id, &domain_adapter, registry)
                                .description(&format!("Identifier for the {type_name} object")),
                        )
                        .argument(
                            register_domain_argument("input", input, &domain_adapter, registry)
                                .description(&format!("Input data for the {type_name} update")),
                        ),
                    MutationKind::Delete { id } => registry
                        .field_convert::<Node, _, GqlContext>(
                            name,
                            &NodeTypeInfo(info.0.node_adapter(entity_ref.type_data.operator_id)),
                        )
                        .argument(
                            register_domain_argument("_id", id, &domain_adapter, registry)
                                .description(&format!("Identifier for the {type_name} object")),
                        ),
                }
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
            let mutation_data = info
                .0
                .domain_data
                .mutations
                .get(field_name)
                .expect("No such field");

            match mutation_data.kind {
                MutationKind::Create { input } => {
                    let input_value = deserialize_argument(arguments, "input", input, env)?;

                    debug!("CREATE {input_value:?}");
                }
                MutationKind::Update { id, input } => {
                    let id_value = deserialize_argument(arguments, "_id", id, env)?;
                    let input_value = deserialize_argument(arguments, "input", input, env)?;

                    debug!("UPDATE {id_value:?}: {input_value:?}");
                }
                MutationKind::Delete { id } => {
                    let id_value = deserialize_argument(arguments, "_id", id, env)?;
                    debug!("DELETE {id_value:?}");
                }
            };

            Ok(juniper::Value::null())
        })
    }
}

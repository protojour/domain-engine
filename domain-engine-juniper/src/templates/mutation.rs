use tracing::debug;

use crate::{
    adapter::{data::MutationKind, DomainAdapter},
    gql_scalar::GqlScalar,
    input_value_deserializer::{deserialize_def_argument, deserialize_operator_argument},
    macros::impl_graphql_value,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
    GqlContext,
};

use super::node::{Node, NodeTypeInfo};

pub struct Mutation;

#[derive(Clone)]
pub struct MutationTypeInfo(pub DomainAdapter);

impl GraphqlTypeName for MutationTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.mutation_type_name
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
        let mut reg = RegistryWrapper::new(registry, &info.0);

        let fields: Vec<_> = info
            .0
            .mutations
            .iter()
            .map(|(name, mutation_data)| {
                let node_type_info = NodeTypeInfo(info.0.type_adapter(mutation_data.entity));
                let type_name = &node_type_info.0.type_data().type_name;

                match mutation_data.kind {
                    MutationKind::Create { input } => reg
                        .field_convert::<Node, _, GqlContext>(name, &node_type_info)
                        .argument(
                            reg.register_def_argument("input", input)
                                .description(&format!("Input data for new {type_name}")),
                        ),
                    MutationKind::Update { input, id } => reg
                        .field_convert::<Node, _, GqlContext>(name, &node_type_info)
                        .argument(
                            reg.register_operator_argument("_id", id)
                                .description(&format!("Identifier for the {type_name} object")),
                        )
                        .argument(
                            reg.register_def_argument("input", input)
                                .description(&format!("Input data for the {type_name} update")),
                        ),
                    MutationKind::Delete { id } => reg
                        .field_convert::<Node, _, GqlContext>(name, &node_type_info)
                        .argument(
                            reg.register_operator_argument("_id", id)
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
            let env = info.0.env.as_ref();
            let mutation_data = info.0.mutations.get(field_name).expect("No such field");

            match mutation_data.kind {
                MutationKind::Create { input } => {
                    let input_value = deserialize_def_argument(arguments, "input", input, env)?;

                    debug!("CREATE {input_value:?}");
                }
                MutationKind::Update { id, input } => {
                    let id_value = deserialize_operator_argument(arguments, "_id", id, env)?;
                    let input_value = deserialize_def_argument(arguments, "input", input, env)?;

                    debug!("UPDATE {id_value:?}: {input_value:?}");
                }
                MutationKind::Delete { id } => {
                    let id_value = deserialize_operator_argument(arguments, "_id", id, env)?;
                    debug!("DELETE {id_value:?}");
                }
            };

            Ok(juniper::Value::null())
        })
    }
}

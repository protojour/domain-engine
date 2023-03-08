use crate::{
    adapter::{TypeAdapter, UnionKind},
    gql_scalar::GqlScalar,
    registry_wrapper::RegistryWrapper,
    type_info::GraphqlTypeName,
    GqlContext,
};

pub struct Union;

#[derive(Clone)]
pub struct UnionTypeInfo(pub TypeAdapter<UnionKind>);

impl GraphqlTypeName for UnionTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.data().type_name
    }
}

impl juniper::GraphQLValue<GqlScalar> for Union {
    type Context = crate::GqlContext;
    type TypeInfo = UnionTypeInfo;
    fn type_name<'i>(&self, type_info: &'i Self::TypeInfo) -> Option<&'i str> {
        Some(type_info.graphql_type_name())
    }

    fn concrete_type_name(&self, _context: &GqlContext, _info: &UnionTypeInfo) -> String {
        todo!("Need to look at Value associated with this instance")
    }

    fn resolve_into_type(
        &self,
        _info: &Self::TypeInfo,
        _type_name: &str,
        _selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        _executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        todo!("Need to look at Value associated with this instance")
    }
}

impl juniper::GraphQLType<GqlScalar> for Union {
    fn name(info: &UnionTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &UnionTypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = RegistryWrapper::new(registry, &info.0.domain_data);

        let types = info
            .0
            .data()
            .variants
            .iter()
            .map(|(variant_def_id, _)| {
                reg.get_domain_type(info.0.domain_data.type_adapter(*variant_def_id))
            })
            .collect::<Vec<_>>();

        registry.build_union_type::<Self>(info, &types).into_meta()
    }
}

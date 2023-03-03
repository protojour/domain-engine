macro_rules! impl_graphql_value {
    ($ident:ident $(<$generics:tt>)?, TypeInfo = $type_info:ty) => {
        impl $(<$generics>)? ::juniper::GraphQLValue<crate::gql_scalar::GqlScalar> for $ident $(<$generics>)? {
            type Context = crate::GqlContext;
            type TypeInfo = $type_info;

            fn type_name<'i>(&self, type_info: &'i Self::TypeInfo) -> Option<&'i str> {
                Some(type_info.graphql_type_name())
            }

            fn resolve_field(
                &self,
                type_info: &Self::TypeInfo,
                field_name: &str,
                _arguments: &juniper::Arguments<crate::gql_scalar::GqlScalar>,
                _executor: &juniper::Executor<Self::Context, crate::gql_scalar::GqlScalar>,
            ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
                panic!(
                    "Tried to resolve async field {} on type {:?} with a sync resolver",
                    field_name,
                    <Self as juniper::GraphQLType<crate::gql_scalar::GqlScalar>>::name(type_info)
                )
            }
        }
    };
}

pub(crate) use impl_graphql_value;

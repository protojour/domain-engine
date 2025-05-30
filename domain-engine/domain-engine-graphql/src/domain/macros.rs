macro_rules! impl_graphql_value {
    ($ident:ident $(<$generics:tt>)?) => {
        impl $(<$generics>)? ::juniper::GraphQLValue<crate::gql_scalar::GqlScalar> for $ident $(<$generics>)? {
            type Context = crate::domain::context::ServiceCtx;
            type TypeInfo = crate::domain::context::SchemaType;

            fn type_name(&self, info: &Self::TypeInfo) -> Option<::arcstr::ArcStr> {
                Some(info.typename())
            }

            fn resolve_field(
                &self,
                type_info: &crate::domain::context::SchemaType,
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

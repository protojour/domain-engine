use crate::virtual_schema::VirtualIndexedTypeInfo;

pub fn resolve_indexed_type<'e, T, C, S>(
    value: T,
    sub_type_info: VirtualIndexedTypeInfo,
    executor: &juniper::Executor<'e, 'e, C, S>,
) -> juniper::ExecutionResult<S>
where
    T: juniper::GraphQLValue<S, Context = C, TypeInfo = VirtualIndexedTypeInfo>,
    C: juniper::Context + Send + Sync + 'static,
    S: juniper::ScalarValue + Send + Sync,
{
    let res = juniper::IntoResolvable::into_resolvable(value, executor.context());
    match res {
        Ok(Some((ctx, r))) => {
            let sub = executor.replaced_context(ctx);
            sub.resolve_with_ctx(&sub_type_info, &r)
        }
        Ok(None) => Ok(juniper::Value::null()),
        Err(e) => Err(e),
    }
}

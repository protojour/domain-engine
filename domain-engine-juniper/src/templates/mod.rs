use crate::context::SchemaType;

pub mod attribute_type;
pub mod indexed_input_value;
pub mod mutation_type;
pub mod query_type;
pub mod sequence_type;

pub fn resolve_schema_type_field<'e, T, C, S>(
    value: T,
    sub_type: SchemaType,
    executor: &juniper::Executor<'e, 'e, C, S>,
) -> juniper::ExecutionResult<S>
where
    T: juniper::GraphQLValue<S, Context = C, TypeInfo = SchemaType>,
    C: juniper::Context + Send + Sync + 'static,
    S: juniper::ScalarValue + Send + Sync,
{
    let res = juniper::IntoResolvable::into_resolvable(value, executor.context());
    match res {
        Ok(Some((ctx, r))) => {
            let sub = executor.replaced_context(ctx);
            sub.resolve_with_ctx(&sub_type, &r)
        }
        Ok(None) => Ok(juniper::Value::null()),
        Err(e) => Err(e),
    }
}

use ontol_runtime::value::Attribute;

use crate::{gql_scalar::GqlScalar, schema_ctx::IndexedTypeInfo, GqlContext};

use super::attribute_type::AttributeType;

pub struct SequenceType<'v> {
    pub seq: &'v [Attribute],
}

impl<'v> juniper::GraphQLValue<GqlScalar> for SequenceType<'v> {
    type Context = GqlContext;
    type TypeInfo = IndexedTypeInfo;

    fn type_name<'i>(&self, _info: &'i Self::TypeInfo) -> Option<&'i str> {
        None
    }

    fn resolve(
        &self,
        info: &Self::TypeInfo,
        _selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        let stop_on_null = executor
            .current_type()
            .list_contents()
            .expect("Current type is not a list type")
            .is_non_null();
        let mut result = Vec::with_capacity(self.seq.len());

        for attr in self.seq {
            let val = executor.resolve::<AttributeType>(info, &AttributeType { attr })?;
            if stop_on_null && val.is_null() {
                return Ok(val);
            } else {
                result.push(val)
            }
        }

        Ok(juniper::Value::list(result))
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for SequenceType<'v> {
    fn name(_info: &Self::TypeInfo) -> Option<&str> {
        None
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        registry
            .build_list_type::<AttributeType>(info, None)
            .into_meta()
    }
}

use ontol_runtime::attr::{AttrMatrixRef, AttrRef, AttrTupleRef};

use crate::{
    domain::{context::SchemaType, ServiceCtx},
    gql_scalar::GqlScalar,
};

use super::attribute_type::AttributeType;

/// A type which becomes a GraphQL list, but is a matrix in ONTOL parlance
pub struct MatrixType<'v> {
    pub matrix: AttrMatrixRef<'v>,
}

impl<'v> juniper::GraphQLValue<GqlScalar> for MatrixType<'v> {
    type Context = ServiceCtx;
    type TypeInfo = SchemaType;

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
        let mut result = Vec::with_capacity(
            self.matrix
                .columns
                .first()
                .map(|col| col.elements().len())
                .unwrap_or(0),
        );

        let mut tup = Default::default();
        let mut rows = self.matrix.rows();

        while rows.iter_next(&mut tup) {
            let val = executor.resolve::<AttributeType>(
                info,
                &AttributeType {
                    attr: AttrRef::Tuple(AttrTupleRef::Row(&tup.elements)),
                },
            )?;
            if stop_on_null && val.is_null() {
                return Ok(val);
            } else {
                result.push(val)
            }
        }

        Ok(juniper::Value::list(result))
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for MatrixType<'v> {
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

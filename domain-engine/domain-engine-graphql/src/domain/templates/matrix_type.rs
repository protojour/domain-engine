use arcstr::ArcStr;
use ontol_runtime::attr::{AttrMatrixRef, AttrRef, AttrTupleRef};

use crate::{
    domain::{ServiceCtx, context::SchemaType},
    gql_scalar::GqlScalar,
};

use super::attribute_type::AttributeType;

/// A type which becomes a GraphQL list, but is a matrix in ONTOL parlance
pub struct MatrixType<'v> {
    pub matrix: AttrMatrixRef<'v>,
}

impl juniper::GraphQLValue<GqlScalar> for MatrixType<'_> {
    type Context = ServiceCtx;
    type TypeInfo = SchemaType;

    fn type_name(&self, _info: &Self::TypeInfo) -> Option<ArcStr> {
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

impl juniper::GraphQLType<GqlScalar> for MatrixType<'_> {
    fn name(_info: &Self::TypeInfo) -> Option<ArcStr> {
        None
    }

    fn meta(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<GqlScalar>,
    ) -> juniper::meta::MetaType<GqlScalar> {
        registry
            .build_list_type::<AttributeType>(info, None)
            .into_meta()
    }
}

use ontol_runtime::{attr::AttrMatrixRef, sequence::SubSequence};

use crate::{
    context::SchemaType, cursor_util::serialize_cursor, gql_scalar::GqlScalar, ServiceCtx,
};

pub struct PageInfoType<'v> {
    pub matrix: AttrMatrixRef<'v>,
}

impl<'v> PageInfoType<'v> {
    fn find_sub(&self) -> Option<&SubSequence> {
        self.matrix.columns.iter().find_map(|column| column.sub())
    }
}

impl<'v> juniper::GraphQLValue<GqlScalar> for PageInfoType<'v> {
    type Context = ServiceCtx;
    type TypeInfo = SchemaType;

    fn type_name<'i>(&self, info: &'i Self::TypeInfo) -> Option<&'i str> {
        Some(info.typename())
    }

    fn resolve_field(
        &self,
        _info: &SchemaType,
        field_name: &str,
        _arguments: &juniper::Arguments<crate::gql_scalar::GqlScalar>,
        _executor: &juniper::Executor<Self::Context, crate::gql_scalar::GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        match field_name {
            "hasNextPage" => {
                let has_next_page = self
                    .find_sub()
                    .map(|sub_seq| sub_seq.has_next)
                    .unwrap_or(false);

                Ok(juniper::Value::Scalar(GqlScalar::Boolean(has_next_page)))
            }
            "endCursor" => {
                let end_cursor = self
                    .find_sub()
                    .and_then(|sub_seq| sub_seq.end_cursor.as_ref());

                let value = match end_cursor {
                    Some(next_cursor) => juniper::Value::Scalar(serialize_cursor(next_cursor)),
                    None => juniper::Value::Null,
                };

                Ok(value)
            }
            _ => panic!(),
        }
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for PageInfoType<'v> {
    fn name(_info: &Self::TypeInfo) -> Option<&str> {
        None
    }

    fn meta<'r>(
        _info: &Self::TypeInfo,
        _registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        panic!("This type is only used during resolving");
    }
}

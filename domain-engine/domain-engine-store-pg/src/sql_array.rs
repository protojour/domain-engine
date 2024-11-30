use std::{fmt::Debug, iter::Peekable};

use fallible_iterator::FallibleIterator;
use postgres_protocol::types::ArrayValues;
use postgres_types::{FromSql, Type};

use crate::{
    sql_iter::SqlIterator,
    sql_value::{CodecError, CodecResult},
};

pub struct SqlArray<'b> {
    inner: postgres_protocol::types::Array<'b>,
}

impl<'b> SqlArray<'b> {
    pub fn sql_iterator(&self) -> CodecResult<SqlArrayIterator<'b>> {
        let oid = self.inner.element_type();
        let Some(ty) = Type::from_oid(oid) else {
            return Err(CodecError("array contains invalid oid type".into()));
        };

        let iterator = self.inner.values().iterator().peekable();

        Ok(SqlArrayIterator {
            inner: iterator,
            ty,
        })
    }
}

impl<'b> FromSql<'b> for SqlArray<'b> {
    fn from_sql(
        _ty: &Type,
        raw: &'b [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let array = postgres_protocol::types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        Ok(SqlArray { inner: array })
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Debug for SqlArray<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlArray").finish()
    }
}

pub struct SqlArrayIterator<'b> {
    // inner: postgres_protocol::types::Array<'b>,
    inner: Peekable<fallible_iterator::Iterator<ArrayValues<'b>>>,
    ty: Type,
}

impl<'b> SqlIterator<'b> for SqlArrayIterator<'b> {
    fn has_next(&mut self) -> bool {
        self.inner.peek().is_some()
    }

    fn next<T: FromSql<'b>>(&mut self) -> CodecResult<T> {
        let Some(buf_result) = self.inner.next() else {
            return Err(CodecError("read beyond SqlArray length".into()));
        };
        T::from_sql_nullable(&self.ty, buf_result?).map_err(CodecError)
    }
}

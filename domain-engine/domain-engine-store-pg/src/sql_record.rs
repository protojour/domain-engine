use std::fmt::Debug;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;
use postgres_types::{FromSql, Type};
use tokio_postgres::Row;

use crate::{
    pg_model::PgRegKey,
    sql_iter::SqlIterator,
    sql_value::{CodecError, CodecResult},
};

pub struct SqlRecord<'b> {
    field_count: usize,
    buf: &'b [u8],
}

impl<'b> SqlRecord<'b> {
    pub fn def_key(&self) -> CodecResult<PgRegKey> {
        self.fields().next::<PgRegKey>()
    }

    pub fn fields(&self) -> RecordFields<'b> {
        RecordFields { buf: self.buf }
    }
}

impl<'b> FromSql<'b> for SqlRecord<'b> {
    fn from_sql(
        _ty: &Type,
        mut raw: &'b [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let field_count = read_record_field_count(&mut raw).map_err(|err| err.0)?;

        Ok(SqlRecord {
            field_count,
            buf: raw,
        })
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Debug for SqlRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlRecord")
            .field("field_count", &self.field_count)
            .finish()
    }
}

pub struct RecordFields<'b> {
    buf: &'b [u8],
}

impl<'b> SqlIterator<'b> for RecordFields<'b> {
    fn has_next(&mut self) -> bool {
        !self.buf.is_empty()
    }

    fn next<T: FromSql<'b>>(&mut self) -> CodecResult<T> {
        let oid = self
            .buf
            .read_u32::<BigEndian>()
            .map_err(|e| CodecError(e.into()))?;
        let field_len = self
            .buf
            .read_i32::<BigEndian>()
            .map_err(|e| CodecError(e.into()))?;

        let Some(ty) = Type::from_oid(oid) else {
            return Err(CodecError("invalid oid type".into()));
        };

        if field_len > 0 {
            let field_len = field_len as usize;
            let field_buf = &self.buf[0..field_len];

            self.buf.advance(field_len);

            T::from_sql_nullable(&ty, Some(field_buf)).map_err(CodecError)
        } else {
            T::from_sql_null(&ty).map_err(CodecError)
        }
    }
}

/// It was hard to find the binary documentation for composite/record types.
/// Used this as a reference: https://github.com/jackc/pgtype/blob/a4d4bbf043f7988ea29696a612cf311026fedf92/composite_type.go
fn read_record_field_count(buf: &mut &[u8]) -> CodecResult<usize> {
    let field_count = buf
        .read_i32::<BigEndian>()
        .map_err(|err| CodecError(err.into()))?;
    if field_count < 0 {
        return Err(CodecError("invalid field count".into()));
    }
    Ok(field_count as usize)
}

pub struct SqlColumnStream<'a> {
    row: &'a Row,
    index: usize,
}

impl<'a> SqlColumnStream<'a> {
    pub fn new(row: &'a Row) -> Self {
        Self { row, index: 0 }
    }
}

impl<'a> SqlIterator<'a> for SqlColumnStream<'a> {
    fn has_next(&mut self) -> bool {
        self.index < self.row.len()
    }

    fn next<T: FromSql<'a>>(&mut self) -> CodecResult<T> {
        if self.index >= self.row.columns().len() {
            Err(CodecError(
                format!("no column at index {}", self.index).into(),
            ))
        } else {
            let index = self.index;
            self.index += 1;

            self.row
                .try_get(index)
                .map_err(|err| CodecError(err.into()))
        }
    }
}

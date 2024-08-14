use std::fmt::Debug;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;
use tokio_postgres::Row;

use crate::{
    pg_model::{PgRegKey, PgType},
    sql_value::{CodecError, CodecResult, Layout, SqlVal},
};

pub trait SqlRecordIterator<'b> {
    fn next_field(&mut self, layout: &Layout) -> CodecResult<SqlVal<'b>>;
}

pub struct SqlRecord<'b> {
    field_count: usize,
    buf: &'b [u8],
}

impl<'b> SqlRecord<'b> {
    /// It was hard to find the binary documentation for composite/record types.
    /// Used this as a reference: https://github.com/jackc/pgtype/blob/a4d4bbf043f7988ea29696a612cf311026fedf92/composite_type.go
    pub(crate) fn from_sql(mut buf: &'b [u8]) -> CodecResult<Self> {
        let field_count = read_record_field_count(&mut buf)?;

        Ok(SqlRecord { field_count, buf })
    }

    pub fn field_count(&self) -> usize {
        self.field_count
    }

    pub fn def_key(&self) -> CodecResult<PgRegKey> {
        let mut buf = self.buf;
        let SqlVal::I32(def_key) = decode_record_field(&mut buf, &Layout::Scalar(PgType::Integer))?
        else {
            return Err(CodecError(
                "sql dyn record must start with an integer".into(),
            ));
        };
        Ok(def_key)
    }

    pub fn fields(&self) -> RecordFields<'b> {
        RecordFields { buf: self.buf }
    }
}

impl<'b> Debug for SqlRecord<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlRecord")
            .field("field_count", &self.field_count)
            .finish()
    }
}

pub struct RecordFields<'b> {
    buf: &'b [u8],
}

impl<'b> SqlRecordIterator<'b> for RecordFields<'b> {
    fn next_field(&mut self, layout: &Layout) -> CodecResult<SqlVal<'b>> {
        if self.buf.is_empty() {
            panic!("sql record exhausted");
        }

        decode_record_field(&mut self.buf, layout)
    }
}

fn read_record_field_count(buf: &mut &[u8]) -> CodecResult<usize> {
    let field_count = buf
        .read_i32::<BigEndian>()
        .map_err(|err| CodecError(err.into()))?;
    if field_count < 0 {
        return Err(CodecError("invalid field count".into()));
    }
    Ok(field_count as usize)
}

fn decode_record_field<'b>(buf: &mut &'b [u8], layout: &Layout) -> CodecResult<SqlVal<'b>> {
    let _oid = buf
        .read_u32::<BigEndian>()
        .map_err(|e| CodecError(e.into()))?;
    let field_len = buf
        .read_i32::<BigEndian>()
        .map_err(|e| CodecError(e.into()))?;

    if field_len > 0 {
        let field_len = field_len as usize;
        let field_buf = &buf[0..field_len];

        buf.advance(field_len);

        SqlVal::decode(Some(field_buf), layout)
    } else {
        Ok(SqlVal::Null)
    }
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

impl<'a> SqlRecordIterator<'a> for SqlColumnStream<'a> {
    fn next_field(&mut self, layout: &Layout) -> CodecResult<SqlVal<'a>> {
        if self.index >= self.row.columns().len() {
            Err(CodecError(
                format!("no column at index {}", self.index).into(),
            ))
        } else {
            let index = self.index;
            self.index += 1;
            let buf = self.row.col_buffer(index);

            SqlVal::decode(buf, layout)
        }
    }
}

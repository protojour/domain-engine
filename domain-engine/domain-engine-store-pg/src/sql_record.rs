use std::fmt::Debug;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;
use postgres_types::{FromSql, Type};
use tokio_postgres::Row;

use crate::{
    pg_model::{PgRegKey, PgType},
    sql_value::{CodecError, CodecResult, Layout, SqlOutput, SqlScalar},
};

pub trait SqlRecordIterator<'b> {
    fn next_field<T: FromSql<'b>>(&mut self) -> CodecResult<T>;

    fn next_field_dyn(&mut self, pg_type: PgType) -> CodecResult<SqlOutput<'b>> {
        match pg_type {
            PgType::Boolean => match self.next_field::<Option<bool>>()? {
                Some(bool) => Ok(SqlOutput::Scalar(SqlScalar::Bool(bool))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Integer => match self.next_field::<Option<i32>>()? {
                Some(i) => Ok(SqlOutput::Scalar(SqlScalar::I32(i))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::BigInt | PgType::Bigserial => match self.next_field::<Option<i64>>()? {
                Some(i) => Ok(SqlOutput::Scalar(SqlScalar::I64(i))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::DoublePrecision => match self.next_field::<Option<f64>>()? {
                Some(f) => Ok(SqlOutput::Scalar(SqlScalar::F64(f.into()))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Text => match self.next_field::<Option<String>>()? {
                Some(s) => Ok(SqlOutput::Scalar(SqlScalar::Text(s))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Bytea => match self.next_field::<Option<&[u8]>>()? {
                Some(b) => Ok(SqlOutput::Scalar(SqlScalar::Octets(
                    ontol_runtime::value::OctetSequence(b.iter().copied().collect()),
                ))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::TimestampTz => {
                match self.next_field::<Option<chrono::DateTime<chrono::Utc>>>()? {
                    Some(dt) => Ok(SqlOutput::Scalar(SqlScalar::Timestamp(dt))),
                    None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
                }
            }
        }
    }
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

    pub fn def_key(&self) -> CodecResult<PgRegKey> {
        let mut buf = self.buf;
        let SqlOutput::Scalar(SqlScalar::I32(def_key)) =
            decode_record_field(&mut buf, &Layout::Scalar(PgType::Integer))?
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
    fn next_field<T: FromSql<'b>>(&mut self) -> CodecResult<T> {
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

fn decode_record_field<'b>(buf: &mut &'b [u8], layout: &Layout) -> CodecResult<SqlOutput<'b>> {
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

        SqlOutput::decode(Some(field_buf), layout)
    } else {
        Ok(SqlOutput::Scalar(SqlScalar::Null))
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
    fn next_field<T: FromSql<'a>>(&mut self) -> CodecResult<T> {
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

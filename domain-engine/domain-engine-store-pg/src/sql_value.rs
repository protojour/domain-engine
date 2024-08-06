use std::fmt::Debug;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BytesMut};
use domain_engine_core::{DomainError, DomainResult};
use fallible_iterator::FallibleIterator;
use ontol_runtime::value::{Value, ValueTag};
use postgres_types::ToSql;
use tokio_postgres::Row;

use crate::pg_model::PgType;

pub type CodecResult<T> = Result<T, Box<dyn std::error::Error + Sync + Send>>;

#[derive(Debug)]
pub enum SqlVal<'b> {
    Null,
    Unit,
    I32(i32),
    I64(i64),
    F64(f64),
    Text(String),
    Octets(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Array(SqlArray<'b>),
    Record(SqlComposite<'b>),
}

/// Layout of Sql data
#[derive(Debug)]
pub enum Layout {
    Ignore,
    Scalar(PgType),
    Array(Box<Layout>),
    Record(Vec<Layout>),
}

pub fn domain_codec_error(error: Box<dyn std::error::Error + Sync + Send>) -> DomainError {
    DomainError::data_store(format!("codec error: {error:?}"))
}

impl<'b> SqlVal<'b> {
    pub fn null_filter(self) -> Option<Self> {
        match self {
            Self::Null => None,
            other => Some(other),
        }
    }

    pub fn non_null(self) -> DomainResult<Self> {
        match self {
            Self::Null => Err(DomainError::data_store("unexpected db null value")),
            other => Ok(other),
        }
    }

    pub fn into_i64(self) -> DomainResult<i64> {
        match self {
            Self::I64(int) => Ok(int),
            _ => Err(DomainError::data_store("expected i64")),
        }
    }

    pub fn into_array(self) -> DomainResult<SqlArray<'b>> {
        match self {
            Self::Array(array) => Ok(array),
            _ => Err(DomainError::data_store("expected array")),
        }
    }

    pub fn into_record(self) -> DomainResult<SqlComposite<'b>> {
        match self {
            Self::Record(composite) => Ok(composite),
            _ => Err(DomainError::data_store("expected record")),
        }
    }

    pub fn into_ontol(self, tag: ValueTag) -> DomainResult<Value> {
        match self {
            SqlVal::Unit | SqlVal::Null => Ok(Value::Unit(tag)),
            SqlVal::I32(i) => Ok(Value::I64(i as i64, tag)),
            SqlVal::I64(i) => Ok(Value::I64(i, tag)),
            SqlVal::F64(f) => Ok(Value::F64(f, tag)),
            SqlVal::Text(string) => Ok(Value::Text(string.into(), tag)),
            SqlVal::Octets(vec) => Ok(Value::OctetSequence(vec.into(), tag)),
            SqlVal::DateTime(dt) => Ok(Value::ChronoDateTime(dt, tag)),
            SqlVal::Date(d) => Ok(Value::ChronoDate(d, tag)),
            SqlVal::Time(t) => Ok(Value::ChronoTime(t, tag)),
            SqlVal::Array(_) | SqlVal::Record(_) => Err(DomainError::data_store(
                "cannot turn a composite into a value",
            )),
        }
    }

    pub fn next_column(
        iter: &mut impl Iterator<Item = CodecResult<SqlVal<'b>>>,
    ) -> DomainResult<SqlVal<'b>> {
        match iter.next() {
            Some(result) => result.map_err(domain_codec_error),
            None => Err(DomainError::data_store("too few columns")),
        }
    }

    fn decode(buf: Option<&'b [u8]>, layout: &'b Layout) -> CodecResult<Self> {
        let Some(raw) = buf else {
            return Ok(Self::Null);
        };

        match layout {
            Layout::Ignore => Ok(Self::Null),
            Layout::Scalar(PgType::BigInt | PgType::Bigserial) => {
                Ok(SqlVal::I64(postgres_protocol::types::int8_from_sql(raw)?))
            }
            Layout::Scalar(PgType::DoublePrecision) => {
                Ok(SqlVal::F64(postgres_protocol::types::float8_from_sql(raw)?))
            }
            Layout::Scalar(PgType::Boolean) => Ok(SqlVal::I64(
                if postgres_protocol::types::bool_from_sql(raw)? {
                    1
                } else {
                    0
                },
            )),
            Layout::Scalar(PgType::Text) => Ok(SqlVal::Text(
                postgres_protocol::types::text_from_sql(raw)?.to_string(),
            )),
            Layout::Scalar(PgType::Bytea) => Ok(SqlVal::Octets(
                postgres_protocol::types::bytea_from_sql(raw).into(),
            )),
            Layout::Scalar(PgType::Timestamp) => {
                //DateTime::<Utc>::from_sql(&Type::new(), raw);
                todo!()
            }
            Layout::Array(element_layout) => {
                let array = postgres_protocol::types::array_from_sql(raw)?;
                if array.dimensions().count()? > 1 {
                    return Err("array contains too many dimensions".into());
                }

                Ok(Self::Array(SqlArray {
                    inner: array,
                    element_layout,
                }))
            }
            Layout::Record(field_layouts) => {
                Ok(Self::Record(SqlComposite::from_sql(raw, field_layouts)?))
            }
        }
    }
}

impl<'b> ToSql for SqlVal<'b> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> CodecResult<tokio_postgres::types::IsNull>
    where
        Self: Sized,
    {
        match &self {
            SqlVal::Unit | SqlVal::Null => Option::<i32>::None.to_sql(ty, out),
            SqlVal::I32(i) => i.to_sql(ty, out),
            SqlVal::I64(i) => i.to_sql(ty, out),
            SqlVal::F64(f) => f.to_sql(ty, out),
            SqlVal::Text(s) => s.as_str().to_sql(ty, out),
            SqlVal::Octets(s) => s.as_slice().to_sql(ty, out),
            SqlVal::DateTime(dt) => dt.to_sql(ty, out),
            SqlVal::Date(d) => d.to_sql(ty, out),
            SqlVal::Time(t) => t.to_sql(ty, out),
            SqlVal::Array(_) | SqlVal::Record(_) => {
                Err("cannot convert output values to SQL".into())
            }
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> CodecResult<tokio_postgres::types::IsNull> {
        match &self {
            SqlVal::Unit | SqlVal::Null => Option::<i32>::None.to_sql_checked(ty, out),
            SqlVal::I32(i) => i.to_sql_checked(ty, out),
            SqlVal::I64(i) => i.to_sql_checked(ty, out),
            SqlVal::F64(f) => f.to_sql_checked(ty, out),
            SqlVal::Text(s) => s.as_str().to_sql_checked(ty, out),
            SqlVal::Octets(s) => s.as_slice().to_sql_checked(ty, out),
            SqlVal::DateTime(dt) => dt.to_sql_checked(ty, out),
            SqlVal::Date(d) => d.to_sql_checked(ty, out),
            SqlVal::Time(t) => t.to_sql_checked(ty, out),
            SqlVal::Array(_) | SqlVal::Record(_) => {
                Err("cannot convert output values to SQL".into())
            }
        }
    }
}

pub struct RowDecodeIterator<'b> {
    row: &'b Row,
    layout: &'b [Layout],
    index: usize,
}

impl<'b> RowDecodeIterator<'b> {
    pub fn new(row: &'b Row, layout: &'b [Layout]) -> Self {
        Self {
            row,
            layout,
            index: 0,
        }
    }
}

impl<'b> Iterator for RowDecodeIterator<'b> {
    type Item = CodecResult<SqlVal<'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row.columns().len() {
            None
        } else {
            let index = self.index;
            self.index += 1;
            let buf = self.row.col_buffer(index);

            Some(SqlVal::decode(buf, &self.layout[index]))
        }
    }
}

pub struct SqlArray<'b> {
    inner: postgres_protocol::types::Array<'b>,
    element_layout: &'b Layout,
}

impl<'b> SqlArray<'b> {
    pub fn elements(&self) -> impl Iterator<Item = CodecResult<SqlVal<'b>>> {
        let element_layout = self.element_layout;
        self.inner
            .values()
            .iterator()
            .map(move |buf_result| SqlVal::decode(buf_result?, element_layout))
    }
}

impl<'b> Debug for SqlArray<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlArray").finish()
    }
}

pub struct SqlComposite<'b> {
    field_count: i32,
    buf: &'b [u8],
    field_layout: &'b [Layout],
}

impl<'b> Debug for SqlComposite<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlComposite")
            .field("field_count", &self.field_count)
            .finish()
    }
}

impl<'b> SqlComposite<'b> {
    /// It was hard to find the binary documentation for composite types.
    /// Used this as a reference: https://github.com/jackc/pgtype/blob/a4d4bbf043f7988ea29696a612cf311026fedf92/composite_type.go
    fn from_sql(mut buf: &'b [u8], field_layout: &'b [Layout]) -> CodecResult<SqlComposite<'b>> {
        let field_count = buf.read_i32::<BigEndian>()?;
        if field_count < 0 {
            return Err("invalid field count".into());
        }

        if field_count as usize != field_layout.len() {
            return Err(
                "field layout length does not correspond to composite value field count".into(),
            );
        }

        Ok(SqlComposite {
            field_count,
            buf,
            field_layout,
        })
    }

    pub fn fields(&self) -> CompositeFields<'b> {
        CompositeFields {
            buf: self.buf,
            layout_iter: self.field_layout.iter(),
        }
    }
}

pub struct CompositeFields<'b> {
    buf: &'b [u8],
    layout_iter: std::slice::Iter<'b, Layout>,
}

impl<'b> Iterator for CompositeFields<'b> {
    type Item = CodecResult<SqlVal<'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return if self.layout_iter.next().is_some() {
                Some(Err("layout is longer than actual composite fields".into()))
            } else {
                None
            };
        }

        Some(self.decode_next_field())
    }
}

impl<'b> CompositeFields<'b> {
    fn decode_next_field(&mut self) -> CodecResult<SqlVal<'b>> {
        let Some(layout) = self.layout_iter.next() else {
            return Err("more composite fields than layout fields".into());
        };

        let _oid = self.buf.read_u32::<BigEndian>()?;
        let field_len: usize = self.buf.read_u32::<BigEndian>()? as usize;
        let field_buf = &self.buf[0..field_len];

        self.buf.advance(field_len);

        if !field_buf.is_empty() {
            SqlVal::decode(Some(field_buf), layout)
        } else {
            Ok(SqlVal::Null)
        }
    }
}

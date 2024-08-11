use std::fmt::Debug;

use bytes::BytesMut;
use domain_engine_core::{DomainError, DomainResult};
use fallible_iterator::FallibleIterator;
use postgres_types::{FromSql, ToSql, Type};

use crate::{
    ds_err,
    pg_model::PgType,
    sql_record::{SqlDynRecord, SqlRecord},
};

pub type CodecResult<T> = Result<T, Box<dyn std::error::Error + Sync + Send>>;

mod wellknown_oid {
    pub const TIMESTAMPTZ: u32 = 1184;
}

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
    Record(SqlRecord<'b>),
    DynRecord(SqlDynRecord<'b>),
}

/// Layout of Sql data
#[derive(Debug)]
pub enum Layout {
    Ignore,
    Scalar(PgType),
    Array(Box<Layout>),
    Record(Vec<Layout>),
    DynRecord,
}

pub fn domain_codec_error(error: Box<dyn std::error::Error + Sync + Send>) -> DomainError {
    ds_err(format!("codec error: {error:?}"))
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
            Self::Null => Err(ds_err("unexpected db null value")),
            other => Ok(other),
        }
    }

    pub fn into_i32(self) -> DomainResult<i32> {
        match self {
            Self::I32(int) => Ok(int),
            _ => Err(ds_err("expected i32")),
        }
    }

    pub fn into_i64(self) -> DomainResult<i64> {
        match self {
            Self::I64(int) => Ok(int),
            _ => Err(ds_err("expected i64")),
        }
    }

    pub fn into_array(self) -> DomainResult<SqlArray<'b>> {
        match self {
            Self::Array(array) => Ok(array),
            _ => Err(ds_err("expected array")),
        }
    }

    pub fn into_record(self) -> DomainResult<SqlRecord<'b>> {
        match self {
            Self::Record(record) => Ok(record),
            _ => Err(ds_err("expected record")),
        }
    }

    pub fn into_dyn_record(self) -> DomainResult<SqlDynRecord<'b>> {
        match self {
            Self::DynRecord(record) => Ok(record),
            _ => Err(ds_err("expected dyn record")),
        }
    }

    pub fn next_column(
        iter: &mut impl Iterator<Item = CodecResult<SqlVal<'b>>>,
    ) -> DomainResult<SqlVal<'b>> {
        match iter.next() {
            Some(result) => result.map_err(domain_codec_error),
            None => panic!("too few columns"),
            // None => Err(ds_err("too few columns")),
        }
    }

    pub(crate) fn decode(buf: Option<&'b [u8]>, layout: &'b Layout) -> CodecResult<Self> {
        let Some(raw) = buf else {
            return Ok(Self::Null);
        };

        match layout {
            Layout::Ignore => Ok(Self::Null),
            Layout::Scalar(PgType::Integer) => {
                Ok(SqlVal::I32(postgres_protocol::types::int4_from_sql(raw)?))
            }
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
            Layout::Scalar(PgType::TimestampTz) => Ok(SqlVal::DateTime(FromSql::from_sql(
                &Type::from_oid(wellknown_oid::TIMESTAMPTZ).unwrap(),
                raw,
            )?)),
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
                Ok(Self::Record(SqlRecord::from_sql(raw, field_layouts)?))
            }
            Layout::DynRecord => Ok(Self::DynRecord(SqlDynRecord::from_sql(raw)?)),
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
            Self::Unit | Self::Null => Option::<i32>::None.to_sql(ty, out),
            Self::I32(i) => i.to_sql(ty, out),
            Self::I64(i) => i.to_sql(ty, out),
            Self::F64(f) => f.to_sql(ty, out),
            Self::Text(s) => s.as_str().to_sql(ty, out),
            Self::Octets(s) => s.as_slice().to_sql(ty, out),
            Self::DateTime(dt) => dt.to_sql(ty, out),
            Self::Date(d) => d.to_sql(ty, out),
            Self::Time(t) => t.to_sql(ty, out),
            Self::Array(_) | Self::Record(_) | Self::DynRecord(_) => {
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
            Self::Unit | Self::Null => Option::<i32>::None.to_sql_checked(ty, out),
            Self::I32(i) => i.to_sql_checked(ty, out),
            Self::I64(i) => i.to_sql_checked(ty, out),
            Self::F64(f) => f.to_sql_checked(ty, out),
            Self::Text(s) => s.as_str().to_sql_checked(ty, out),
            Self::Octets(s) => s.as_slice().to_sql_checked(ty, out),
            Self::DateTime(dt) => dt.to_sql_checked(ty, out),
            Self::Date(d) => d.to_sql_checked(ty, out),
            Self::Time(t) => t.to_sql_checked(ty, out),
            Self::Array(_) | Self::Record(_) | Self::DynRecord(_) => {
                Err("cannot convert output values to SQL".into())
            }
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

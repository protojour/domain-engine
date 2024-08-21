use std::fmt::Debug;

use bytes::BytesMut;
use domain_engine_core::{DomainError, DomainResult};
use fallible_iterator::FallibleIterator;
use ontol_runtime::value::OctetSequence;
use postgres_types::{FromSql, ToSql, Type};
use tracing::error;

use crate::{pg_error::PgError, pg_model::PgType, sql_record::SqlRecord};

type BoxError = Box<dyn std::error::Error + Sync + Send>;

pub struct CodecError(pub BoxError);

impl From<Box<dyn std::error::Error + Sync + Send>> for CodecError {
    fn from(value: Box<dyn std::error::Error + Sync + Send>) -> Self {
        Self(value)
    }
}

impl From<CodecError> for DomainError {
    fn from(value: CodecError) -> Self {
        error!("codec error: {:?}", value.0);
        DomainError::data_store("internal datastore error")
    }
}

pub type CodecResult<T> = Result<T, CodecError>;

mod wellknown_oid {
    pub const TIMESTAMPTZ: u32 = 1184;
}

#[derive(Debug)]
pub enum SqlVal<'b> {
    Null,
    Unit,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Text(String),
    Octets(OctetSequence),
    DateTime(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Array(SqlArray<'b>),
    Record(SqlRecord<'b>),
}

/// Layout of Sql data
#[derive(Debug)]
pub enum Layout {
    Scalar(PgType),
    Array,
    Record,
}

impl<'b> SqlVal<'b> {
    pub fn null_filter(self) -> Option<Self> {
        match self {
            Self::Null => None,
            other => Some(other),
        }
    }

    #[allow(unused)]
    pub fn non_null(self) -> DomainResult<Self> {
        match self {
            Self::Null => Err(PgError::InvalidType("null").into()),
            other => Ok(other),
        }
    }

    pub fn into_i32(self) -> DomainResult<i32> {
        match self {
            Self::I32(int) => Ok(int),
            _ => Err(PgError::ExpectedType("i32").into()),
        }
    }

    pub fn into_i64(self) -> DomainResult<i64> {
        match self {
            Self::I64(int) => Ok(int),
            _ => Err(PgError::ExpectedType("i64").into()),
        }
    }

    pub fn into_array(self) -> DomainResult<SqlArray<'b>> {
        match self {
            Self::Array(array) => Ok(array),
            _ => Err(PgError::ExpectedType("array").into()),
        }
    }

    pub fn into_record(self) -> DomainResult<SqlRecord<'b>> {
        match self {
            Self::Record(record) => Ok(record),
            _ => Err(PgError::ExpectedType("record").into()),
        }
    }

    pub(crate) fn decode(buf: Option<&'b [u8]>, layout: &Layout) -> CodecResult<Self> {
        let Some(raw) = buf else {
            return Ok(Self::Null);
        };

        match layout {
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
            Layout::Scalar(PgType::Bytea) => Ok(SqlVal::Octets(OctetSequence(
                postgres_protocol::types::bytea_from_sql(raw).into(),
            ))),
            Layout::Scalar(PgType::TimestampTz) => Ok(SqlVal::DateTime(FromSql::from_sql(
                &Type::from_oid(wellknown_oid::TIMESTAMPTZ).unwrap(),
                raw,
            )?)),
            Layout::Array => {
                let array = postgres_protocol::types::array_from_sql(raw)?;
                if array.dimensions().count()? > 1 {
                    return Err(CodecError("array contains too many dimensions".into()));
                }

                Ok(Self::Array(SqlArray { inner: array }))
            }
            Layout::Record => Ok(Self::Record(SqlRecord::from_sql(raw)?)),
        }
    }
}

impl<'b> ToSql for SqlVal<'b> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, BoxError>
    where
        Self: Sized,
    {
        match &self {
            Self::Unit | Self::Null => Option::<i32>::None.to_sql(ty, out),
            Self::Bool(b) => b.to_sql(ty, out),
            Self::I32(i) => i.to_sql(ty, out),
            Self::I64(i) => i.to_sql(ty, out),
            Self::F64(f) => f.to_sql(ty, out),
            Self::Text(s) => s.as_str().to_sql(ty, out),
            Self::Octets(s) => s.0.as_slice().to_sql(ty, out),
            Self::DateTime(dt) => dt.to_sql(ty, out),
            Self::Date(d) => d.to_sql(ty, out),
            Self::Time(t) => t.to_sql(ty, out),
            Self::Array(_) | Self::Record(_) => Err("cannot convert output values to SQL".into()),
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
    ) -> Result<tokio_postgres::types::IsNull, BoxError> {
        match &self {
            Self::Unit | Self::Null => Option::<i32>::None.to_sql_checked(ty, out),
            Self::Bool(b) => b.to_sql_checked(ty, out),
            Self::I32(i) => i.to_sql_checked(ty, out),
            Self::I64(i) => i.to_sql_checked(ty, out),
            Self::F64(f) => f.to_sql_checked(ty, out),
            Self::Text(s) => s.as_str().to_sql_checked(ty, out),
            Self::Octets(s) => s.0.as_slice().to_sql_checked(ty, out),
            Self::DateTime(dt) => dt.to_sql_checked(ty, out),
            Self::Date(d) => d.to_sql_checked(ty, out),
            Self::Time(t) => t.to_sql_checked(ty, out),
            Self::Array(_) | Self::Record(_) => Err("cannot convert output values to SQL".into()),
        }
    }
}

pub struct SqlArray<'b> {
    inner: postgres_protocol::types::Array<'b>,
}

impl<'b> SqlArray<'b> {
    pub fn elements<'l>(
        &self,
        element_layout: &'l Layout,
    ) -> impl Iterator<Item = CodecResult<SqlVal<'b>>> + 'l
    where
        'b: 'l,
    {
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

use std::fmt::Debug;

use bytes::BytesMut;
use domain_engine_core::{DomainError, DomainResult};
use ontol_runtime::value::OctetSequence;
use ordered_float::OrderedFloat;
use postgres_types::ToSql;
use tracing::error;

use crate::pg_error::PgError;

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

/// Something put into PG
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SqlScalar {
    Null,
    Unit,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(OrderedFloat<f64>),
    Text(String),
    Octets(OctetSequence),
    Timestamp(PgTimestamp),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
}

pub type PgTimestamp = chrono::DateTime<chrono::Utc>;

/// Something read out of PG
#[derive(Debug)]
pub enum SqlOutput {
    Scalar(SqlScalar),
}

impl<'b> From<SqlScalar> for SqlOutput {
    fn from(value: SqlScalar) -> Self {
        Self::Scalar(value)
    }
}

impl SqlOutput {
    pub fn null_filter(self) -> Option<Self> {
        match self {
            Self::Scalar(SqlScalar::Null) => None,
            other => Some(other),
        }
    }

    #[expect(unused)]
    pub fn non_null(self) -> DomainResult<Self> {
        match self {
            Self::Scalar(SqlScalar::Null) => Err(PgError::InvalidType("null").into()),
            other => Ok(other),
        }
    }
}

impl ToSql for SqlScalar {
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
            Self::Timestamp(dt) => dt.to_sql(ty, out),
            Self::Date(d) => d.to_sql(ty, out),
            Self::Time(t) => t.to_sql(ty, out),
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
            Self::Timestamp(dt) => dt.to_sql_checked(ty, out),
            Self::Date(d) => d.to_sql_checked(ty, out),
            Self::Time(t) => t.to_sql_checked(ty, out),
        }
    }
}

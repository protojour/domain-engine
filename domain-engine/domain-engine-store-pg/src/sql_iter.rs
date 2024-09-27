use postgres_types::FromSql;

use crate::{
    pg_model::PgType,
    sql_value::{CodecResult, SqlOutput, SqlScalar},
};

pub trait SqlIterator<'b> {
    fn has_next(&mut self) -> bool;

    fn next<T: FromSql<'b>>(&mut self) -> CodecResult<T>;

    fn next_dyn(&mut self, pg_type: PgType) -> CodecResult<SqlOutput> {
        match pg_type {
            PgType::Boolean => match self.next::<Option<bool>>()? {
                Some(bool) => Ok(SqlOutput::Scalar(SqlScalar::Bool(bool))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Integer => match self.next::<Option<i32>>()? {
                Some(i) => Ok(SqlOutput::Scalar(SqlScalar::I32(i))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::BigInt | PgType::Bigserial => match self.next::<Option<i64>>()? {
                Some(i) => Ok(SqlOutput::Scalar(SqlScalar::I64(i))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::DoublePrecision => match self.next::<Option<f64>>()? {
                Some(f) => Ok(SqlOutput::Scalar(SqlScalar::F64(f.into()))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Text => match self.next::<Option<String>>()? {
                Some(s) => Ok(SqlOutput::Scalar(SqlScalar::Text(s))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::Bytea => match self.next::<Option<&[u8]>>()? {
                Some(b) => Ok(SqlOutput::Scalar(SqlScalar::Octets(
                    ontol_runtime::value::OctetSequence(b.iter().copied().collect()),
                ))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
            PgType::TimestampTz => match self.next::<Option<chrono::DateTime<chrono::Utc>>>()? {
                Some(dt) => Ok(SqlOutput::Scalar(SqlScalar::Timestamp(dt))),
                None => Ok(SqlOutput::Scalar(SqlScalar::Null)),
            },
        }
    }
}

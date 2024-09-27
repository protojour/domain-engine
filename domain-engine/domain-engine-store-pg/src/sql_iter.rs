use postgres_types::FromSql;

use crate::{
    pg_model::PgType,
    sql_value::{CodecResult, SqlScalar},
};

pub trait SqlIterator<'b> {
    fn has_next(&mut self) -> bool;

    fn next<T: FromSql<'b>>(&mut self) -> CodecResult<T>;

    fn next_dyn(&mut self, pg_type: PgType) -> CodecResult<SqlScalar> {
        match pg_type {
            PgType::Boolean => match self.next()? {
                Some(b) => Ok(SqlScalar::Bool(b)),
                None => Ok(SqlScalar::Null),
            },
            PgType::Integer => match self.next()? {
                Some(i) => Ok(SqlScalar::I32(i)),
                None => Ok(SqlScalar::Null),
            },
            PgType::BigInt | PgType::Bigserial => match self.next()? {
                Some(i) => Ok(SqlScalar::I64(i)),
                None => Ok(SqlScalar::Null),
            },
            PgType::DoublePrecision => match self.next::<Option<f64>>()? {
                Some(f) => Ok(SqlScalar::F64(f.into())),
                None => Ok(SqlScalar::Null),
            },
            PgType::Text => match self.next()? {
                Some(s) => Ok(SqlScalar::Text(s)),
                None => Ok(SqlScalar::Null),
            },
            PgType::Bytea => match self.next::<Option<&[u8]>>()? {
                Some(b) => Ok(SqlScalar::Octets(ontol_runtime::value::OctetSequence(
                    b.iter().copied().collect(),
                ))),
                None => Ok(SqlScalar::Null),
            },
            PgType::TimestampTz => match self.next()? {
                Some(dt) => Ok(SqlScalar::Timestamp(dt)),
                None => Ok(SqlScalar::Null),
            },
        }
    }
}

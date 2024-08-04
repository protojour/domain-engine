use std::collections::BTreeMap;

use anyhow::anyhow;
use bytes::BytesMut;
use domain_engine_core::{DomainError, DomainResult};
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    attr::Attr,
    query::filter::Filter,
    sequence::Sequence,
    value::{Value, ValueTag},
    RelId,
};
use thin_vec::ThinVec;
use tokio_postgres::types::{FromSql, ToSql};

use crate::pg_model::PgDataTable;

pub enum Data {
    Scalar(Scalar),
    #[allow(unused)]
    Compound(Compound),
}

impl From<Scalar> for Data {
    fn from(value: Scalar) -> Self {
        Self::Scalar(value)
    }
}

impl From<Compound> for Data {
    fn from(value: Compound) -> Self {
        Self::Compound(value)
    }
}

#[derive(Debug)]
pub enum Scalar {
    Unit,
    I64(i64),
    F64(f64),
    Text(smartstring::alias::String),
    Octets(ThinVec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
}

#[derive(Debug)]
#[allow(unused)]
pub enum Compound {
    Struct(Box<FnvHashMap<RelId, Attr>>, ValueTag),
    Dict(BTreeMap<smartstring::alias::String, Value>, ValueTag),
    Sequence(Sequence<Value>, ValueTag),
    DeleteRelationship(ValueTag),
    Filter(Box<Filter>, ValueTag),
}

impl Data {
    pub fn try_from_value(value: Value) -> DomainResult<Self> {
        match value {
            Value::Unit(_) | Value::Void(_) => Ok(Scalar::Unit.into()),
            Value::I64(n, _) => Ok(Scalar::I64(n).into()),
            Value::F64(f, _) => Ok(Scalar::F64(f).into()),
            Value::Serial(s, _) => {
                let i: i64 =
                    s.0.try_into()
                        .map_err(|_| DomainError::data_store_bad_request("serial overflow"))?;

                Ok(Scalar::I64(i).into())
            }
            Value::Rational(_, _) => Err(DomainError::data_store_bad_request(
                "rational not supported yet",
            )),
            Value::Text(s, _) => Ok(Scalar::Text(s).into()),
            Value::OctetSequence(s, _) => Ok(Scalar::Octets(s).into()),
            Value::ChronoDateTime(dt, _) => Ok(Scalar::DateTime(dt).into()),
            Value::ChronoDate(d, _) => Ok(Scalar::Date(d).into()),
            Value::ChronoTime(t, _) => Ok(Scalar::Time(t).into()),
            Value::Struct(map, tag) => Ok(Compound::Struct(map, tag).into()),
            Value::Dict(map, tag) => Ok(Compound::Dict(*map, tag).into()),
            Value::Sequence(seq, tag) => Ok(Compound::Sequence(seq, tag).into()),
            Value::DeleteRelationship(tag) => Ok(Compound::DeleteRelationship(tag).into()),
            Value::Filter(f, tag) => Ok(Compound::Filter(f, tag).into()),
        }
    }
}

impl Scalar {
    pub fn into_value(self, tag: ValueTag) -> Value {
        match self {
            Scalar::Unit => Value::Unit(tag),
            Scalar::I64(i) => Value::I64(i, tag),
            Scalar::F64(f) => Value::F64(f, tag),
            Scalar::Text(t) => Value::Text(t, tag),
            Scalar::Octets(o) => Value::OctetSequence(o, tag),
            Scalar::DateTime(dt) => Value::ChronoDateTime(dt, tag),
            Scalar::Date(d) => Value::ChronoDate(d, tag),
            Scalar::Time(t) => Value::ChronoTime(t, tag),
        }
    }
}

pub struct ScalarAttrs<'d> {
    pub map: FnvHashMap<RelId, Scalar>,
    pub datatable: &'d PgDataTable,
}

impl<'d> ScalarAttrs<'d> {
    pub(super) fn column_selection(&self) -> DomainResult<Vec<&'d str>> {
        let datatable = self.datatable;

        self.map
            .keys()
            .map(|rel_id| {
                let field = datatable.find_data_field(rel_id)?;
                Ok(field.column_name.as_ref())
            })
            .try_collect()
    }

    pub(super) fn as_params(&self) -> impl ExactSizeIterator<Item = &Scalar> {
        self.map.values()
    }
}

impl<'a> FromSql<'a> for Scalar {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.name() {
            "boolean" => Ok(Self::I64(if bool::from_sql(ty, raw)? { 1 } else { 0 })),
            "smallint" | "int" | "integer" | "bigint" | "int2" | "int4" | "int8" => {
                Ok(Self::I64(i64::from_sql(ty, raw)?))
            }
            "real" | "double precision" => Ok(Self::F64(f64::from_sql(ty, raw)?)),
            "text" => {
                let text = String::from_sql(ty, raw)?;
                Ok(Self::Text(text.into()))
            }
            "bytea" => {
                let octets = Vec::<u8>::from_sql(ty, raw)?;
                Ok(Self::Octets(octets.into()))
            }
            _ => Err(anyhow!("cannot deserialize Scalar from sql: {}", ty.name()).into()),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}

impl ToSql for Scalar {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self {
            Scalar::Unit => Option::<i32>::None.to_sql(ty, out),
            Scalar::I64(i) => i.to_sql(ty, out),
            Scalar::F64(f) => f.to_sql(ty, out),
            Scalar::Text(s) => s.as_str().to_sql(ty, out),
            Scalar::Octets(s) => s.as_slice().to_sql(ty, out),
            Scalar::DateTime(dt) => dt.to_sql(ty, out),
            Scalar::Date(d) => d.to_sql(ty, out),
            Scalar::Time(t) => t.to_sql(ty, out),
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
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self {
            Scalar::Unit => Option::<i32>::None.to_sql_checked(ty, out),
            Scalar::I64(i) => i.to_sql_checked(ty, out),
            Scalar::F64(f) => f.to_sql_checked(ty, out),
            Scalar::Text(s) => s.as_str().to_sql_checked(ty, out),
            Scalar::Octets(s) => s.as_slice().to_sql_checked(ty, out),
            Scalar::DateTime(dt) => dt.to_sql_checked(ty, out),
            Scalar::Date(d) => d.to_sql_checked(ty, out),
            Scalar::Time(t) => t.to_sql_checked(ty, out),
        }
    }
}

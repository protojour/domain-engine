use std::collections::BTreeMap;

use domain_engine_core::{transact::DataOperation, DomainResult};
use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DefKind, DefRepr},
    query::filter::Filter,
    sequence::Sequence,
    value::{Serial, Value, ValueTag},
    DefId, PropId,
};
use tracing::trace;

use crate::{
    pg_error::{ds_bad_req, ds_err},
    pg_model::{PgDataKey, PgRegKey},
    sql_value::{SqlOutput, SqlScalar},
};

use super::TransactCtx;

pub struct RowValue {
    pub value: Value,
    pub def_key: PgRegKey,
    pub data_key: PgDataKey,
    pub op: DataOperation,
}

pub enum Data {
    Sql(SqlScalar),
    #[allow(unused)]
    Compound(Compound),
}

impl From<SqlScalar> for Data {
    fn from(value: SqlScalar) -> Self {
        Self::Sql(value)
    }
}

impl From<Compound> for Data {
    fn from(value: Compound) -> Self {
        Self::Compound(value)
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum Compound {
    Struct(Box<FnvHashMap<PropId, Attr>>, ValueTag),
    Dict(BTreeMap<smartstring::alias::String, Value>, ValueTag),
    Sequence(Sequence<Value>, ValueTag),
    DeleteRelationship(ValueTag),
    Filter(Box<Filter>, ValueTag),
}

impl<'a> TransactCtx<'a> {
    pub fn deserialize_sql(&self, def_id: DefId, sql_val: SqlOutput) -> DomainResult<Value> {
        trace!("pg deserialize sql {def_id:?}");
        let tag = ValueTag::from(def_id);

        match &self.ontology.def(def_id).kind {
            DefKind::Data(basic) => match (sql_val, &basic.repr) {
                (sql_val, DefRepr::FmtStruct(Some((attr_prop_id, attr_def_id)))) => {
                    Ok(Value::Struct(
                        Box::new(
                            [(
                                *attr_prop_id,
                                Attr::Unit(self.deserialize_sql(*attr_def_id, sql_val)?),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        def_id.into(),
                    ))
                }
                (_sql_val, DefRepr::FmtStruct(None)) => {
                    unreachable!("tried to deserialize an empty FmtStruct (has no data)")
                }
                (SqlOutput::Scalar(SqlScalar::Unit | SqlScalar::Null), _) => Ok(Value::Unit(tag)),
                (SqlOutput::Scalar(SqlScalar::Bool(b)), _) => {
                    Ok(Value::I64(if b { 1 } else { 0 }, tag))
                }
                (SqlOutput::Scalar(SqlScalar::I32(i)), _) => Ok(Value::I64(i as i64, tag)),
                (SqlOutput::Scalar(SqlScalar::I64(i)), DefRepr::Serial) => Ok(Value::Serial(
                    Serial(i.try_into().map_err(|_| ds_err("serial underflow"))?),
                    tag,
                )),
                (SqlOutput::Scalar(SqlScalar::I64(i)), _) => Ok(Value::I64(i, tag)),
                (SqlOutput::Scalar(SqlScalar::F64(f)), _) => Ok(Value::F64(f.into(), tag)),
                (SqlOutput::Scalar(SqlScalar::Text(string)), _) => {
                    Ok(Value::Text(string.into(), tag))
                }
                (SqlOutput::Scalar(SqlScalar::Octets(seq)), _) => {
                    Ok(Value::OctetSequence(seq, tag))
                }
                (SqlOutput::Scalar(SqlScalar::DateTime(dt)), _) => {
                    Ok(Value::ChronoDateTime(dt, tag))
                }
                (SqlOutput::Scalar(SqlScalar::Date(d)), _) => Ok(Value::ChronoDate(d, tag)),
                (SqlOutput::Scalar(SqlScalar::Time(t)), _) => Ok(Value::ChronoTime(t, tag)),
                (SqlOutput::Array(_) | SqlOutput::Record(_), _) => {
                    Err(ds_err("cannot turn a composite into a value"))
                }
            },
            _ => Err(ds_err("unrecognized DefKind for PG scalar deserialization")),
        }
    }

    pub fn data_from_value(&self, value: Value) -> DomainResult<Data> {
        let def = self.ontology.def(value.type_def_id());

        match (value, &def.kind) {
            (Value::Unit(_) | Value::Void(_), _) => Ok(SqlScalar::Unit.into()),
            (Value::I64(int, _), _) => match def.repr() {
                Some(DefRepr::Boolean) => Ok(SqlScalar::Bool(int > 0).into()),
                _ => Ok(SqlScalar::I64(int).into()),
            },
            (Value::F64(float, _), _) => Ok(SqlScalar::F64(float.into()).into()),
            (Value::Serial(serial, _), _) => {
                let int: i64 = serial
                    .0
                    .try_into()
                    .map_err(|_| ds_bad_req("serial overflow"))?;

                Ok(SqlScalar::I64(int).into())
            }
            (Value::Rational(_, _), _) => Err(ds_bad_req("rational not supported yet")),
            (Value::Text(s, _), _) => Ok(SqlScalar::Text(s.into()).into()),
            (Value::OctetSequence(seq, _), _) => Ok(SqlScalar::Octets(seq).into()),
            (Value::ChronoDateTime(dt, _), _) => Ok(SqlScalar::DateTime(dt).into()),
            (Value::ChronoDate(d, _), _) => Ok(SqlScalar::Date(d).into()),
            (Value::ChronoTime(t, _), _) => Ok(SqlScalar::Time(t).into()),
            (Value::Struct(map, tag), DefKind::Data(basic_def)) => match &basic_def.repr {
                DefRepr::FmtStruct(Some(_)) => {
                    let inner_value = map
                        .into_values()
                        .next()
                        .ok_or_else(|| ds_bad_req("missing property in fmt struct"))?
                        .unwrap_unit();

                    self.data_from_value(inner_value)
                }
                _ => Ok(Compound::Struct(map, tag).into()),
            },
            (Value::Struct(map, tag), _) => Ok(Compound::Struct(map, tag).into()),
            (Value::Dict(map, tag), _) => Ok(Compound::Dict(*map, tag).into()),
            (Value::Sequence(seq, tag), _) => Ok(Compound::Sequence(seq, tag).into()),
            (Value::DeleteRelationship(tag), _) => Ok(Compound::DeleteRelationship(tag).into()),
            (Value::Filter(f, tag), _) => Ok(Compound::Filter(f, tag).into()),
        }
    }
}

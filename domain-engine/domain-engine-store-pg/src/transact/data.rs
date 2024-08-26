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
    sql_value::SqlVal,
};

use super::TransactCtx;

pub struct RowValue {
    pub value: Value,
    pub def_key: PgRegKey,
    pub data_key: PgDataKey,
    pub op: DataOperation,
}

pub enum Data<'a> {
    Sql(SqlVal<'a>),
    #[allow(unused)]
    Compound(Compound),
}

impl<'a> From<SqlVal<'a>> for Data<'a> {
    fn from(value: SqlVal<'a>) -> Self {
        Self::Sql(value)
    }
}

impl<'a> From<Compound> for Data<'a> {
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
    pub fn deserialize_sql(&self, def_id: DefId, sql_val: SqlVal) -> DomainResult<Value> {
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
                (SqlVal::Unit | SqlVal::Null, _) => Ok(Value::Unit(tag)),
                (SqlVal::Bool(b), _) => Ok(Value::I64(if b { 1 } else { 0 }, tag)),
                (SqlVal::I32(i), _) => Ok(Value::I64(i as i64, tag)),
                (SqlVal::I64(i), DefRepr::Serial) => Ok(Value::Serial(
                    Serial(i.try_into().map_err(|_| ds_err("serial underflow"))?),
                    tag,
                )),
                (SqlVal::I64(i), _) => Ok(Value::I64(i, tag)),
                (SqlVal::F64(f), _) => Ok(Value::F64(f, tag)),
                (SqlVal::Text(string), _) => Ok(Value::Text(string.into(), tag)),
                (SqlVal::Octets(seq), _) => Ok(Value::OctetSequence(seq, tag)),
                (SqlVal::DateTime(dt), _) => Ok(Value::ChronoDateTime(dt, tag)),
                (SqlVal::Date(d), _) => Ok(Value::ChronoDate(d, tag)),
                (SqlVal::Time(t), _) => Ok(Value::ChronoTime(t, tag)),
                (SqlVal::Array(_) | SqlVal::Record(_), _) => {
                    Err(ds_err("cannot turn a composite into a value"))
                }
            },
            _ => Err(ds_err("unrecognized DefKind for PG scalar deserialization")),
        }
    }

    pub fn data_from_value(&self, value: Value) -> DomainResult<Data<'static>> {
        let def = self.ontology.def(value.type_def_id());

        match (value, &def.kind) {
            (Value::Unit(_) | Value::Void(_), _) => Ok(SqlVal::Unit.into()),
            (Value::I64(int, _), _) => match def.repr() {
                Some(DefRepr::Boolean) => Ok(SqlVal::Bool(int > 0).into()),
                _ => Ok(SqlVal::I64(int).into()),
            },
            (Value::F64(float, _), _) => Ok(SqlVal::F64(float).into()),
            (Value::Serial(serial, _), _) => {
                let int: i64 = serial
                    .0
                    .try_into()
                    .map_err(|_| ds_bad_req("serial overflow"))?;

                Ok(SqlVal::I64(int).into())
            }
            (Value::Rational(_, _), _) => Err(ds_bad_req("rational not supported yet")),
            (Value::Text(s, _), _) => Ok(SqlVal::Text(s.into()).into()),
            (Value::OctetSequence(seq, _), _) => Ok(SqlVal::Octets(seq).into()),
            (Value::ChronoDateTime(dt, _), _) => Ok(SqlVal::DateTime(dt).into()),
            (Value::ChronoDate(d, _), _) => Ok(SqlVal::Date(d).into()),
            (Value::ChronoTime(t, _), _) => Ok(SqlVal::Time(t).into()),
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

use std::collections::BTreeMap;

use compact_str::CompactString;
use domain_engine_core::{DomainResult, transact::DataOperation};
use fnv::FnvHashMap;
use ontol_runtime::{
    DefId, PropId,
    attr::Attr,
    crdt::CrdtStruct,
    ontology::domain::{DefKind, DefRepr},
    query::filter::Filter,
    sequence::Sequence,
    value::{Serial, Value, ValueTag},
};

use crate::{
    pg_error::{ds_bad_req, ds_err},
    pg_model::{PgDataKey, PgRegKey},
    sql_value::{PgTimestamp, SqlScalar},
};

use super::TransactCtx;

pub struct RowValue {
    pub value: Value,
    pub def_key: PgRegKey,
    pub data_key: PgDataKey,
    pub updated_at: PgTimestamp,
    pub op: DataOperation,
}

pub enum Data {
    Sql(SqlScalar),
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
#[expect(unused)]
pub enum Compound {
    Struct(Box<FnvHashMap<PropId, Attr>>, ValueTag),
    CrdtStruct(CrdtStruct, ValueTag),
    Dict(BTreeMap<CompactString, Value>, ValueTag),
    Sequence(Sequence<Value>, ValueTag),
    DeleteRelationship(ValueTag),
    Filter(Box<Filter>, ValueTag),
}

pub struct ParentProp {
    pub prop_id: PropId,
    pub key: PgDataKey,
}

impl TransactCtx<'_> {
    pub fn deserialize_sql(&self, def_id: DefId, sql_val: SqlScalar) -> DomainResult<Value> {
        let tag = ValueTag::from(def_id);

        match &self.ontology_defs.def(def_id).kind {
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
                (SqlScalar::Unit | SqlScalar::Null, _) => Ok(Value::Unit(tag)),
                (SqlScalar::Bool(b), _) => Ok(Value::I64(if b { 1 } else { 0 }, tag)),
                (SqlScalar::I32(i), _) => Ok(Value::I64(i as i64, tag)),
                (SqlScalar::I64(i), DefRepr::Serial) => Ok(Value::Serial(
                    Serial(i.try_into().map_err(|_| ds_err("serial underflow"))?),
                    tag,
                )),
                (SqlScalar::I64(i), _) => Ok(Value::I64(i, tag)),
                (SqlScalar::F64(f), _) => Ok(Value::F64(f.into(), tag)),
                (SqlScalar::Text(string), _) => Ok(Value::Text(string.into(), tag)),
                (SqlScalar::Octets(seq), _) => Ok(Value::OctetSequence(seq, tag)),
                (SqlScalar::Timestamp(dt), _) => Ok(Value::ChronoDateTime(dt, tag)),
                (SqlScalar::Date(d), _) => Ok(Value::ChronoDate(d, tag)),
                (SqlScalar::Time(t), _) => Ok(Value::ChronoTime(t, tag)),
            },
            _ => Err(ds_err("unrecognized DefKind for PG scalar deserialization")),
        }
    }

    pub fn data_from_value(&self, value: Value) -> DomainResult<Data> {
        let def = self.ontology_defs.def(value.type_def_id());

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
            (Value::ChronoDateTime(dt, _), _) => Ok(SqlScalar::Timestamp(dt).into()),
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
            (Value::CrdtStruct(crdt, tag), _) => Ok(Compound::CrdtStruct(crdt, tag).into()),
            (Value::Dict(map, tag), _) => Ok(Compound::Dict(*map, tag).into()),
            (Value::Sequence(seq, tag), _) => Ok(Compound::Sequence(seq, tag).into()),
            (Value::DeleteRelationship(tag), _) => Ok(Compound::DeleteRelationship(tag).into()),
            (Value::Filter(f, tag), _) => Ok(Compound::Filter(f, tag).into()),
        }
    }
}

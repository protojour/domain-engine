use std::collections::BTreeMap;

use domain_engine_core::{transact::DataOperation, DomainError, DomainResult};
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DefKind, DefRepr},
    query::filter::Filter,
    sequence::Sequence,
    value::{Value, ValueTag},
    DefId, RelId,
};
use tracing::trace;

use crate::{
    pg_model::{PgDataKey, PgDataTable},
    sql_value::SqlVal,
};

use super::TransactCtx;

pub struct RowValue {
    pub value: Value,
    #[allow(unused)]
    pub key: PgDataKey,
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
    Struct(Box<FnvHashMap<RelId, Attr>>, ValueTag),
    Dict(BTreeMap<smartstring::alias::String, Value>, ValueTag),
    Sequence(Sequence<Value>, ValueTag),
    DeleteRelationship(ValueTag),
    Filter(Box<Filter>, ValueTag),
}

impl<'a> TransactCtx<'a> {
    pub fn deserialize_sql(&self, def_id: DefId, sql: SqlVal) -> DomainResult<Value> {
        trace!("pg deserialize sql {def_id:?}");

        match &self.ontology.def(def_id).kind {
            DefKind::Data(basic) => match basic.repr {
                DefRepr::FmtStruct(Some((attr_rel_id, attr_def_id))) => Ok(Value::Struct(
                    Box::new(
                        [(attr_rel_id, Attr::Unit(sql.into_ontol(attr_def_id.into())?))]
                            .into_iter()
                            .collect(),
                    ),
                    def_id.into(),
                )),
                DefRepr::FmtStruct(None) => {
                    unreachable!("tried to deserialize an empty FmtStruct (has no data)")
                }
                _ => sql.into_ontol(def_id.into()),
            },
            _ => Err(DomainError::data_store(
                "unrecognized DefKind for PG scalar deserialization",
            )),
        }
    }

    pub fn data_from_value(&self, value: Value) -> DomainResult<Data> {
        let def = self.ontology.def(value.type_def_id());

        match (value, &def.kind) {
            (Value::Unit(_) | Value::Void(_), _) => Ok(SqlVal::Unit.into()),
            (Value::I64(int, _), _) => Ok(SqlVal::I64(int).into()),
            (Value::F64(float, _), _) => Ok(SqlVal::F64(float).into()),
            (Value::Serial(serial, _), _) => {
                let int: i64 = serial
                    .0
                    .try_into()
                    .map_err(|_| DomainError::data_store_bad_request("serial overflow"))?;

                Ok(SqlVal::I64(int).into())
            }
            (Value::Rational(_, _), _) => Err(DomainError::data_store_bad_request(
                "rational not supported yet",
            )),
            (Value::Text(s, _), _) => Ok(SqlVal::Text(s.into()).into()),
            (Value::OctetSequence(vec, _), _) => Ok(SqlVal::Octets(vec.into()).into()),
            (Value::ChronoDateTime(dt, _), _) => Ok(SqlVal::DateTime(dt).into()),
            (Value::ChronoDate(d, _), _) => Ok(SqlVal::Date(d).into()),
            (Value::ChronoTime(t, _), _) => Ok(SqlVal::Time(t).into()),
            (Value::Struct(map, tag), DefKind::Data(basic_def)) => match &basic_def.repr {
                DefRepr::FmtStruct(Some(_)) => {
                    let inner_value = map
                        .into_values()
                        .next()
                        .ok_or_else(|| {
                            DomainError::data_store_bad_request("missing property in fmt struct")
                        })?
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

pub struct ScalarAttrs<'m, 'b> {
    pub map: FnvHashMap<RelId, SqlVal<'b>>,
    pub datatable: &'m PgDataTable,
}

impl<'m, 'b> ScalarAttrs<'m, 'b> {
    pub(super) fn column_selection(&self) -> DomainResult<Vec<&'m str>> {
        let datatable = self.datatable;

        self.map
            .keys()
            .map(|rel_id| {
                let field = datatable.field(rel_id)?;
                Ok(field.col_name.as_ref())
            })
            .try_collect()
    }

    pub(super) fn as_params(&self) -> impl ExactSizeIterator<Item = &SqlVal> {
        self.map.values()
    }
}

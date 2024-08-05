use std::error::Error;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BytesMut};
use domain_engine_core::{DomainError, DomainResult};
use fallible_iterator::FallibleIterator;
use ontol_runtime::{
    ontology::{
        domain::{DefKind, DefRepr},
        Ontology,
    },
    DefId,
};
use postgres_types::ToSql;
use thin_vec::ThinVec;
use tokio_postgres::Row;

use crate::pg_model::PgType;

#[derive(Clone, Debug)]
pub enum SqlVal {
    Unit,
    I32(i32),
    I64(i64),
    F64(f64),
    Text(smartstring::alias::String),
    Octets(ThinVec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Array(Vec<Option<SqlVal>>),
    Record(Vec<Option<SqlVal>>),
}

/// Layout of Sql data
#[derive(Debug)]
pub enum Layout {
    Ignore,
    Scalar(PgType),
    Array(Box<Layout>),
    Record(Vec<Layout>),
}

pub fn get_pg_type(def_id: DefId, ontology: &Ontology) -> DomainResult<Option<PgType>> {
    let def = ontology.get_def(def_id).unwrap();
    let def_repr = match &def.kind {
        DefKind::Data(basic_def) => &basic_def.repr,
        _ => &DefRepr::Unknown,
    };

    match def_repr {
        DefRepr::Unit => Err(DomainError::data_store("TODO: ignore unit column")),
        DefRepr::I64 => Ok(Some(PgType::BigInt)),
        DefRepr::F64 => Ok(Some(PgType::DoublePrecision)),
        DefRepr::Serial => Ok(Some(PgType::Bigserial)),
        DefRepr::Boolean => Ok(Some(PgType::Boolean)),
        DefRepr::Text => Ok(Some(PgType::Text)),
        DefRepr::Octets => Ok(Some(PgType::Bytea)),
        DefRepr::DateTime => Ok(Some(PgType::Timestamp)),
        DefRepr::FmtStruct(Some((_rel_id, def_id))) => get_pg_type(*def_id, ontology),
        DefRepr::FmtStruct(None) => Ok(None),
        DefRepr::Seq => todo!("seq"),
        DefRepr::Struct => todo!("struct"),
        DefRepr::Intersection(_) => todo!("intersection"),
        DefRepr::Union(..) => todo!("union"),
        DefRepr::Unknown => Err(DomainError::data_store("unknown repr: {def_id:?}")),
    }
}

pub fn read_column(row: &Row, layout: &[Layout], index: usize) -> DomainResult<Option<SqlVal>> {
    if let Some(buffer) = row.col_buffer(index) {
        read_value(&layout[index], Some(buffer))
            .map_err(|e| DomainError::data_store(format!("failed to deserialize column: {e:?}")))
    } else {
        Ok(None)
    }
}

fn read_value(
    layout: &Layout,
    buf: Option<&[u8]>,
) -> Result<Option<SqlVal>, Box<dyn std::error::Error + Sync + Send>> {
    let Some(raw) = buf else { return Ok(None) };

    match layout {
        Layout::Ignore => Ok(None),
        Layout::Scalar(PgType::BigInt | PgType::Bigserial) => Ok(Some(SqlVal::I64(
            postgres_protocol::types::int8_from_sql(raw)?,
        ))),
        Layout::Scalar(PgType::DoublePrecision) => Ok(Some(SqlVal::F64(
            postgres_protocol::types::float8_from_sql(raw)?,
        ))),
        Layout::Scalar(PgType::Boolean) => Ok(Some(SqlVal::I64(
            if postgres_protocol::types::bool_from_sql(raw)? {
                1
            } else {
                0
            },
        ))),
        Layout::Scalar(PgType::Text) => Ok(Some(SqlVal::Text(
            postgres_protocol::types::text_from_sql(raw)?.into(),
        ))),
        Layout::Scalar(PgType::Bytea) => Ok(Some(SqlVal::Octets(
            postgres_protocol::types::bytea_from_sql(raw).into(),
        ))),
        Layout::Scalar(PgType::Timestamp) => todo!(),
        Layout::Array(sub) => {
            let array = postgres_protocol::types::array_from_sql(raw)?;
            if array.dimensions().count()? > 1 {
                return Err("array contains too many dimensions".into());
            }

            let items = array.values().map(|v| read_value(sub, v)).collect()?;

            Ok(Some(SqlVal::Array(items)))
        }
        Layout::Record(field_layouts) => {
            let composite = Composite::from_sql(raw)?;

            if composite.field_count as usize != field_layouts.len() {
                return Err(
                    "field layout length does not correspond to composite value field count".into(),
                );
            }

            let mut elements: Vec<Option<SqlVal>> =
                Vec::with_capacity(composite.field_count as usize);

            let mut fields = composite.fields();
            let mut field_layout_iter = field_layouts.iter();

            for field_layout in field_layout_iter.by_ref() {
                if let Some(element) = fields.next_field(field_layout)? {
                    elements.push(element);
                } else {
                    break;
                }
            }

            assert!(field_layout_iter.next().is_none());

            Ok(Some(SqlVal::Record(elements)))
        }
    }
}

impl ToSql for SqlVal {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self {
            SqlVal::Unit => Option::<i32>::None.to_sql(ty, out),
            SqlVal::I32(i) => i.to_sql(ty, out),
            SqlVal::I64(i) => i.to_sql(ty, out),
            SqlVal::F64(f) => f.to_sql(ty, out),
            SqlVal::Text(s) => s.as_str().to_sql(ty, out),
            SqlVal::Octets(s) => s.as_slice().to_sql(ty, out),
            SqlVal::DateTime(dt) => dt.to_sql(ty, out),
            SqlVal::Date(d) => d.to_sql(ty, out),
            SqlVal::Time(t) => t.to_sql(ty, out),
            SqlVal::Array(v) | SqlVal::Record(v) => v.as_slice().to_sql(ty, out),
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
            SqlVal::Unit => Option::<i32>::None.to_sql_checked(ty, out),
            SqlVal::I32(i) => i.to_sql_checked(ty, out),
            SqlVal::I64(i) => i.to_sql_checked(ty, out),
            SqlVal::F64(f) => f.to_sql_checked(ty, out),
            SqlVal::Text(s) => s.as_str().to_sql_checked(ty, out),
            SqlVal::Octets(s) => s.as_slice().to_sql_checked(ty, out),
            SqlVal::DateTime(dt) => dt.to_sql_checked(ty, out),
            SqlVal::Date(d) => d.to_sql_checked(ty, out),
            SqlVal::Time(t) => t.to_sql_checked(ty, out),
            SqlVal::Array(v) | SqlVal::Record(v) => v.as_slice().to_sql_checked(ty, out),
        }
    }
}

struct Composite<'a> {
    field_count: i32,
    buf: &'a [u8],
}

impl<'a> Composite<'a> {
    /// It was hard to find the binary documentation for composite types.
    /// Used this as a reference: https://github.com/jackc/pgtype/blob/a4d4bbf043f7988ea29696a612cf311026fedf92/composite_type.go
    fn from_sql(mut buf: &[u8]) -> Result<Composite<'_>, Box<dyn Error + Sync + Send>> {
        let field_count = buf.read_i32::<BigEndian>()?;
        if field_count < 0 {
            return Err("invalid field count".into());
        }

        Ok(Composite { field_count, buf })
    }

    fn fields(&self) -> CompositeFields {
        CompositeFields { buf: self.buf }
    }
}

struct CompositeFields<'a> {
    buf: &'a [u8],
}

impl<'a> CompositeFields<'a> {
    fn next_field(
        &mut self,
        layout: &Layout,
    ) -> Result<Option<Option<SqlVal>>, Box<dyn std::error::Error + Sync + Send>> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let _oid = self.buf.read_u32::<BigEndian>()?;
        let field_len = self.buf.read_u32::<BigEndian>()? as usize;

        let field_buf = &self.buf[0..field_len];
        self.buf.advance(field_len);

        let value = read_value(layout, Some(field_buf))?;

        Ok(Some(value))
    }
}

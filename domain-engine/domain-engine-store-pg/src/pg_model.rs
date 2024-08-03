use fnv::FnvHashMap;
use ontol_runtime::DefRelTag;
use serde::{de::value::StrDeserializer, Deserialize, Serialize};
use tokio_postgres::types::FromSql;

pub type PgSerial = i64;
pub type DomainUid = ulid::Ulid;

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(i32)]
pub enum RegVersion {
    Init = 1,
}

impl RegVersion {
    pub const fn current() -> Self {
        Self::Init
    }
}

impl TryFrom<i32> for RegVersion {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Init),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
pub struct DefUid(pub DomainUid, pub u16);

#[derive(Clone)]
pub struct PgDomain {
    pub key: Option<PgSerial>,
    pub schema_name: Box<str>,
    pub datatables: FnvHashMap<DefUid, PgDataTable>,
}

#[derive(Clone)]
pub struct PgDataTable {
    pub key: PgSerial,
    pub table_name: Box<str>,
    pub data_fields: FnvHashMap<DefRelTag, PgDataField>,
}

#[derive(Clone)]
pub struct PgDataField {
    pub column_name: Box<str>,
    pub pg_type: PgType,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub enum PgType {
    /// TODO: Can join all bool fields in one bitstring that's just appended to?
    Boolean,
    Text,
    Bytea,
}

impl PgType {
    pub fn to_string(&self) -> anyhow::Result<String> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(string)) => Ok(string),
            _ => panic!("cannot serialize to string"),
        }
    }
}

impl<'a> FromSql<'a> for PgType {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let str = <&str as FromSql>::from_sql(ty, raw)?;
        Self::deserialize(StrDeserializer::<serde_json::Error>::new(str)).map_err(|err| err.into())
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

use std::ops::Deref;

use fnv::FnvHashMap;
use ontol_runtime::{value::Value, DefId, DefRelTag, PackageId};
use serde::{de::value::StrDeserializer, Deserialize, Serialize};
use tokio_postgres::types::FromSql;

pub type PgSerial = i64;
pub type DomainUid = ulid::Ulid;

pub struct PgModel {
    #[allow(unused)]
    pub(crate) domains: FnvHashMap<PackageId, PgDomain>,
}

impl PgModel {
    pub(crate) fn get_datatable(&self, pkg_id: PackageId, def_id: DefId) -> Option<&PgDataTable> {
        let pg_domain = self.domains.get(&pkg_id)?;

        pg_domain.datatables.get(&def_id)
    }
}

/// Something belonging to a specific persisted domain
#[derive(Clone, Copy)]
pub struct InDomain<T> {
    pub pkg_id: PackageId,
    pub value: T,
}

impl<T> Deref for InDomain<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl From<Value> for InDomain<Value> {
    fn from(value: Value) -> Self {
        Self {
            pkg_id: value.type_def_id().package_id(),
            value,
        }
    }
}

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

#[derive(Clone)]
pub struct PgDomain {
    pub key: Option<PgSerial>,
    pub schema_name: Box<str>,
    pub datatables: FnvHashMap<DefId, PgDataTable>,
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
#[serde(rename_all = "lowercase")]
pub enum PgType {
    /// TODO: Can join all bool fields in one bitstring that's just appended to?
    Boolean,
    Text,
    Bytea,
    Timestamp,
    Bigserial,
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

use std::ops::Deref;

use domain_engine_core::{DomainError, DomainResult};
use fnv::FnvHashMap;
use ontol_runtime::{value::Value, DefId, DefRelTag, PackageId, RelId};
use serde::{de::value::StrDeserializer, Deserialize, Serialize};
use tokio_postgres::types::FromSql;
use tracing::debug;

pub type PgSerial = i64;
pub type DomainUid = ulid::Ulid;

pub struct PgModel {
    #[allow(unused)]
    pub(crate) domains: FnvHashMap<PackageId, PgDomain>,
}

impl PgModel {
    pub(crate) fn find_pg_domain(&self, pkg_id: PackageId) -> DomainResult<&PgDomain> {
        self.domains
            .get(&pkg_id)
            .ok_or_else(|| DomainError::data_store(format!("pg_domain not found for {pkg_id:?}")))
    }

    pub(crate) fn find_datatable(
        &self,
        pkg_id: PackageId,
        def_id: DefId,
    ) -> DomainResult<&PgDataTable> {
        self.domains
            .get(&pkg_id)
            .and_then(|pg_domain| pg_domain.datatables.get(&def_id))
            .ok_or_else(|| {
                DomainError::data_store(format!(
                    "pg_datatable not found for {pkg_id:?}->{def_id:?}"
                ))
            })
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

impl PgDataTable {
    pub fn find_data_field(&self, rel_id: &RelId) -> DomainResult<&PgDataField> {
        self.data_fields.get(&rel_id.tag()).ok_or_else(|| {
            debug!("field not found in {:?}", self.data_fields);

            DomainError::data_store(format!(
                "datatable not found for {rel_id:?} in {}",
                self.table_name
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct PgDataField {
    pub column_name: Box<str>,
    pub pg_type: PgType,
}

/// NB: Do not change the names of these enum variants.
/// They are serialized to and deserialized from DB.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum PgType {
    /// TODO: Can join all bool fields in one bitstring that's just appended to?
    Boolean,
    /// i64
    BigInt,
    /// f64
    DoublePrecision,
    Text,
    /// byte array
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

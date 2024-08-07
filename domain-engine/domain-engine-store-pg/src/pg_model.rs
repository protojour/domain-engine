use std::{collections::BTreeMap, ops::Deref};

use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::{
        domain::{DefKind, DefRepr},
        Ontology,
    },
    tuple::CardinalIdx,
    value::Value,
    DefId, DefRelTag, PackageId, RelId,
};
use postgres_types::ToSql;
use serde::{de::value::StrDeserializer, Deserialize, Serialize};
use tokio_postgres::types::FromSql;
use tracing::debug;

use crate::{ds_err, sql};

/// The key type used in the registry for metadata
pub type PgRegKey = i32;

/// The key type used for data in the domains
pub type PgDataKey = i64;

pub type DomainUid = ulid::Ulid;

pub struct PgModel {
    #[allow(unused)]
    pub(crate) domains: FnvHashMap<PackageId, PgDomain>,

    pub(crate) entity_id_to_entity: FnvHashMap<DefId, DefId>,
}

impl PgModel {
    pub(crate) fn pg_domain(&self, pkg_id: PackageId) -> DomainResult<&PgDomain> {
        self.domains
            .get(&pkg_id)
            .ok_or_else(|| ds_err(format!("pg_domain not found for {pkg_id:?}")))
    }

    pub(crate) fn pg_domain_table(
        &self,
        pkg_id: PackageId,
        def_id: DefId,
    ) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(pkg_id)?;
        let datatable = self.datatable(pkg_id, def_id)?;
        Ok(PgDomainTable { domain, datatable })
    }

    pub(crate) fn find_datatable(&self, pkg_id: PackageId, def_id: DefId) -> Option<&PgDataTable> {
        self.domains
            .get(&pkg_id)
            .and_then(|pg_domain| pg_domain.datatables.get(&def_id))
    }

    pub(crate) fn datatable(&self, pkg_id: PackageId, def_id: DefId) -> DomainResult<&PgDataTable> {
        self.find_datatable(pkg_id, def_id)
            .ok_or_else(|| ds_err(format!("pg_datatable not found for {pkg_id:?}->{def_id:?}")))
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
    pub key: Option<PgRegKey>,
    pub schema_name: Box<str>,
    pub datatables: FnvHashMap<DefId, PgDataTable>,
    pub edges: FnvHashMap<u16, PgEdge>,
}

#[derive(Clone)]
pub struct PgDataTable {
    pub key: PgRegKey,
    pub table_name: Box<str>,
    pub data_fields: FnvHashMap<DefRelTag, PgDataField>,
    pub datafield_indexes: FnvHashMap<(DefId, PgIndexType), PgIndexData>,
}

impl PgDataTable {
    pub fn field(&self, rel_id: &RelId) -> DomainResult<&PgDataField> {
        self.data_fields.get(&rel_id.tag()).ok_or_else(|| {
            debug!("field not found in {:?}", self.data_fields);

            ds_err(format!(
                "datatable not found for {rel_id:?} in {}",
                self.table_name
            ))
        })
    }

    pub fn field_by_key(&self, key: PgRegKey) -> Option<&PgDataField> {
        self.data_fields
            .values()
            .find(|datafield| datafield.key == key)
    }
}

#[derive(Clone, Debug)]
pub struct PgIndexData {
    pub datafield_keys: Vec<PgRegKey>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, ToSql, FromSql, Debug)]
#[postgres(name = "m6m_index_type")]
pub enum PgIndexType {
    #[postgres(name = "unique")]
    Unique,
    #[postgres(name = "btree")]
    BTree,
}

#[derive(Clone, Copy)]
pub struct PgDomainTable<'a> {
    pub domain: &'a PgDomain,
    pub datatable: &'a PgDataTable,
}

impl<'a> PgDomainTable<'a> {
    pub fn table_name(self) -> sql::TableName<'a> {
        sql::TableName(&self.domain.schema_name, &self.datatable.table_name)
    }
}

#[derive(Clone, Debug)]
pub struct PgDataField {
    pub key: PgRegKey,
    pub col_name: Box<str>,
    pub pg_type: PgType,
}

#[derive(Clone)]
pub struct PgEdge {
    pub key: PgRegKey,
    pub table_name: Box<str>,
    pub cardinals: BTreeMap<usize, PgEdgeCardinal>,
}

impl PgEdge {
    pub fn cardinal(&self, c: CardinalIdx) -> DomainResult<&PgEdgeCardinal> {
        self.cardinals
            .get(&(c.0 as usize))
            .ok_or_else(|| ds_err("edge cardinal not found"))
    }
}

#[derive(Clone)]
pub struct PgEdgeCardinal {
    #[allow(unused)]
    pub key: PgRegKey,
    #[allow(unused)]
    pub ident: Box<str>,
    pub def_col_name: Box<str>,
    pub key_col_name: Box<str>,
}

/// NB: Do not change the names of these enum variants.
/// They are serialized to and deserialized from DB.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum PgType {
    /// TODO: Can join all bool fields in one bitstring that's just appended to?
    Boolean,
    /// i32
    Integer,
    /// i64
    BigInt,
    /// f64
    DoublePrecision,
    Text,
    /// byte array
    Bytea,
    TimestampTz,
    Bigserial,
}

impl PgType {
    pub fn as_string(&self) -> anyhow::Result<String> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(string)) => Ok(string),
            _ => panic!("cannot serialize to string"),
        }
    }

    pub fn from_def_id(def_id: DefId, ontology: &Ontology) -> DomainResult<Option<PgType>> {
        let def = ontology.get_def(def_id).unwrap();
        let def_repr = match &def.kind {
            DefKind::Data(basic_def) => &basic_def.repr,
            _ => &DefRepr::Unknown,
        };

        match def_repr {
            DefRepr::Unit => Err(ds_err("TODO: ignore unit column")),
            DefRepr::I64 => Ok(Some(PgType::BigInt)),
            DefRepr::F64 => Ok(Some(PgType::DoublePrecision)),
            DefRepr::Serial => Ok(Some(PgType::Bigserial)),
            DefRepr::Boolean => Ok(Some(PgType::Boolean)),
            DefRepr::Text => Ok(Some(PgType::Text)),
            DefRepr::Octets => Ok(Some(PgType::Bytea)),
            DefRepr::DateTime => Ok(Some(PgType::TimestampTz)),
            DefRepr::FmtStruct(Some((_rel_id, def_id))) => Self::from_def_id(*def_id, ontology),
            DefRepr::FmtStruct(None) => Ok(None),
            DefRepr::Seq => todo!("seq"),
            DefRepr::Struct => todo!("struct"),
            DefRepr::Intersection(_) => todo!("intersection"),
            DefRepr::Union(..) => todo!("union"),
            DefRepr::Unknown => Err(ds_err("unknown repr: {def_id:?}")),
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

use std::{collections::BTreeMap, ops::Deref};

use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::{
        domain::{Def, DefKind, DefRepr},
        Ontology,
    },
    tuple::CardinalIdx,
    value::Value,
    DefId, DefRelTag, EdgeId, PackageId, RelId,
};
use postgres_types::ToSql;
use tokio_postgres::types::FromSql;
use tracing::debug;

use crate::{pg_error::PgModelError, sql};

/// The key type used in the registry for metadata
pub type PgRegKey = i32;

/// The key type used for data in the domains
pub type PgDataKey = i64;

pub type DomainUid = ulid::Ulid;

pub enum PgTableKey {
    Data { pkg_id: PackageId, def_id: DefId },
}

pub struct PgModel {
    #[allow(unused)]
    pub(crate) domains: FnvHashMap<PackageId, PgDomain>,
    /// A map from PG table key to semantic key
    /// This is used for dynamic typing.
    pub(crate) reg_key_to_table_key: FnvHashMap<PgRegKey, PgTableKey>,
    pub(crate) entity_id_to_entity: FnvHashMap<DefId, DefId>,
}

impl PgModel {
    pub(crate) fn new(
        domains: FnvHashMap<PackageId, PgDomain>,
        entity_id_to_entity: FnvHashMap<DefId, DefId>,
    ) -> Self {
        let mut reg_key_to_table_key: FnvHashMap<PgRegKey, PgTableKey> = Default::default();

        for (pkg_id, pg_domain) in &domains {
            for (def_id, pg_table) in &pg_domain.datatables {
                reg_key_to_table_key.insert(
                    pg_table.key,
                    PgTableKey::Data {
                        pkg_id: *pkg_id,
                        def_id: *def_id,
                    },
                );
            }
        }

        Self {
            domains,
            reg_key_to_table_key,
            entity_id_to_entity,
        }
    }

    pub(crate) fn pg_domain(&self, pkg_id: PackageId) -> DomainResult<&PgDomain> {
        Ok(self
            .domains
            .get(&pkg_id)
            .ok_or(PgModelError::DomainNotFound(pkg_id))?)
    }

    pub(crate) fn pg_domain_datatable(
        &self,
        pkg_id: PackageId,
        def_id: DefId,
    ) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(pkg_id)?;
        let datatable = self.datatable(pkg_id, def_id)?;
        Ok(PgDomainTable {
            domain,
            table: datatable,
        })
    }

    pub(crate) fn pg_domain_edgetable(&self, edge_id: &EdgeId) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(edge_id.0)?;
        let datatable = self.edgetable(edge_id)?;
        Ok(PgDomainTable {
            domain,
            table: datatable,
        })
    }

    pub(crate) fn find_datatable(&self, pkg_id: PackageId, def_id: DefId) -> Option<&PgTable> {
        self.domains
            .get(&pkg_id)
            .and_then(|pg_domain| pg_domain.datatables.get(&def_id))
    }

    pub(crate) fn datatable(&self, pkg_id: PackageId, def_id: DefId) -> DomainResult<&PgTable> {
        Ok(self
            .find_datatable(pkg_id, def_id)
            .ok_or(PgModelError::CollectionNotFound(pkg_id, def_id))?)
    }

    pub(crate) fn find_edgetable(&self, edge_id: &EdgeId) -> Option<&PgTable> {
        self.domains
            .get(&edge_id.0)
            .and_then(|pg_domain| pg_domain.edgetables.get(&edge_id.1))
    }

    pub(crate) fn edgetable(&self, edge_id: &EdgeId) -> DomainResult<&PgTable> {
        Ok(self
            .find_edgetable(edge_id)
            .ok_or(PgModelError::EdgeNotFound(*edge_id))?)
    }

    pub(crate) fn datatable_key_by_def_key(
        &self,
        def_key: PgRegKey,
    ) -> DomainResult<(PackageId, DefId)> {
        let Some(PgTableKey::Data { pkg_id, def_id }) = self.reg_key_to_table_key.get(&def_key)
        else {
            return Err(PgModelError::NotFoundInRegistry(def_key).into());
        };
        Ok((*pkg_id, *def_id))
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
    pub datatables: FnvHashMap<DefId, PgTable>,
    pub edgetables: FnvHashMap<u16, PgTable>,
}

#[derive(Clone, Copy, Debug)]
pub enum PgTableIdUnion {
    Def(DefId),
    Edge(EdgeId),
}

#[derive(Clone)]
pub struct PgTable {
    pub key: PgRegKey,
    pub table_name: Box<str>,
    pub has_fkey: bool,
    pub data_fields: FnvHashMap<DefRelTag, PgDataField>,
    pub edge_cardinals: BTreeMap<CardinalIdx, PgEdgeCardinal>,
    pub datafield_indexes: FnvHashMap<(DefId, PgIndexType), PgIndexData>,
}

impl PgDomain {
    pub fn get_table(&self, id: &PgTableIdUnion) -> Option<&PgTable> {
        match id {
            PgTableIdUnion::Def(def_id) => self.datatables.get(def_id),
            PgTableIdUnion::Edge(edge_id) => self.edgetables.get(&edge_id.1),
        }
    }
}

impl PgTable {
    pub fn field(&self, rel_id: &RelId) -> DomainResult<&PgDataField> {
        self.data_fields.get(&rel_id.tag()).ok_or_else(|| {
            debug!("field not found in {:?}", self.data_fields);

            PgModelError::FieldNotFound(self.table_name.clone(), *rel_id).into()
        })
    }

    pub fn field_by_key(&self, key: PgRegKey) -> Option<&PgDataField> {
        self.data_fields
            .values()
            .find(|datafield| datafield.key == key)
    }

    pub fn edge_cardinal(&self, c: CardinalIdx) -> DomainResult<&PgEdgeCardinal> {
        Ok(self
            .edge_cardinals
            .get(&c)
            .ok_or(PgModelError::EdgeCardinalNotFound(c))?)
    }
}

pub struct PgDef<'a> {
    pub def: &'a Def,
    pub pg: PgDomainTable<'a>,
}

#[derive(Clone, Debug)]
pub struct PgIndexData {
    pub datafield_keys: Vec<PgRegKey>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, ToSql, FromSql, Debug)]
#[postgres(name = "m6m_pg_index_type")]
pub enum PgIndexType {
    #[postgres(name = "unique")]
    Unique,
    #[postgres(name = "btree")]
    BTree,
}

#[derive(Clone, Copy)]
pub struct PgDomainTable<'a> {
    pub domain: &'a PgDomain,
    pub table: &'a PgTable,
}

impl<'a> PgDomainTable<'a> {
    pub fn table_name(self) -> sql::TableName<'a> {
        sql::TableName(&self.domain.schema_name, &self.table.table_name)
    }
}

#[derive(Clone, Debug)]
pub struct PgDataField {
    pub key: PgRegKey,
    pub col_name: Box<str>,
    pub pg_type: PgType,
}

#[derive(Clone, Debug)]
pub struct PgEdgeCardinal {
    #[allow(unused)]
    pub key: PgRegKey,
    #[allow(unused)]
    pub ident: Box<str>,
    pub kind: PgEdgeCardinalKind,
    pub index_type: Option<PgIndexType>,
}

impl PgEdgeCardinal {
    pub fn key_col_name(&self) -> Option<&str> {
        match &self.kind {
            PgEdgeCardinalKind::Dynamic { key_col_name, .. } => Some(key_col_name),
            PgEdgeCardinalKind::PinnedDef { key_col_name, .. } => Some(key_col_name),
            PgEdgeCardinalKind::Parameters(_) => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PgEdgeCardinalKind {
    /// Dynamic can link to unions, so it needs a def_col_name
    Dynamic {
        def_col_name: Box<str>,
        key_col_name: Box<str>,
    },
    /// Unique can only link to fixed vertex type, the edge cardinal
    /// is an "extension" of that vertex.
    /// Unique edge cardinals have ON DELETE CASCADE set up.
    PinnedDef {
        def_id: DefId,
        key_col_name: Box<str>,
    },
    Parameters(DefId),
}

/// NB: Do not change the names of these enum variants.
/// They are serialized to and deserialized from DB.
#[derive(Clone, Copy, PartialEq, ToSql, FromSql, Debug)]
#[postgres(name = "m6m_pg_type")]
pub enum PgType {
    /// TODO: Can join all bool fields in one bitstring that's just appended to?
    #[postgres(name = "boolean")]
    Boolean,
    /// i32
    #[postgres(name = "integer")]
    Integer,
    /// i64
    #[postgres(name = "bigint")]
    BigInt,
    /// f64
    #[postgres(name = "double precision")]
    DoublePrecision,
    #[postgres(name = "text")]
    Text,
    /// byte array
    #[postgres(name = "bytea")]
    Bytea,
    #[postgres(name = "timestamptz")]
    TimestampTz,
    #[postgres(name = "bigserial")]
    Bigserial,
}

impl PgType {
    pub fn from_def_id(def_id: DefId, ontology: &Ontology) -> DomainResult<Option<PgType>> {
        let def = ontology.get_def(def_id).unwrap();
        let def_repr = match &def.kind {
            DefKind::Data(basic_def) => &basic_def.repr,
            _ => &DefRepr::Unknown,
        };

        match def_repr {
            DefRepr::Unit => Ok(None),
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
            DefRepr::Struct => Err(PgModelError::DataTypeNotSupported("struct").into()),
            DefRepr::Intersection(_) => {
                Err(PgModelError::DataTypeNotSupported("intersection").into())
            }
            DefRepr::Union(..) => Err(PgModelError::DataTypeNotSupported("union").into()),
            DefRepr::Macro => Ok(None),
            DefRepr::Unknown => Err(PgModelError::DataTypeNotSupported("unknown").into()),
        }
    }

    pub fn insert_default(&self) -> bool {
        matches!(self, Self::Bigserial)
    }
}

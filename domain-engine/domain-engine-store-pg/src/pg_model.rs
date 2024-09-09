use std::{collections::BTreeMap, ops::Deref};

use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::{
        domain::{Def, DefKind, DefRepr, DefReprUnionBound},
        Ontology,
    },
    tuple::CardinalIdx,
    value::Value,
    DefId, DefPropTag, DomainIndex, OntolDefTag, PropId,
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EdgeId(pub DefId);

impl EdgeId {
    #[inline]
    pub fn domain_index(&self) -> DomainIndex {
        self.0 .0
    }

    #[inline]
    pub fn def_id(&self) -> DefId {
        self.0
    }
}

pub enum PgTableKey {
    Data {
        domain_index: DomainIndex,
        def_id: DefId,
    },
}

pub struct PgModel {
    pub(crate) domains: FnvHashMap<DomainIndex, PgDomain>,
    /// A map from PG table key to semantic key
    /// This is used for dynamic typing.
    pub(crate) reg_key_to_table_key: FnvHashMap<PgRegKey, PgTableKey>,
    pub(crate) entity_id_to_entity: FnvHashMap<DefId, DefId>,
}

impl PgModel {
    pub(crate) fn new(
        domains: FnvHashMap<DomainIndex, PgDomain>,
        entity_id_to_entity: FnvHashMap<DefId, DefId>,
    ) -> Self {
        let mut reg_key_to_table_key: FnvHashMap<PgRegKey, PgTableKey> = Default::default();

        for (domain_index, pg_domain) in &domains {
            for (def_id, pg_table) in &pg_domain.datatables {
                reg_key_to_table_key.insert(
                    pg_table.key,
                    PgTableKey::Data {
                        domain_index: *domain_index,
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

    pub(crate) fn pg_domain(&self, domain_index: DomainIndex) -> DomainResult<&PgDomain> {
        Ok(self
            .domains
            .get(&domain_index)
            .ok_or(PgModelError::DomainNotFound(domain_index))?)
    }

    pub(crate) fn pg_domain_datatable(
        &self,
        domain_index: DomainIndex,
        def_id: DefId,
    ) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(domain_index)?;
        let datatable = self.datatable(domain_index, def_id)?;
        Ok(PgDomainTable {
            domain,
            table: datatable,
        })
    }

    pub(crate) fn pg_domain_edgetable(&self, edge_id: &EdgeId) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(edge_id.domain_index())?;
        let datatable = self.edgetable(edge_id)?;
        Ok(PgDomainTable {
            domain,
            table: datatable,
        })
    }

    pub(crate) fn find_datatable(
        &self,
        domain_index: DomainIndex,
        def_id: DefId,
    ) -> Option<&PgTable> {
        self.domains
            .get(&domain_index)
            .and_then(|pg_domain| pg_domain.datatables.get(&def_id))
    }

    pub(crate) fn datatable(
        &self,
        domain_index: DomainIndex,
        def_id: DefId,
    ) -> DomainResult<&PgTable> {
        Ok(self
            .find_datatable(domain_index, def_id)
            .ok_or(PgModelError::CollectionNotFound(domain_index, def_id))?)
    }

    pub(crate) fn find_edgetable(&self, edge_id: &EdgeId) -> Option<&PgTable> {
        self.domains
            .get(&edge_id.domain_index())
            .and_then(|pg_domain| pg_domain.edgetables.get(&edge_id.def_id().1))
    }

    pub(crate) fn edgetable(&self, edge_id: &EdgeId) -> DomainResult<&PgTable> {
        Ok(self
            .find_edgetable(edge_id)
            .ok_or(PgModelError::EdgeNotFound(*edge_id))?)
    }

    pub(crate) fn datatable_key_by_def_key(
        &self,
        def_key: PgRegKey,
    ) -> DomainResult<(DomainIndex, DefId)> {
        let Some(PgTableKey::Data {
            domain_index,
            def_id,
        }) = self.reg_key_to_table_key.get(&def_key)
        else {
            return Err(PgModelError::NotFoundInRegistry(def_key).into());
        };
        Ok((*domain_index, *def_id))
    }
}

/// Something belonging to a specific persisted domain
#[derive(Clone, Copy)]
pub struct InDomain<T> {
    pub domain_index: DomainIndex,
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
            domain_index: value.type_def_id().domain_index(),
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
    pub properties: FnvHashMap<DefPropTag, PgProperty>,
    pub edge_cardinals: BTreeMap<CardinalIdx, PgEdgeCardinal>,
    pub property_indexes: FnvHashMap<(DefId, PgIndexType), PgIndexData>,
}

impl PgDomain {
    pub fn get_table(&self, id: &PgTableIdUnion) -> Option<&PgTable> {
        match id {
            PgTableIdUnion::Def(def_id) => self.datatables.get(def_id),
            PgTableIdUnion::Edge(edge_id) => self.edgetables.get(&edge_id.def_id().1),
        }
    }
}

impl PgTable {
    pub fn find_column(&self, prop_id: &PropId) -> Option<&PgColumn> {
        self.properties
            .get(&prop_id.tag())
            .and_then(PgProperty::as_column)
    }

    pub fn find_abstract_property(&self, prop_id: &PropId) -> Option<PgRegKey> {
        match self.properties.get(&prop_id.tag()) {
            Some(PgProperty::Abstract(reg_key)) => Some(*reg_key),
            Some(PgProperty::Column(_)) | None => None,
        }
    }

    pub fn column(&self, prop_id: &PropId) -> DomainResult<&PgColumn> {
        self.find_column(prop_id).ok_or_else(|| {
            debug!("field not found in {:?}", self.properties);

            PgModelError::PropertyNotFound(self.table_name.clone(), *prop_id).into()
        })
    }

    pub fn abstract_property(&self, prop_id: &PropId) -> DomainResult<PgRegKey> {
        Ok(self
            .find_abstract_property(prop_id)
            .ok_or_else(|| PgModelError::PropertyNotFound(self.table_name.clone(), *prop_id))?)
    }

    pub fn column_by_key(&self, key: PgRegKey) -> Option<&PgColumn> {
        self.properties
            .values()
            .filter_map(PgProperty::as_column)
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
    pub property_keys: Vec<PgRegKey>,
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
pub enum PgProperty {
    Column(PgColumn),
    Abstract(PgRegKey),
}

#[derive(Debug)]
pub enum PgPropertyData {
    Scalar { col_name: Box<str>, pg_type: PgType },
    Abstract,
}

impl PgProperty {
    pub fn as_column(&self) -> Option<&PgColumn> {
        match self {
            Self::Column(column) => Some(column),
            Self::Abstract(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PgColumn {
    pub key: PgRegKey,
    pub col_name: Box<str>,
    pub pg_type: PgType,
}

#[derive(Clone, Debug)]
pub struct PgEdgeCardinal {
    #[expect(unused)]
    pub key: PgRegKey,
    #[expect(unused)]
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
        pinned_def_id: DefId,
        key_col_name: Box<str>,
    },
    Parameters(DefId),
}

/// How PG will represent an inherent property
#[derive(Debug)]
pub enum PgRepr {
    /// Something that has only one possible value, and therefore doesn't need storage
    Unit,
    /// A scalar-like data field that can be stored in a column
    Scalar(PgType, OntolDefTag),
    /// Something that will be stored in an abstracted manner in "child table"
    Abstract,
    /// PG can't represent it (yet?)
    NotSupported(&'static str),
}

impl PgRepr {
    pub fn classify(def_id: DefId, ontology: &Ontology) -> Self {
        let def = ontology.get_def(def_id).unwrap();
        let def_repr = match &def.kind {
            DefKind::Data(basic_def) => &basic_def.repr,
            _ => &DefRepr::Unknown,
        };

        Self::classify_def_repr(def_repr, ontology)
    }

    pub fn classify_def_repr(def_repr: &DefRepr, ontology: &Ontology) -> Self {
        match def_repr {
            DefRepr::Unit => Self::Unit,
            DefRepr::I64 => Self::Scalar(PgType::BigInt, OntolDefTag::I64),
            DefRepr::F64 => Self::Scalar(PgType::DoublePrecision, OntolDefTag::F64),
            DefRepr::Serial => Self::Scalar(PgType::Bigserial, OntolDefTag::Serial),
            DefRepr::Boolean => Self::Scalar(PgType::Boolean, OntolDefTag::Boolean),
            DefRepr::Text => Self::Scalar(PgType::Text, OntolDefTag::Text),
            DefRepr::TextConstant(_) => Self::Unit,
            DefRepr::Octets => Self::Scalar(PgType::Bytea, OntolDefTag::Octets),
            DefRepr::DateTime => Self::Scalar(PgType::TimestampTz, OntolDefTag::DateTime),
            DefRepr::FmtStruct(Some((_prop_id, def_id))) => Self::classify(*def_id, ontology),
            DefRepr::FmtStruct(None) => Self::Unit,
            DefRepr::Seq => todo!("seq"),
            DefRepr::Struct => Self::Abstract,
            DefRepr::Intersection(_) => Self::NotSupported("intersection"),
            DefRepr::Union(_variants, bound) => match bound {
                DefReprUnionBound::Scalar(scalar_repr) => {
                    Self::classify_def_repr(scalar_repr, ontology)
                }
                _ => Self::Abstract,
            },
            DefRepr::Macro => Self::Unit,
            DefRepr::Vertex => Self::NotSupported("vertex"),
            DefRepr::Unknown => Self::NotSupported("unknown"),
        }
    }

    pub fn classify_opt_def_repr(def_repr: Option<&DefRepr>, ontology: &Ontology) -> Option<Self> {
        def_repr.map(|r| Self::classify_def_repr(r, ontology))
    }
}

#[derive(Clone, Copy, PartialEq, ToSql, FromSql, Debug)]
#[postgres(name = "m6m_pg_domaintable_type")]
pub enum PgDomainTableType {
    #[postgres(name = "vertex")]
    Vertex,
    #[postgres(name = "edge")]
    Edge,
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
    pub fn insert_default(&self) -> bool {
        matches!(self, Self::Bigserial)
    }
}

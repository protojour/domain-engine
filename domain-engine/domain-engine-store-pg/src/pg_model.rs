use std::{collections::BTreeMap, ops::Deref};

use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    debug::OntolDebug,
    ontology::{
        aspects::DefsAspect,
        domain::{
            DataRelationshipInfo, DataRelationshipKind, DataTreeRepr, Def, DefKind, DefRepr,
            DefReprUnionBound,
        },
        ontol::ValueGenerator,
    },
    tuple::CardinalIdx,
    value::Value,
    DefId, DefPropTag, DomainIndex, OntolDefTag, PropId,
};
use postgres_types::ToSql;
use tokio_postgres::types::FromSql;
use tracing::{debug, trace};

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
    pub fn new(
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

    pub fn stable_property_index(&self, prop_id: PropId) -> Option<u32> {
        let def_id = prop_id.0;
        let pg_domain = self.domains.get(&def_id.0)?;
        let pg_table = pg_domain.datatables.get(&def_id)?;
        let pg_property = pg_table.properties.get(&prop_id.1)?;

        match pg_property {
            PgProperty::Column(pg_column) => pg_column.key.try_into().ok(),
            PgProperty::AbstractStruct(_) => None,
            PgProperty::AbstractCrdt(_) => None,
        }
    }

    pub fn pg_domain(&self, domain_index: DomainIndex) -> DomainResult<&PgDomain> {
        Ok(self
            .domains
            .get(&domain_index)
            .ok_or(PgModelError::DomainNotFound(domain_index))?)
    }

    pub fn pg_domain_datatable(
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

    pub fn pg_domain_edgetable(&self, edge_id: &EdgeId) -> DomainResult<PgDomainTable> {
        let domain = self.pg_domain(edge_id.domain_index())?;
        let datatable = self.edgetable(edge_id)?;
        Ok(PgDomainTable {
            domain,
            table: datatable,
        })
    }

    pub fn find_datatable(&self, domain_index: DomainIndex, def_id: DefId) -> Option<&PgTable> {
        self.domains
            .get(&domain_index)
            .and_then(|pg_domain| pg_domain.datatables.get(&def_id))
    }

    pub fn datatable(&self, domain_index: DomainIndex, def_id: DefId) -> DomainResult<&PgTable> {
        Ok(self
            .find_datatable(domain_index, def_id)
            .ok_or(PgModelError::CollectionNotFound(domain_index, def_id))?)
    }

    pub fn find_edgetable(&self, edge_id: &EdgeId) -> Option<&PgTable> {
        self.domains
            .get(&edge_id.domain_index())
            .and_then(|pg_domain| pg_domain.edgetables.get(&edge_id.def_id().1))
    }

    pub fn edgetable(&self, edge_id: &EdgeId) -> DomainResult<&PgTable> {
        Ok(self
            .find_edgetable(edge_id)
            .ok_or(PgModelError::EdgeNotFound(*edge_id))?)
    }

    pub fn datatable_key_by_def_key(
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
#[repr(u32)]
pub enum RegVersion {
    Init = 1,
    Crdt = 2,
}

impl RegVersion {
    pub const fn current() -> Self {
        Self::Crdt
    }
}

impl TryFrom<u32> for RegVersion {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Init),
            2 => Ok(Self::Crdt),
            other => Err(other),
        }
    }
}

#[derive(Clone)]
pub struct PgDomain {
    pub key: Option<PgRegKey>,
    pub schema_name: Box<str>,
    pub datatables: FnvHashMap<DefId, PgTable>,
    pub edgetables: FnvHashMap<u16, PgTable>,
    pub has_crdt: bool,
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
    pub fn find_property_ref(&self, prop_id: &PropId) -> Option<PgPropertyRef<'_>> {
        if prop_id.0.domain_index() != DomainIndex::ontol() {
            self.properties
                .get(&prop_id.tag())
                .map(|prop| prop.as_ref())
        } else if prop_id == &OntolDefTag::RelationDataStoreAddress.prop_id_0() {
            Some(PgPropertyRef::Column(PgColumnRef {
                key: -1,
                col_name: "_key",
                pg_type: PgType::Bigserial,
                standard: true,
            }))
        } else if prop_id == &OntolDefTag::CreateTime.prop_id_0() {
            Some(PgPropertyRef::Column(PgColumnRef {
                key: -1,
                col_name: "_created",
                pg_type: PgType::TimestampTz,
                standard: true,
            }))
        } else if prop_id == &OntolDefTag::UpdateTime.prop_id_0() {
            Some(PgPropertyRef::Column(PgColumnRef {
                key: -1,
                col_name: "_updated",
                pg_type: PgType::TimestampTz,
                standard: true,
            }))
        } else {
            None
        }
    }

    pub fn find_column(&self, prop_id: &PropId) -> Option<PgColumnRef> {
        self.find_property_ref(prop_id)
            .and_then(|p_ref| p_ref.as_column())
    }

    pub fn find_abstract_property(&self, prop_id: &PropId) -> Option<PgRegKey> {
        match self.properties.get(&prop_id.tag()) {
            Some(PgProperty::AbstractStruct(reg_key)) => Some(*reg_key),
            Some(PgProperty::AbstractCrdt(reg_key)) => Some(*reg_key),
            Some(PgProperty::Column(_)) | None => None,
        }
    }

    pub fn column(&self, prop_id: &PropId) -> DomainResult<PgColumnRef> {
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

    pub fn column_by_key(&self, key: PgRegKey) -> Option<PgColumnRef> {
        self.properties
            .values()
            .map(PgProperty::as_ref)
            .filter_map(PgPropertyRef::as_column)
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
    AbstractStruct(PgRegKey),
    AbstractCrdt(PgRegKey),
}

#[derive(Clone, Copy)]
pub enum PgPropertyRef<'a> {
    Column(PgColumnRef<'a>),
    Abstract(PgRegKey),
}

#[derive(Debug)]
pub enum PgPropertyData {
    Scalar { col_name: Box<str>, pg_type: PgType },
    AbstractStruct,
    AbstractCrdt,
}

impl PgProperty {
    pub fn as_ref(&self) -> PgPropertyRef {
        match self {
            Self::Column(column) => PgPropertyRef::Column(PgColumnRef {
                key: column.key,
                col_name: &column.col_name,
                pg_type: column.pg_type,
                standard: false,
            }),
            Self::AbstractStruct(reg_key) => PgPropertyRef::Abstract(*reg_key),
            Self::AbstractCrdt(reg_key) => PgPropertyRef::Abstract(*reg_key),
        }
    }
}

impl<'a> PgPropertyRef<'a> {
    pub fn as_column(self) -> Option<PgColumnRef<'a>> {
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

#[derive(Clone, Copy)]
pub struct PgColumnRef<'a> {
    pub key: PgRegKey,
    pub col_name: &'a str,
    pub pg_type: PgType,
    pub standard: bool,
}

#[derive(Clone, Debug)]
pub struct PgEdgeCardinal {
    pub key: PgRegKey,
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
    CreatedAtColumn,
    UpdatedAtColumn,
    /// Something that will be stored in an abstracted manner in "child table"
    Abstract(DataTreeRepr),
    /// PG can't represent it (yet?)
    NotSupported(&'static str),
}

impl PgRepr {
    pub fn classify_property(
        rel_info: &DataRelationshipInfo,
        def_id: DefId,
        ontology_defs: &DefsAspect,
    ) -> Self {
        trace!(rel_info = ?rel_info.debug(ontology_defs), "classify property");

        match rel_info.generator {
            Some(ValueGenerator::CreatedAtTime) => Self::CreatedAtColumn,
            Some(ValueGenerator::UpdatedAtTime) => Self::UpdatedAtColumn,
            _ => match Self::classify_def_id(def_id, ontology_defs) {
                Self::Abstract(protocol) => match rel_info.kind {
                    DataRelationshipKind::Tree(tree_repr) => Self::Abstract(tree_repr),
                    _ => Self::Abstract(protocol),
                },
                other => other,
            },
        }
    }

    pub fn classify_opt_def_repr(
        def_repr: Option<&DefRepr>,
        ontology_defs: &DefsAspect,
    ) -> Option<Self> {
        def_repr.map(|r| Self::classify_def_repr(r, ontology_defs))
    }

    fn classify_def_id(def_id: DefId, ontology_defs: &DefsAspect) -> Self {
        let def = ontology_defs.def(def_id);
        let def_repr = match &def.kind {
            DefKind::Data(basic_def) => &basic_def.repr,
            _ => &DefRepr::Unknown,
        };

        Self::classify_def_repr(def_repr, ontology_defs)
    }

    fn classify_def_repr(def_repr: &DefRepr, ontology_defs: &DefsAspect) -> Self {
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
            DefRepr::FmtStruct(Some((_prop_id, def_id))) => {
                Self::classify_def_id(*def_id, ontology_defs)
            }
            DefRepr::FmtStruct(None) => Self::Unit,
            DefRepr::Seq => todo!("seq"),
            DefRepr::Struct => Self::Abstract(DataTreeRepr::Plain),
            DefRepr::Intersection(_) => Self::NotSupported("intersection"),
            DefRepr::Union(_variants, bound) => match bound {
                DefReprUnionBound::Scalar(scalar_repr) => {
                    Self::classify_def_repr(scalar_repr, ontology_defs)
                }
                _ => Self::Abstract(DataTreeRepr::Plain),
            },
            DefRepr::Macro => Self::Unit,
            DefRepr::Vertex => Self::NotSupported("vertex"),
            DefRepr::Unknown => Self::NotSupported("unknown"),
        }
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

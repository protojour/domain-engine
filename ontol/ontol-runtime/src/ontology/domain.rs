//! Domain description model, part of the Ontology

use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_core::{OntologyDomainId, TopologyGeneration, vec_map::VecMap};
use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};
use thin_vec::ThinVec;

use crate::{
    DefId, DefIdSet, DefTag, FnvIndexMap, PropId, impl_ontol_debug,
    interface::serde::operator::SerdeOperatorAddr, property::Cardinality, query::order::Direction,
    tuple::CardinalIdx,
};

use super::{
    Ontology,
    ontol::{TextConstant, ValueGenerator},
};

/// A domain in the ONTOL ontology.
#[derive(Serialize, Deserialize)]
pub struct Domain {
    domain_id: OntologyDomainId,
    def_id: DefId,

    unique_name: TextConstant,
    generation: TopologyGeneration,

    /// defs by tag (the type's index within the domain)
    persistent_defs: VecMap<DefTag, Def>,
    transient_defs: VecMap<DefTag, Def>,
}

impl Domain {
    pub fn new(
        domain_id: OntologyDomainId,
        def_id: DefId,
        unique_name: TextConstant,
        generation: TopologyGeneration,
    ) -> Self {
        Self {
            domain_id,
            def_id,
            unique_name,
            generation,
            persistent_defs: Default::default(),
            transient_defs: Default::default(),
        }
    }

    pub fn domain_id(&self) -> &OntologyDomainId {
        &self.domain_id
    }

    /// The DefId of the domain itself
    pub fn def_id(&self) -> DefId {
        self.def_id
    }

    pub fn unique_name(&self) -> TextConstant {
        self.unique_name
    }

    pub fn topology_generation(&self) -> TopologyGeneration {
        self.generation
    }

    pub fn def_count(&self) -> usize {
        self.persistent_defs.len()
    }

    #[inline]
    pub fn def(&self, def_id: DefId) -> &Def {
        self.def_option(def_id)
            .unwrap_or_else(|| panic!("{def_id:?} is not in the ontology"))
    }

    #[inline]
    pub fn def_option(&self, def_id: DefId) -> Option<&Def> {
        if def_id.is_persistent() {
            self.persistent_defs.get(&DefTag(def_id.tag()))
        } else {
            self.transient_defs.get(&DefTag(def_id.tag()))
        }
    }

    pub fn defs(&self) -> impl Iterator<Item = &Def> {
        self.persistent_defs
            .iter()
            .map(|(_idx, def)| def)
            .chain(self.transient_defs.iter().map(|(_idx, def)| def))
    }

    pub fn edges(&self) -> impl Iterator<Item = (&DefId, &EdgeInfo)> {
        self.defs().filter_map(|def| {
            if let DefKind::Edge(edge_info) = &def.kind {
                Some((&def.id, edge_info))
            } else {
                None
            }
        })
    }

    pub fn find_edge(&self, id: DefId) -> Option<&EdgeInfo> {
        let def = self.def(id);
        if let DefKind::Edge(edge_info) = &def.kind {
            Some(edge_info)
        } else {
            None
        }
    }

    pub fn find_def_by_name(&self, name: TextConstant) -> Option<&Def> {
        self.defs().find(|info| info.ident() == Some(name))
    }

    pub fn add_def(&mut self, info: Def) {
        self.register_def(info);
    }

    fn register_def(&mut self, def: Def) {
        if def.id.is_persistent() {
            self.persistent_defs.insert(DefTag(def.id.tag()), def);
        } else {
            self.transient_defs.insert(DefTag(def.id.tag()), def);
        }
    }
}

/// Metadata about any definition inside a domain
#[derive(Clone, Serialize, Deserialize)]
pub struct Def {
    pub id: DefId,
    pub kind: DefKind,
    pub public: bool,

    /// The SerdeOperatorAddr used for JSON.
    /// FIXME: This should really be connected to a DomainInterface.
    pub operator_addr: Option<SerdeOperatorAddr>,
    pub store_key: Option<TextConstant>,

    /// FIXME: These should be called "properties", because that's what they model?
    /// FIXME: This should use a VecMap with K=DefPropTag
    /// when foreign properties are fully banned.
    /// (https://gitlab.com/protojour/memoriam/domain-engine/-/issues/142)
    pub data_relationships: FnvIndexMap<PropId, DataRelationshipInfo>,
}

impl Def {
    pub fn ident(&self) -> Option<TextConstant> {
        match &self.kind {
            DefKind::Entity(info) => Some(info.ident),
            DefKind::Edge(edge_info) => Some(edge_info.ident),
            DefKind::Data(info)
            | DefKind::Relation(info)
            | DefKind::Function(info)
            | DefKind::Domain(info)
            | DefKind::Generator(info) => info.ident,
        }
    }

    /// Returns Some if this Def represents an entity
    pub fn entity(&self) -> Option<&Entity> {
        match &self.kind {
            DefKind::Entity(entity) => Some(entity),
            _ => None,
        }
    }

    pub fn repr(&self) -> Option<&DefRepr> {
        match &self.kind {
            DefKind::Entity(_) => Some(&DefRepr::Struct),
            DefKind::Data(basic_def) => Some(&basic_def.repr),
            _ => None,
        }
    }

    pub fn edge_relationships(
        &self,
    ) -> impl Iterator<Item = (&PropId, &DataRelationshipInfo, EdgeCardinalProjection)> {
        self.data_relationships
            .iter()
            .filter_map(|(rel_id, info)| match info.kind {
                DataRelationshipKind::Edge(projection) => Some((rel_id, info, projection)),
                _ => None,
            })
    }

    pub fn data_relationship_by_name(
        &self,
        name: &str,
        ontology: &Ontology,
    ) -> Option<(PropId, &DataRelationshipInfo)> {
        self.data_relationships
            .iter()
            .find(|(_, rel)| &ontology[rel.name] == name)
            .map(|(id, rel)| (*id, rel))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum DefKind {
    Entity(Entity),
    Data(BasicDef),
    Edge(EdgeInfo),
    Relation(BasicDef),
    Function(BasicDef),
    Domain(BasicDef),
    Generator(BasicDef),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BasicDef {
    pub ident: Option<TextConstant>,
    pub repr: DefRepr,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DefRepr {
    Unit,
    I64,
    F64,
    Serial,
    Boolean,
    Text,
    TextConstant(TextConstant),
    Octets,
    DateTime,
    Seq,
    Struct,
    Intersection(ThinVec<DefId>),
    Union(ThinVec<DefId>, DefReprUnionBound),
    /// FIXME: not sure if this should exist as a separate DefRepr:
    /// It exists as a simplification for now:
    FmtStruct(Option<(PropId, DefId)>),
    Macro,
    Vertex,
    Unknown,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DefReprUnionBound {
    Any,
    Scalar(Box<DefRepr>),
    Struct,
    Fmt,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Entity {
    pub ident: TextConstant,
    pub id_prop: PropId,
    pub id_value_def_id: DefId,
    pub id_operator_addr: SerdeOperatorAddr,
    /// Whether all inherent fields are part of the primary id of this entity.
    /// In other words: The entity has only one field, its ID.
    pub is_self_identifying: bool,
    pub id_value_generator: Option<ValueGenerator>,
}

/// Data relationships for a type in the domain
#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct DataRelationshipInfo {
    pub name: TextConstant,
    pub kind: DataRelationshipKind,
    pub cardinality: Cardinality,
    pub source: DataRelationshipSource,
    pub target: DataRelationshipTarget,
    pub generator: Option<ValueGenerator>,
}

impl DataRelationshipInfo {
    pub fn edge_kind(&self) -> Option<&EdgeCardinalProjection> {
        if let DataRelationshipKind::Edge(projection) = &self.kind {
            Some(projection)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug, Debug)]
pub enum DataRelationshipKind {
    /// The relationship is between an entity and its identifier
    Id,
    /// A Tree data relationship that can never be circular.
    /// It expresses a simple composition of a composite (the parent) and the component (the child).
    Tree(DataTreeRepr),
    /// Graph data relationships can be circular and involves entities.
    /// The Graph relationship kind must go from one entity to another entity.
    Edge(EdgeCardinalProjection),
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug, Debug)]
pub enum DataTreeRepr {
    Plain,
    Crdt,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug, Debug)]
pub enum DataRelationshipTarget {
    Unambiguous(DefId),
    /// The target is a union of types, only known during runtime.
    /// The union's variants are accessed using [Ontology::union_variants].
    Union(DefId),
}

impl DataRelationshipTarget {
    pub fn def_id(&self) -> DefId {
        match self {
            Self::Unambiguous(def_id) => *def_id,
            Self::Union(def_id) => *def_id,
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ExtendedEntityInfo {
    pub order_union: Option<DefId>,

    /// The `order` definitions on the entity.
    pub order_table: FnvHashMap<DefId, VertexOrder>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct VertexOrder {
    pub tuple: Box<[FieldPath]>,
    pub direction: Direction,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FieldPath(pub Box<[PropId]>);

#[derive(Clone, Serialize, Deserialize)]
pub struct EdgeInfo {
    pub ident: TextConstant,

    /// The cardinals represents a template for how to represent the edge.
    /// The length of the cardinals is the edge's cardinality.
    pub cardinals: Vec<EdgeCardinal>,

    /// Custom name for this edge, for naming in data stores
    pub store_key: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EdgeCardinal {
    /// The target type of this cardinal
    pub target: DefIdSet,

    pub flags: EdgeCardinalFlags,
}

impl EdgeCardinal {
    pub fn is_one_to_one(&self) -> bool {
        self.flags.contains(EdgeCardinalFlags::PINNED_DEF)
    }

    pub fn is_entity(&self) -> bool {
        self.flags.contains(EdgeCardinalFlags::ENTITY)
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize, Debug)]
    pub struct EdgeCardinalFlags: u8 {
        /// Whether the values in this edge cardinal references an entity
        const ENTITY = 0b00000001;
        /// Whether each linked vertex can only appear once in this cardinal for this edge.
        const UNIQUE = 0b00000010;
        /// Whether the link represented by this cardinal can only
        /// ever link to just one entity/vertex def.
        /// The reason can be that parts of the edge represents direct properties in the vertex.
        /// Implies UNIQUE.
        const PINNED_DEF = 0b00000100;
    }
}

/// An edge cardinal projection.
///
/// It is used to model the the projection of hyper-edges as properties.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct EdgeCardinalProjection {
    /// The edge id that is the source of this data point
    pub edge_id: DefId,

    /// The object cardinal of the projection, the attribute value.
    pub object: CardinalIdx,

    /// The subject cardinal of the projection.
    /// This is the origin: The value that owns the property.
    pub subject: CardinalIdx,

    /// Indicates that the subject has a one-to-one relationship with the hyperedge itself,
    /// i.e. they are necessarily coexistent
    pub pinned: bool,
}

impl EdgeCardinalProjection {
    pub fn proj(&self) -> (u8, u8) {
        (self.subject.0, self.object.0)
    }
}

impl Debug for EdgeCardinalProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}<-{}",
            self.edge_id.domain_index().index(),
            self.edge_id.tag(),
            self.object,
            self.subject,
        )
    }
}

impl_ontol_debug!(EdgeCardinalProjection);

//! Domain description model, part of the Ontology

use std::{collections::BTreeMap, fmt::Debug};

use fnv::FnvHashMap;
use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

use crate::{
    impl_ontol_debug, interface::serde::operator::SerdeOperatorAddr, property::Cardinality,
    query::order::Direction, tuple::CardinalIdx, DefId, EdgeId, RelationshipId,
};

use super::ontol::{TextConstant, ValueGenerator};

/// A domain in the ONTOL ontology.
#[derive(Serialize, Deserialize)]
pub struct Domain {
    def_id: DefId,

    unique_name: TextConstant,

    /// Types by DefId.1 (the type's index within the domain)
    types: Vec<Def>,

    edges: BTreeMap<EdgeId, EdgeInfo>,
}

impl Domain {
    pub fn new(def_id: DefId, unique_name: TextConstant) -> Self {
        Self {
            def_id,
            unique_name,
            types: Default::default(),
            edges: Default::default(),
        }
    }

    pub fn def_id(&self) -> DefId {
        self.def_id
    }

    pub fn unique_name(&self) -> TextConstant {
        self.unique_name
    }

    pub fn type_count(&self) -> usize {
        self.types.len()
    }

    pub fn def(&self, def_id: DefId) -> &Def {
        &self.types[def_id.1 as usize]
    }

    pub fn def_option(&self, def_id: DefId) -> Option<&Def> {
        self.types.get(def_id.1 as usize)
    }

    pub fn defs(&self) -> impl Iterator<Item = &Def> {
        self.types.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = (&EdgeId, &EdgeInfo)> {
        self.edges.iter()
    }

    pub fn find_edge(&self, id: EdgeId) -> Option<&EdgeInfo> {
        self.edges.get(&id)
    }

    pub fn find_def_by_name(&self, name: TextConstant) -> Option<&Def> {
        self.types.iter().find(|info| info.name() == Some(name))
    }

    pub fn add_def(&mut self, info: Def) {
        self.register_def(info);
    }

    fn register_def(&mut self, info: Def) {
        let index = info.id.1 as usize;

        // pad the vector
        let new_size = std::cmp::max(self.types.len(), index + 1);
        self.types.resize_with(new_size, || Def {
            id: DefId(info.id.0, 0),
            public: false,
            kind: DefKind::Data(BasicDef { name: None }),
            store_key: None,
            operator_addr: None,
            data_relationships: Default::default(),
        });

        self.types[index] = info;
    }

    pub fn set_edges(&mut self, edges: impl IntoIterator<Item = (EdgeId, EdgeInfo)>) {
        self.edges = edges.into_iter().collect();
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
    pub data_relationships: FnvHashMap<RelationshipId, DataRelationshipInfo>,
}

impl Def {
    pub fn name(&self) -> Option<TextConstant> {
        match &self.kind {
            DefKind::Entity(info) => Some(info.name),
            DefKind::Data(info)
            | DefKind::Relationship(info)
            | DefKind::Function(info)
            | DefKind::Domain(info)
            | DefKind::Generator(info) => info.name,
        }
    }

    /// Returns Some if this Def represents an entity
    pub fn entity(&self) -> Option<&Entity> {
        match &self.kind {
            DefKind::Entity(entity) => Some(entity),
            _ => None,
        }
    }

    pub fn edge_relationships(
        &self,
    ) -> impl Iterator<
        Item = (
            &RelationshipId,
            &DataRelationshipInfo,
            EdgeCardinalProjection,
        ),
    > {
        self.data_relationships
            .iter()
            .filter_map(|(rel_id, info)| match info.kind {
                DataRelationshipKind::Edge(projection) => Some((rel_id, info, projection)),
                _ => None,
            })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum DefKind {
    Entity(Entity),
    Data(BasicDef),
    Relationship(BasicDef),
    Function(BasicDef),
    Domain(BasicDef),
    Generator(BasicDef),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct BasicDef {
    pub name: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Entity {
    pub name: TextConstant,
    pub id_relationship_id: RelationshipId,
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
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipKind {
    /// The relationship is between an entity and its identifier
    Id,
    /// A Tree data relationship that can never be circular.
    /// It expresses a simple composition of a composite (the parent) and the component (the child).
    Tree,
    /// Graph data relationships can be circular and involves entities.
    /// The Graph relationship kind must go from one entity to another entity.
    Edge(EdgeCardinalProjection),
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
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
    pub order_table: FnvHashMap<DefId, EntityOrder>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityOrder {
    pub tuple: Box<[FieldPath]>,
    pub direction: Direction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldPath(pub Box<[RelationshipId]>);

#[derive(Serialize, Deserialize)]
pub struct EdgeInfo {
    /// The cardinals represents a template for how to represent the edge.
    /// The length of the cardinals is the edge's cardinality.
    pub cardinals: Vec<EdgeCardinal>,

    /// Custom name for this edge, for naming in data stores
    pub store_key: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EdgeCardinal {
    /// The target type of this cardinal
    pub target: DataRelationshipTarget,

    /// The cardinality of the _data point_ of this cardinal
    pub cardinality: Cardinality,

    pub is_entity: bool,
}

/// An edge cardinal projection.
///
/// It is used to model the the projection of hyper-edges as properties.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct EdgeCardinalProjection {
    /// The edge id that is the source of this data point
    pub id: EdgeId,

    /// The object cardinal of the projection, the attribute value.
    pub object: CardinalIdx,

    /// The subject cardinal of the projection.
    /// This is the origin: The value that owns the property.
    pub subject: CardinalIdx,
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
            self.id.0 .0 .0, self.id.0 .1, self.object, self.subject,
        )
    }
}

impl_ontol_debug!(EdgeCardinalProjection);

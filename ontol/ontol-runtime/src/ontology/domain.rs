//! Domain description model, part of the Ontology

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use fnv::FnvHashMap;
use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

use crate::{
    format_utils::AsAlpha,
    impl_ontol_debug,
    interface::serde::operator::SerdeOperatorAddr,
    property::{Cardinality, PropertyId},
    query::order::Direction,
    DefId, EdgeId, RelationshipId,
};

use super::ontol::{TextConstant, ValueGenerator};

/// A domain in the ONTOL ontology.
#[derive(Serialize, Deserialize)]
pub struct Domain {
    def_id: DefId,

    unique_name: TextConstant,

    /// Types by DefId.1 (the type's index within the domain)
    types: Vec<TypeInfo>,

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

    pub fn type_info(&self, def_id: DefId) -> &TypeInfo {
        &self.types[def_id.1 as usize]
    }

    pub fn type_info_option(&self, def_id: DefId) -> Option<&TypeInfo> {
        self.types.get(def_id.1 as usize)
    }

    pub fn type_infos(&self) -> impl Iterator<Item = &TypeInfo> {
        self.types.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = (&EdgeId, &EdgeInfo)> {
        self.edges.iter()
    }

    pub fn find_edge(&self, id: EdgeId) -> Option<&EdgeInfo> {
        self.edges.get(&id)
    }

    pub fn find_type_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.types
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }

    pub fn add_type(&mut self, type_info: TypeInfo) {
        self.register_type_info(type_info);
    }

    fn register_type_info(&mut self, type_info: TypeInfo) {
        let index = type_info.def_id.1 as usize;

        // pad the vector
        let new_size = std::cmp::max(self.types.len(), index + 1);
        self.types.resize_with(new_size, || TypeInfo {
            def_id: DefId(type_info.def_id.0, 0),
            public: false,
            kind: TypeKind::Data(BasicTypeInfo { name: None }),
            store_key: None,
            operator_addr: None,
            data_relationships: Default::default(),
        });

        self.types[index] = type_info;
    }

    pub fn find_type_info_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.types
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }

    pub fn set_edges(&mut self, edges: impl IntoIterator<Item = (EdgeId, EdgeInfo)>) {
        self.edges = edges.into_iter().collect();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    pub def_id: DefId,
    pub kind: TypeKind,
    pub public: bool,
    /// The SerdeOperatorAddr used for JSON.
    /// FIXME: This should really be connected to a DomainInterface.
    pub operator_addr: Option<SerdeOperatorAddr>,
    pub store_key: Option<TextConstant>,
    pub data_relationships: FnvHashMap<PropertyId, DataRelationshipInfo>,
}

impl TypeInfo {
    pub fn name(&self) -> Option<TextConstant> {
        match &self.kind {
            TypeKind::Entity(info) => Some(info.name),
            TypeKind::Data(info)
            | TypeKind::Relationship(info)
            | TypeKind::Function(info)
            | TypeKind::Domain(info)
            | TypeKind::Generator(info) => info.name,
        }
    }

    pub fn entity_info(&self) -> Option<&EntityInfo> {
        match &self.kind {
            TypeKind::Entity(entity_info) => Some(entity_info),
            _ => None,
        }
    }

    pub fn edge_relationships(
        &self,
    ) -> impl Iterator<Item = (&PropertyId, &DataRelationshipInfo, EdgeCardinalId)> {
        self.data_relationships
            .iter()
            .filter_map(|(prop_id, info)| match info.kind {
                DataRelationshipKind::Edge(edge_cardinal) => Some((prop_id, info, edge_cardinal)),
                _ => None,
            })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TypeKind {
    Entity(EntityInfo),
    Data(BasicTypeInfo),
    Relationship(BasicTypeInfo),
    Function(BasicTypeInfo),
    Domain(BasicTypeInfo),
    Generator(BasicTypeInfo),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct BasicTypeInfo {
    pub name: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityInfo {
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
    Edge(EdgeCardinalId),
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
pub struct FieldPath(pub Box<[PropertyId]>);

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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CardinalIdx(pub u8);

impl_ontol_debug!(CardinalIdx);

impl Debug for CardinalIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", AsAlpha(self.0 as u32, 'A'))
    }
}

impl Display for CardinalIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", AsAlpha(self.0 as u32, 'A'))
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct EdgeCardinalId {
    /// The edge id that is the source of this data point
    pub id: EdgeId,

    /// The cardinal index of this data point within the edge
    pub cardinal_idx: CardinalIdx,
}

impl Debug for EdgeCardinalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.id.0 .0 .0, self.id.0 .1, self.cardinal_idx,
        )
    }
}

impl_ontol_debug!(EdgeCardinalId);

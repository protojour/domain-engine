use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};
use thin_vec::ThinVec;

use crate::{
    interface::serde::operator::SerdeOperatorAddr,
    ontology::{ontol::TextConstant, Ontology, OntologyInit},
    phf::PhfIndexMap,
    var::Var,
    DefId, MapKey, RelationshipId,
};

use super::argument::{self};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct TypeAddr(pub u32);

#[derive(Clone, Copy, Default, Serialize, Deserialize, Debug)]
pub enum Optionality {
    #[default]
    Mandatory,
    Optional,
}

impl Optionality {
    pub fn from_optional(optional: bool) -> Self {
        if optional {
            Self::Optional
        } else {
            Self::Mandatory
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum TypeModifier {
    Unit(Optionality),
    Array {
        array: Optionality,
        element: Optionality,
    },
}

impl TypeModifier {
    pub fn new_unit(optionality: Optionality) -> Self {
        TypeModifier::Unit(optionality)
    }

    pub fn unit_optionality(&self) -> Optionality {
        match self {
            Self::Unit(unit) => *unit,
            Self::Array { element, .. } => *element,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum UnitTypeRef {
    Addr(TypeAddr),
    NativeScalar(NativeScalarRef),
}

impl UnitTypeRef {
    pub fn unwrap_addr(&self) -> TypeAddr {
        match self {
            Self::Addr(addr) => *addr,
            Self::NativeScalar(_) => panic!("Cannot get addr from native scalar"),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct NativeScalarRef {
    pub operator_addr: SerdeOperatorAddr,
    pub kind: NativeScalarKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum NativeScalarKind {
    Unit,
    Boolean,
    Int(DefId),
    Number(DefId),
    String,
    ID,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct TypeRef {
    pub modifier: TypeModifier,
    pub unit: UnitTypeRef,
}

impl TypeRef {
    pub fn mandatory(unit: UnitTypeRef) -> Self {
        Self {
            modifier: TypeModifier::Unit(Optionality::Mandatory),
            unit,
        }
    }

    pub fn optional(unit: UnitTypeRef) -> Self {
        Self {
            modifier: TypeModifier::Unit(Optionality::Optional),
            unit,
        }
    }

    pub fn to_array(self, array_optionality: Optionality) -> Self {
        Self {
            modifier: TypeModifier::Array {
                array: array_optionality,
                element: self.modifier.unit_optionality(),
            },
            unit: self.unit,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TypeData {
    pub typename: TextConstant,
    pub input_typename: Option<TextConstant>,
    pub partial_input_typename: Option<TextConstant>,
    pub kind: TypeKind,
}

impl TypeData {
    pub fn docs(&self, ontology: &Ontology) -> Option<TextConstant> {
        match &self.kind {
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Node(nodedata),
                ..
            }) => ontology.get_docs(nodedata.def_id),
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Edge(_),
                ..
            }) => None,
            TypeKind::Object(ObjectData {
                kind: ObjectKind::PageInfo,
                ..
            }) => None,
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Connection(_),
                ..
            }) => None,
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Query { domain_def_id },
                ..
            }) => ontology.get_docs(*domain_def_id),
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Mutation,
                ..
            }) => None,
            TypeKind::Object(ObjectData {
                kind: ObjectKind::MutationResult,
                ..
            }) => None,
            TypeKind::Union(uniondata) => ontology.get_docs(uniondata.union_def_id),
            TypeKind::CustomScalar(_) => None,
        }
    }

    pub fn fields(&self) -> Option<&PhfIndexMap<FieldData>> {
        match &self.kind {
            TypeKind::Object(obj) => Some(&obj.fields),
            _ => None,
        }
    }
}

impl OntologyInit for TypeData {
    fn ontology_init(&mut self, ontology: &Ontology) {
        self.kind.ontology_init(ontology);
    }
}

#[derive(Serialize, Deserialize)]
pub enum TypeKind {
    Object(ObjectData),
    Union(UnionData),
    CustomScalar(ScalarData),
}

impl OntologyInit for TypeKind {
    fn ontology_init(&mut self, ontology: &Ontology) {
        if let Self::Object(object_data) = self {
            object_data.fields.ontology_init(ontology);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ObjectData {
    pub fields: PhfIndexMap<FieldData>,
    pub kind: ObjectKind,
    pub interface: ObjectInterface,
}

#[derive(Serialize, Deserialize)]
pub enum ObjectInterface {
    Implements(ThinVec<TypeAddr>),
    Interface,
}

#[derive(Serialize, Deserialize)]
pub enum ObjectKind {
    Node(NodeData),
    Edge(EdgeData),
    Connection(ConnectionData),
    PageInfo,
    MutationResult,
    Query { domain_def_id: DefId },
    Mutation,
}

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub def_id: DefId,
    pub entity_id: Option<DefId>,
    pub operator_addr: SerdeOperatorAddr,
}

#[derive(Serialize, Deserialize)]
pub struct UnionData {
    pub union_def_id: DefId,
    pub variants: ThinVec<TypeAddr>,
    pub operator_addr: SerdeOperatorAddr,
}

#[derive(Serialize, Deserialize)]
pub struct EdgeData {
    pub node_type_addr: TypeAddr,
    pub node_operator_addr: SerdeOperatorAddr,
    pub rel_edge_ref: Option<UnitTypeRef>,
}

#[derive(Serialize, Deserialize)]
pub struct ConnectionData {
    pub node_type_addr: TypeAddr,
}

#[derive(Serialize, Deserialize)]
pub struct ScalarData {
    pub operator_addr: SerdeOperatorAddr,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FieldData {
    pub kind: FieldKind,
    pub field_type: TypeRef,
}

impl FieldData {
    pub fn mandatory(kind: FieldKind, unit_type_ref: UnitTypeRef) -> Self {
        Self {
            kind,
            field_type: TypeRef::mandatory(unit_type_ref),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum FieldKind {
    /// A normal property of a node
    Property {
        id: RelationshipId,
        addr: SerdeOperatorAddr,
    },
    /// A flattened union property discriminator accessed through a proxy.
    /// This property is part of interfaces, since it's always present regardless of union variant.
    FlattenedPropertyDiscriminator {
        proxy: RelationshipId,
        resolvers: Box<FnvHashMap<DefId, RelationshipId>>,
    },
    /// A flattened property accessed through a "proxy"
    FlattenedProperty {
        proxy: RelationshipId,
        id: RelationshipId,
        addr: SerdeOperatorAddr,
    },
    /// The (incoming/"rel params") edge property of a node
    EdgeProperty {
        id: RelationshipId,
        addr: SerdeOperatorAddr,
    },
    /// The _id field of a node
    Id(IdPropertyData),
    /// The node inside an edge or mutation result
    Node,
    /// The edges of a connection
    Edges,
    /// The nodes of a connection
    Nodes,
    /// The page info of a connection
    PageInfo,
    /// The total count of a connection
    TotalCount,
    /// The open data of a node
    OpenData,
    /// The deleted predicate of a mutation result
    Deleted,
    /// A connection property of an entity node
    ConnectionProperty(Box<ConnectionPropertyField>),
    /// A connection to a map statement
    MapConnection(Box<MapConnectionField>),
    /// A find query from a map statement (zero or one return)
    MapFind(Box<MapFindField>),
    EntityMutation(Box<EntityMutationField>),
    Version,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConnectionPropertyField {
    pub rel_id: RelationshipId,
    pub first_arg: argument::FirstArg,
    pub after_arg: argument::AfterArg,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MapConnectionField {
    pub map_key: MapKey,
    pub queries: FnvHashMap<RelationshipId, Var>,
    pub input_arg: argument::MapInputArg,
    pub first_arg: argument::FirstArg,
    pub after_arg: argument::AfterArg,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MapFindField {
    pub map_key: MapKey,
    pub queries: FnvHashMap<RelationshipId, Var>,
    pub input_arg: argument::MapInputArg,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EntityMutationField {
    pub def_id: DefId,
    pub create_arg: Option<argument::EntityCreateInputsArg>,
    pub update_arg: Option<argument::EntityUpdateInputsArg>,
    pub delete_arg: Option<argument::EntityDeleteInputsArg>,
    pub field_unit_type_addr: TypeAddr,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct IdPropertyData {
    pub relationship_id: RelationshipId,
    pub operator_addr: SerdeOperatorAddr,
}

pub struct EntityData {
    pub type_addr: TypeAddr,
    pub node_def_id: DefId,
    pub id_def_id: DefId,
}

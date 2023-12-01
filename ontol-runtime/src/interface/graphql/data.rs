use fnv::FnvHashMap;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use smartstring::alias::String;

use crate::{
    interface::serde::operator::SerdeOperatorAddr, ontology::Ontology, value::PropertyId, var::Var,
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
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
    pub typename: String,
    pub input_typename: Option<String>,
    pub partial_input_typename: Option<String>,
    pub kind: TypeKind,
}

impl TypeData {
    pub fn description(&self, ontology: &Ontology) -> Option<std::string::String> {
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
                kind: ObjectKind::Query,
                ..
            }) => None,
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

    pub fn fields(&self) -> Option<&IndexMap<String, FieldData>> {
        match &self.kind {
            TypeKind::Object(obj) => Some(&obj.fields),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum TypeKind {
    Object(ObjectData),
    Union(UnionData),
    CustomScalar(ScalarData),
}

#[derive(Serialize, Deserialize)]
pub struct ObjectData {
    pub fields: IndexMap<String, FieldData>,
    pub kind: ObjectKind,
}

#[derive(Serialize, Deserialize)]
pub enum ObjectKind {
    Node(NodeData),
    Edge(EdgeData),
    Connection(ConnectionData),
    PageInfo,
    MutationResult,
    Query,
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
    pub variants: Vec<TypeAddr>,
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

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub enum FieldKind {
    /// A normal property of a node
    Property(PropertyData),
    /// The (incoming/"rel params") edge property of a node
    EdgeProperty(PropertyData),
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
    ConnectionProperty {
        property_id: PropertyId,
        first_arg: argument::FirstArg,
        after_arg: argument::AfterArg,
    },
    /// A connection to a map statement
    MapConnection {
        key: [MapKey; 2],
        queries: FnvHashMap<PropertyId, Var>,
        input_arg: argument::MapInputArg,
        first_arg: argument::FirstArg,
        after_arg: argument::AfterArg,
    },
    /// A find query from a map statement (zero or one return)
    MapFind {
        key: [MapKey; 2],
        queries: FnvHashMap<PropertyId, Var>,
        input_arg: argument::MapInputArg,
    },
    EntityMutation {
        create_arg: argument::EntityCreateInputsArg,
        update_arg: argument::EntityUpdateInputsArg,
        delete_arg: argument::EntityDeleteInputsArg,
        field_unit_type_addr: TypeAddr,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PropertyData {
    pub property_id: PropertyId,
    pub value_operator_addr: SerdeOperatorAddr,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IdPropertyData {
    pub relationship_id: RelationshipId,
    pub operator_addr: SerdeOperatorAddr,
}

pub struct EntityData {
    pub type_addr: TypeAddr,
    pub node_def_id: DefId,
    pub id_def_id: DefId,
}

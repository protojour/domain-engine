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
    Array(Optionality, Optionality),
}

impl TypeModifier {
    pub fn new_unit(optionality: Optionality) -> Self {
        TypeModifier::Unit(optionality)
    }

    pub fn unit_optionality(&self) -> Optionality {
        match self {
            Self::Unit(unit) => *unit,
            Self::Array(_, unit) => *unit,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum UnitTypeRef {
    Addr(TypeAddr),
    NativeScalar(NativeScalarRef),
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

    pub fn _optional(unit: UnitTypeRef) -> Self {
        Self {
            modifier: TypeModifier::Unit(Optionality::Optional),
            unit,
        }
    }

    pub fn to_array(self, array_optionality: Optionality) -> Self {
        Self {
            modifier: TypeModifier::Array(array_optionality, self.modifier.unit_optionality()),
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
                kind: ObjectKind::Connection,
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
    Connection,
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
    pub node_operator_addr: SerdeOperatorAddr,
    pub rel_edge_ref: Option<UnitTypeRef>,
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
    Property(PropertyData),
    EdgeProperty(PropertyData),
    Id(IdPropertyData),
    Node,
    Edges,
    Connection {
        property_id: Option<PropertyId>,
        first: argument::First,
        after: argument::After,
    },
    Map {
        key: [MapKey; 2],
        input_operator_addr: SerdeOperatorAddr,
        /// If this string is defined, there will be a single argument with this name.
        scalar_input_name: Option<String>,
        queries: FnvHashMap<PropertyId, Var>,
    },
    CreateMutation {
        input: argument::Input,
    },
    UpdateMutation {
        id: argument::Id,
        input: argument::Input,
    },
    DeleteMutation {
        id: argument::Id,
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

use indexmap::IndexMap;
use ontol_runtime::{
    env::Env, serde::operator::SerdeOperatorId, value::PropertyId, DefId, RelationId,
};
use smartstring::alias::String;

use super::argument::{self};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct TypeIndex(pub u32);

#[derive(Clone, Copy, Default, Debug)]
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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub enum UnitTypeRef {
    Indexed(TypeIndex),
    NativeScalar(NativeScalarRef),
}

#[derive(Clone, Copy, Debug)]
pub struct NativeScalarRef {
    pub operator_id: SerdeOperatorId,
    pub kind: NativeScalarKind,
}

#[derive(Clone, Copy, Debug)]
pub enum NativeScalarKind {
    Unit,
    Bool,
    Int(DefId),
    Number(DefId),
    String,
    ID,
}

#[derive(Clone, Copy, Debug)]
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

pub struct TypeData {
    pub typename: String,
    pub input_typename: Option<String>,
    pub partial_input_typename: Option<String>,
    pub kind: TypeKind,
}

impl TypeData {
    pub fn description(&self, env: &Env) -> Option<std::string::String> {
        match &self.kind {
            TypeKind::Object(ObjectData {
                kind: ObjectKind::Node(nodedata),
                ..
            }) => env.get_docs(nodedata.def_id),
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
            TypeKind::Union(uniondata) => env.get_docs(uniondata.union_def_id),
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

pub enum TypeKind {
    Object(ObjectData),
    Union(UnionData),
    CustomScalar(ScalarData),
}

pub struct ObjectData {
    pub fields: IndexMap<String, FieldData>,
    pub kind: ObjectKind,
}

pub enum ObjectKind {
    Node(NodeData),
    Edge(EdgeData),
    Connection,
    Query,
    Mutation,
}

pub struct NodeData {
    pub def_id: DefId,
    pub entity_id: Option<DefId>,
    pub operator_id: SerdeOperatorId,
}

pub struct UnionData {
    pub union_def_id: DefId,
    pub variants: Vec<TypeIndex>,
}

pub struct EdgeData {
    pub node_operator_id: SerdeOperatorId,
    pub rel_edge_ref: Option<UnitTypeRef>,
}

pub struct ScalarData {
    pub serde_operator_id: SerdeOperatorId,
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct PropertyData {
    pub property_id: PropertyId,
    pub value_operator_id: SerdeOperatorId,
    pub relationship_id: DefId,
}

#[derive(Debug)]
pub struct IdPropertyData {
    pub relation_id: RelationId,
    pub operator_id: SerdeOperatorId,
}

pub struct EntityData {
    pub type_index: TypeIndex,
    pub node_def_id: DefId,
    pub id_def_id: DefId,
}

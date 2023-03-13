use indexmap::IndexMap;
use ontol_runtime::{serde::operator::SerdeOperatorId, value::PropertyId, DefId, RelationId};
use smartstring::alias::String;

use super::argument::FieldArgument;

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
    Scalar(NativeScalarRef),
}

#[derive(Clone, Copy, Debug)]
pub enum NativeScalarRef {
    Unit,
    Bool,
    Int(DefId),
    Number(DefId),
    String(SerdeOperatorId),
    ID(SerdeOperatorId),
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
            modifier: TypeModifier::Unit(Optionality::Mandatory),
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
    pub fn connection(kind: ConnectionFieldKind, unit_type_ref: UnitTypeRef) -> Self {
        Self {
            kind: FieldKind::Connection(kind),
            field_type: TypeRef::mandatory(unit_type_ref),
        }
    }
}

#[derive(Debug)]
pub enum FieldKind {
    Property(PropertyId),
    Id(RelationId),
    Node,
    Edges,
    Connection(ConnectionFieldKind),
    CreateMutation {
        input: FieldArgument,
    },
    UpdateMutation {
        id: FieldArgument,
        input: FieldArgument,
    },
    DeleteMutation {
        id: FieldArgument,
    },
}

#[derive(Debug)]
pub struct ConnectionFieldKind {
    pub property_id: Option<PropertyId>,
}

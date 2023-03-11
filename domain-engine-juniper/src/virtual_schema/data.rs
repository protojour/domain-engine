use indexmap::IndexMap;
use ontol_runtime::{serde::SerdeOperatorId, DefId};
use smartstring::alias::String;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct TypeIndex(pub u32);

#[derive(Clone, Copy, Default)]
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
pub enum UnitTypeRef {
    Indexed(TypeIndex),
    ID(SerdeOperatorId),
    NativeScalar(NativeScalarRef),
}

#[derive(Clone, Copy)]
pub enum NativeScalarRef {
    Unit,
    Int(DefId),
    Number(DefId),
    Bool,
    String(SerdeOperatorId),
}

#[derive(Clone, Copy)]
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

    pub fn to_mandatory_array(self) -> Self {
        self.to_array(Optionality::Mandatory)
    }
}

pub struct TypeData {
    pub typename: String,
    pub kind: TypeKind,
}

pub enum TypeKind {
    Object(ObjectData),
    Union(UnionData),
    Scalar(ScalarData),
}

pub struct ObjectData {
    pub fields: IndexMap<String, FieldData>,
    pub kind: ObjectKind,
}

pub enum ObjectKind {
    Node(NodeData),
    Edge(EdgeData),
    Connection(ConnectionData),
    Query,
    Mutation,
}

pub struct NodeData {
    pub def_id: DefId,
    pub entity_id: Option<DefId>,
}

pub struct UnionData {
    pub union_def_id: DefId,
    pub variants: Vec<TypeIndex>,
}

pub struct ConnectionData {}

pub struct EdgeData {}

pub struct ScalarData {
    pub serde_operator_id: SerdeOperatorId,
}

pub struct FieldData {
    pub arguments: ArgumentsKind,
    pub field_type: TypeRef,
}

impl FieldData {
    pub fn no_args(field_type: TypeRef) -> Self {
        Self {
            arguments: ArgumentsKind::Empty,
            field_type,
        }
    }

    pub fn connection(unit_type_ref: UnitTypeRef) -> Self {
        Self {
            arguments: ArgumentsKind::ConnectionQuery,
            field_type: TypeRef::mandatory(unit_type_ref),
        }
    }
}

pub enum ArgumentsKind {
    Empty,
    ConnectionQuery,
    CreateMutation,
    UpdateMutation,
    DeleteMutation,
}

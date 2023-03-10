use std::sync::Arc;

use indexmap::IndexMap;
use ontol_runtime::{env::Env, serde::SerdeOperatorId, DefId, PackageId};
use smartstring::alias::String;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct TypeIndex(pub u32);

#[derive(Clone, Copy, Default)]
pub enum Optionality {
    #[default]
    Mandatory,
    Optional,
}

#[derive(Clone, Copy)]
pub enum TypeModifier {
    Unit(Optionality),
    Array(Optionality, Optionality),
}

impl TypeModifier {
    fn unit_optionality(&self) -> Optionality {
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

    pub fn to_mandatory_array(self) -> Self {
        Self {
            modifier: TypeModifier::Array(Optionality::Mandatory, self.modifier.unit_optionality()),
            unit: self.unit,
        }
    }
}

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,
    pub query: TypeIndex,
    pub mutation: TypeIndex,
    pub types: Vec<TypeData>,
}

impl DomainData {
    pub fn type_data(&self, index: TypeIndex) -> &TypeData {
        &self.types[index.0 as usize]
    }

    pub fn object_data_mut(&mut self, index: TypeIndex) -> &mut ObjectData {
        let type_data = self.types.get_mut(index.0 as usize).unwrap();
        match &mut type_data.kind {
            TypeKind::Object(object_data) => object_data,
            _ => panic!("{index:?} is not an object"),
        }
    }

    pub fn entity_check(&self, type_ref: UnitTypeRef) -> Option<(TypeIndex, DefId)> {
        if let UnitTypeRef::Indexed(type_index) = type_ref {
            if let TypeKind::Object(obj) = &self.type_data(type_index).kind {
                if let ObjectKind::Node(node) = &obj.kind {
                    if node.entity_id.is_some() {
                        Some((type_index, node.def_id))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
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

pub enum ArgumentsKind {
    Empty,
    ConnectionQuery,
    CreateMutation,
    UpdateMutation,
    DeleteMutation,
}

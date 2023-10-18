use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{
    condition::{CondTerm, Condition},
    value::PropertyId,
    DefId,
};

#[derive(Clone, Debug)]
pub enum Select {
    EntityId,
    Leaf,
    Struct(StructSelect),
    StructUnion(DefId, Vec<StructSelect>),
    Entity(EntitySelect),
}

#[derive(Clone, Debug)]
pub struct EntitySelect {
    pub source: StructOrUnionSelect,
    pub condition: Condition<CondTerm>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub enum StructOrUnionSelect {
    Struct(StructSelect),
    Union(DefId, Vec<StructSelect>),
}

impl StructOrUnionSelect {
    pub fn def_id(&self) -> DefId {
        match self {
            Self::Struct(struct_) => struct_.def_id,
            Self::Union(def_id, _) => *def_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StructSelect {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, Select>,
}

impl From<StructSelect> for Select {
    fn from(value: StructSelect) -> Self {
        Select::Struct(value)
    }
}

impl From<StructSelect> for EntitySelect {
    fn from(value: StructSelect) -> Self {
        EntitySelect {
            source: StructOrUnionSelect::Struct(value),
            condition: Condition::default(),
            limit: 20,
            cursor: None,
        }
    }
}

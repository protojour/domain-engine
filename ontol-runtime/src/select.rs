use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{condition::Condition, value::PropertyId, DefId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Select {
    EntityId,
    Leaf,
    Struct(StructSelect),
    StructUnion(DefId, Vec<StructSelect>),
    Entity(EntitySelect),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntitySelect {
    pub source: StructOrUnionSelect,
    pub condition: Condition,
    pub limit: usize,
    pub after_cursor: Option<Box<[u8]>>,
    pub include_total_len: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructSelect {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, Select>,
}

impl StructSelect {
    pub fn into_entity_select(self, limit: usize, after_cursor: Option<Box<[u8]>>) -> EntitySelect {
        EntitySelect {
            source: StructOrUnionSelect::Struct(self),
            condition: Condition::default(),
            limit,
            after_cursor,
            include_total_len: false,
        }
    }
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
            after_cursor: None,
            include_total_len: false,
        }
    }
}

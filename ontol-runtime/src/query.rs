use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Query {
    EntityId,
    Leaf,
    Struct(StructQuery),
    StructUnion(DefId, Vec<StructQuery>),
    Entity(EntityQuery),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EntityQuery {
    pub source: StructOrUnionQuery,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum StructOrUnionQuery {
    Struct(StructQuery),
    Union(DefId, Vec<StructQuery>),
}

impl StructOrUnionQuery {
    pub fn def_id(&self) -> DefId {
        match self {
            Self::Struct(struct_) => struct_.def_id,
            Self::Union(def_id, _) => *def_id,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct StructQuery {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, Query>,
}

impl From<StructQuery> for Query {
    fn from(value: StructQuery) -> Self {
        Query::Struct(value)
    }
}

impl From<StructQuery> for EntityQuery {
    fn from(value: StructQuery) -> Self {
        EntityQuery {
            source: StructOrUnionQuery::Struct(value),
            limit: 20,
            cursor: None,
        }
    }
}

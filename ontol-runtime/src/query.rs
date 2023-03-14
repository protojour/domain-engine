use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

#[derive(Debug)]
pub enum Query {
    Leaf,
    Map(MapQuery),
    MapUnion(DefId, Vec<MapQuery>),
    Entity(EntityQuery),
}

#[derive(Debug)]
pub struct EntityQuery {
    pub source: MapOrUnionQuery,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug)]
pub enum MapOrUnionQuery {
    Map(MapQuery),
    Union(DefId, Vec<MapQuery>),
}

impl MapOrUnionQuery {
    pub fn def_id(&self) -> DefId {
        match self {
            Self::Map(map) => map.def_id,
            Self::Union(def_id, _) => *def_id,
        }
    }
}

#[derive(Debug)]
pub struct MapQuery {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, Query>,
}

use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

#[derive(Debug)]
pub enum Query {
    Leaf,
    Map(MapQuery),
    MapUnion(Vec<MapQuery>),
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
    Union(Vec<MapQuery>),
}

#[derive(Debug)]
pub struct MapQuery {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, Query>,
}

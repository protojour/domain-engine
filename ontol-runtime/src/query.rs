use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

#[derive(Debug)]
pub struct EntityQuery {
    pub source: EntityQuerySource,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug)]
pub enum EntityQuerySource {
    Entity(ObjectSource),
    EntityUnion(Vec<ObjectSource>),
}

#[derive(Debug)]
pub struct ObjectSource {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, PropertySelection>,
}

#[derive(Debug)]
pub enum PropertySelection {
    Leaf,
    Object(ObjectSource),
    ObjectUnion(Vec<ObjectSource>),
    EntityQuery(EntityQuery),
}

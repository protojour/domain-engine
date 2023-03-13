use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

pub struct Query {
    pub source: Source,
    pub limit: u32,
    pub cursor: Option<String>,
}

pub enum PropertySelection {
    Leaf,
    Query(Query),
}

pub enum Source {
    Entity(EntitySource),
    EntityUnion(Vec<EntitySource>),
}

pub struct EntitySource {
    pub def_id: DefId,
    pub properties: FnvHashMap<PropertyId, PropertySelection>,
}

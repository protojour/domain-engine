use ontol_runtime::{value::PropertyId, DefId};

pub struct EntityQuery {
    pub def_id: DefId,
    pub selection: Vec<Selection>,
    pub joins: Vec<Join>,
}

pub struct Selection {
    pub property_id: PropertyId,
    pub selection: Vec<Selection>,
}

pub struct Join {}

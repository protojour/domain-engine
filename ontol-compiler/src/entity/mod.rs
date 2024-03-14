//! Entity-related compiler checks

use fnv::FnvHashMap;
use ontol_runtime::DefId;

pub mod check_entity;

mod entity_order;

/// Compiler data structure storing the output of check_entity
#[derive(Default)]
pub struct Entities {
    pub entities: FnvHashMap<DefId, Entity>,
}

impl Entities {
    pub fn order_union(&self, def_id: &DefId) -> Option<DefId> {
        self.entities
            .get(def_id)
            .and_then(|entity| entity.order_union)
    }
}

/// Various information about one entity
pub struct Entity {
    pub order_union: Option<DefId>,
}

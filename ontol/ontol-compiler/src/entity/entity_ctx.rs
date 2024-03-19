use fnv::FnvHashMap;
use ontol_runtime::{ontology::domain::ExtendedEntityInfo, DefId};

/// Compiler data structure storing the output of check_entity
#[derive(Default)]
pub struct EntityCtx {
    pub entities: FnvHashMap<DefId, ExtendedEntityInfo>,
}

impl EntityCtx {
    pub fn order_union(&self, def_id: &DefId) -> Option<DefId> {
        self.entities
            .get(def_id)
            .and_then(|entity| entity.order_union)
    }
}

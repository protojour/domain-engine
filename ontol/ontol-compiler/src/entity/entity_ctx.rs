use fnv::FnvHashMap;
use ontol_runtime::{DefId, ontology::domain::ExtendedEntityInfo};

use crate::{
    properties::PropCtx,
    repr::{repr_ctx::ReprCtx, repr_model::ReprKind},
};

/// Compiler data structure storing the output of check_entity
///
/// "entities" in the ONTOL context are properties that must be persisted together.
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

pub fn def_implies_entity(
    def_id: DefId,
    repr_ctx: &ReprCtx,
    prop_ctx: &PropCtx,
    entity_ctx: &EntityCtx,
) -> bool {
    if let Some(ReprKind::Union(variants, _)) = repr_ctx.get_repr_kind(&def_id) {
        return variants
            .iter()
            .any(|(def_id, _)| def_implies_entity(*def_id, repr_ctx, prop_ctx, entity_ctx));
    }

    if entity_ctx.entities.contains_key(&def_id) {
        return true;
    }

    if let Some(properties) = prop_ctx.properties_by_def_id(def_id) {
        if properties.identifies.is_some() {
            return true;
        }
    }

    false
}

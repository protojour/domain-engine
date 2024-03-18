use fnv::FnvHashMap;
use ontol_runtime::DefId;

use super::repr_model::{Repr, ReprKind};

#[derive(Default)]
pub struct ReprCtx {
    /// Table of repr results
    pub repr_table: FnvHashMap<DefId, Repr>,
}

impl ReprCtx {
    pub fn get_repr_kind(&self, def_id: &DefId) -> Option<&ReprKind> {
        self.repr_table.get(def_id).map(|repr| &repr.kind)
    }
}

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{DefId, PackageId};

use crate::repr::repr_model::{Repr, ReprKind};

#[derive(Default)]
pub struct SealCtx {
    /// Map of repr results
    pub repr_table: FnvHashMap<DefId, Repr>,

    /// Set of completely sealed domains
    sealed_domains: FnvHashSet<PackageId>,

    /// The set of defs currently sealed in a partially sealed package (the one currently compiling).
    partially_sealed_defs: FnvHashSet<DefId>,
}

impl SealCtx {
    pub fn get_repr_kind(&self, def_id: &DefId) -> Option<&ReprKind> {
        self.repr_table.get(def_id).map(|repr| &repr.kind)
    }

    pub fn mark_domain_sealed(&mut self, package_id: PackageId) {
        self.sealed_domains.insert(package_id);
        self.partially_sealed_defs.clear();
    }

    pub fn is_sealed(&self, def_id: DefId) -> bool {
        self.sealed_domains.contains(&def_id.package_id())
    }
}

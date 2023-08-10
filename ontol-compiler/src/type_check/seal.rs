use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{DefId, PackageId};

use crate::def::DefKind;

use super::{repr::repr_model::ReprKind, TypeCheck};

#[derive(Default, Debug)]
pub struct SealCtx {
    /// Map of repr results
    pub repr_table: FnvHashMap<DefId, ReprKind>,

    /// Set of completely sealed domains
    sealed_domains: FnvHashSet<PackageId>,

    /// The set of defs currently sealed in a partially sealed package (the one currently compiling).
    partially_sealed_defs: FnvHashSet<DefId>,
}

impl SealCtx {
    pub fn mark_domain_sealed(&mut self, package_id: PackageId) {
        self.sealed_domains.insert(package_id);
        self.partially_sealed_defs.clear();
    }

    pub fn is_sealed(&self, def_id: DefId) -> bool {
        self.sealed_domains.contains(&def_id.package_id())
            || self.partially_sealed_defs.contains(&def_id)
    }
}

/// Code for sealing definitions.
///
/// Sealed definitions are interpreted as immutable.
impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn seal_def(&mut self, def_id: DefId) {
        if self.seal_ctx.sealed_domains.contains(&def_id.package_id())
            || !self.seal_ctx.partially_sealed_defs.insert(def_id)
        {
            // Already sealed
            return;
        }

        if let Some(def) = self.defs.table.get(&def_id) {
            if let DefKind::Type(_) = def.kind {
                self.check_domain_type_pre_repr(def_id, def);
            }
        }

        self.repr_check(def_id).check_repr_root();

        if let Some(def) = self.defs.table.get(&def_id) {
            if let DefKind::Type(_) = def.kind {
                self.check_domain_type_post_repr(def_id, def);
            }
        }

        if let Some(ReprKind::Union(_) | ReprKind::StructUnion(_)) =
            self.seal_ctx.repr_table.get(&def_id)
        {
            for error in self.check_value_union(def_id) {
                self.errors.push(error);
            }
        }
    }
}

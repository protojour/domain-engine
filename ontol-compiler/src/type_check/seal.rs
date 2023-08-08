use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::DefId;
use tracing::debug;

use crate::def::DefKind;

use super::{repr::repr_model::ReprKind, TypeCheck};

#[derive(Default, Debug)]
pub struct SealedDefs {
    /// The set of defs currently sealed
    pub sealed_set: FnvHashSet<DefId>,

    /// Map of repr results
    pub repr_table: FnvHashMap<DefId, ReprKind>,
}

/// Code for sealing definitions.
///
/// Sealed definitions are interpreted as immutable.
impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn seal_all_defs(&mut self) {
        debug!("seal all defs");

        for def_id in self.defs.table.keys() {
            self.seal_def(*def_id);
        }
    }

    pub fn seal_def(&mut self, def_id: DefId) {
        if !self.sealed_defs.sealed_set.insert(def_id) {
            return;
        }

        if let Some(def) = self.defs.table.get(&def_id) {
            if let DefKind::Type(_) = def.kind {
                self.check_domain_type_properties(def_id, def);
            }
        }

        self.repr_check(def_id).check_repr_root();

        if self.union_ctx.union_set.contains(&def_id) {
            for error in self.check_value_union(def_id) {
                self.errors.push(error);
            }
        }
    }
}

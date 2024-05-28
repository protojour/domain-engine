use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    ontology::map::MapLossiness, resolve_path::ResolverGraph, DefId, MapDirection, PackageId,
};

use crate::{CompileError, Compiler};

impl<'m> Compiler<'m> {
    pub(super) fn persistence_check(&mut self) {
        self.resolver_graph =
            ResolverGraph::new(self.codegen_tasks.result_map_proc_table.keys().map(|key| {
                self.codegen_tasks
                    .result_metadata_table
                    .get(key)
                    .map(|meta| (*key, meta.direction, meta.lossiness))
                    .unwrap_or((*key, MapDirection::Down, MapLossiness::Lossy))
            }));

        let mut entities_by_pkg_id: FnvHashMap<PackageId, Vec<DefId>> = Default::default();

        for def_id in self.entity_ctx.entities.keys() {
            entities_by_pkg_id
                .entry(def_id.package_id())
                .or_default()
                .push(*def_id);
        }

        let mut non_mapped_entities_by_pkd_id: FnvHashMap<PackageId, FnvHashSet<DefId>> =
            Default::default();

        for (pkg_id, entities) in &entities_by_pkg_id {
            for entity_def_id in entities {
                if !self.resolver_graph.is_mapped_down(entity_def_id)
                    || !self.resolver_graph.is_mapped_up(entity_def_id)
                {
                    non_mapped_entities_by_pkd_id
                        .entry(*pkg_id)
                        .or_default()
                        .insert(*entity_def_id);
                }
            }
        }

        match non_mapped_entities_by_pkd_id.len() {
            0 => {}
            1 => {
                self.persistent_domain =
                    Some(non_mapped_entities_by_pkd_id.into_iter().next().unwrap().0);
            }
            _ => {
                for (_pkg_id, entity_def_ids) in non_mapped_entities_by_pkd_id {
                    for entity_def_id in entity_def_ids {
                        CompileError::MultiDomainPersistenceNotAllowed
                            .span(self.defs.def_span(entity_def_id))
                            .report(self);
                    }
                }
            }
        }
    }
}

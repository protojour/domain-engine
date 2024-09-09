use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use ontol_parser::U32Span;
use ontol_runtime::{
    ontology::map::MapLossiness, resolve_path::ResolverGraph, DefId, DomainIndex, MapDirection,
};

use crate::{CompileError, Compiler, SourceSpan};

impl<'m> Compiler<'m> {
    pub(super) fn persistence_check(&mut self) {
        self.resolver_graph =
            ResolverGraph::new(self.code_ctx.result_map_proc_table.keys().map(|key| {
                self.code_ctx
                    .result_metadata_table
                    .get(key)
                    .map(|meta| (*key, meta.direction, meta.lossiness))
                    .unwrap_or((*key, MapDirection::Down, MapLossiness::Lossy))
            }));

        let mut entities_by_domain_idx: FnvHashMap<DomainIndex, Vec<DefId>> = Default::default();

        for def_id in self.entity_ctx.entities.keys() {
            entities_by_domain_idx
                .entry(def_id.domain_index())
                .or_default()
                .push(*def_id);
        }

        let mut non_mapped_entities_by_domain_idx: FnvHashMap<DomainIndex, FnvHashSet<DefId>> =
            Default::default();

        for (domain_index, entities) in &entities_by_domain_idx {
            for entity_def_id in entities {
                if !self.resolver_graph.is_mapped_down(entity_def_id)
                    || !self.resolver_graph.is_mapped_up(entity_def_id)
                {
                    non_mapped_entities_by_domain_idx
                        .entry(*domain_index)
                        .or_default()
                        .insert(*entity_def_id);
                }
            }
        }

        let mut persistent_domains: BTreeSet<DomainIndex> = Default::default();

        // expand "entry level" persistent domains with all transitive, upstream domains,
        // except `DomainIndex::ontol()`
        {
            let mut working_set: FnvHashSet<DomainIndex> =
                non_mapped_entities_by_domain_idx.keys().copied().collect();

            while !working_set.is_empty() {
                for domain_index in std::mem::take(&mut working_set) {
                    if domain_index == DomainIndex::ontol() {
                        continue;
                    }

                    if persistent_domains.insert(domain_index) {
                        if let Some(deps) = self.domain_dep_graph.get(&domain_index) {
                            for dep in deps {
                                working_set.insert(*dep);
                            }
                        }
                    }
                }
            }
        }

        for domain_index in persistent_domains.iter().copied() {
            let domain_id = self.domain_ids.get(&domain_index).unwrap();

            if !domain_id.stable {
                let span = SourceSpan {
                    source_id: self.sources.source_id_for_domain(domain_index).unwrap(),
                    span: U32Span { start: 0, end: 0 },
                };

                CompileError::PersistedDomainIsMissingDomainHeader
                    .span(span)
                    .report(self);
            }
        }

        self.persistent_domains = persistent_domains;
    }
}

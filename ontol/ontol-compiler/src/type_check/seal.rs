use fnv::FnvHashSet;
use ontol_core::tag::DomainIndex;
use ontol_runtime::DefId;

#[derive(Default)]
pub struct SealCtx {
    /// Set of completely sealed domains
    sealed_domains: FnvHashSet<DomainIndex>,

    /// The set of defs currently sealed in a partially sealed domain (the one currently compiling).
    partially_sealed_defs: FnvHashSet<DefId>,
}

impl SealCtx {
    pub fn mark_domain_sealed(&mut self, domain_index: DomainIndex) {
        self.sealed_domains.insert(domain_index);
        self.partially_sealed_defs.clear();
    }

    pub fn is_sealed(&self, def_id: DefId) -> bool {
        self.sealed_domains.contains(&def_id.domain_index())
    }
}

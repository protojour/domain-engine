use std::collections::BTreeMap;

use bit_set::BitSet;
use fnv::FnvHashMap;

use crate::{
    sem_diff::InitDiff,
    sem_model::ProjectedSemModel,
    sem_stmts::{Arc, Def, Rel, Use},
    tables::RelKey,
    tag::{Tag, TagAllocator},
    token::Token,
};

pub(crate) struct MatchTracker {
    pub(super) unmatched_uses: BitSet,
    pub(super) unmatched_defs: BitSet,
    pub(super) unmatched_arcs: BitSet,
    pub(super) unmatched_rels: BitSet,

    pub(super) syntax_builder: SyntaxBuilder,

    pub(super) allocator: TagAllocator,

    prev_syntax: SyntaxMatcher,
}

#[derive(Clone)]
pub(crate) struct SyntaxMatcher {
    pub(super) from_scratch: bool,
    pub(super) syntax: std::sync::Arc<SyntaxTracker>,
}

#[derive(Default, Debug)]
pub(crate) struct SyntaxTracker {
    pub(super) root: Vec<(Tag, SyntaxKind)>,
    pub(super) context: FnvHashMap<Tag, Vec<(Tag, SyntaxKind)>>,
}

#[derive(Default, Debug)]
pub(crate) struct SyntaxBuilder {
    root: BTreeMap<u32, (Tag, SyntaxKind)>,
    context: FnvHashMap<Tag, BTreeMap<u32, (Tag, SyntaxKind)>>,
}

impl SyntaxBuilder {
    pub fn insert(&mut self, parent: Option<Tag>, index: u32, tag: (Tag, SyntaxKind)) {
        if let Some(parent) = parent {
            self.context.entry(parent).or_default().insert(index, tag);
        } else {
            self.root.insert(index, tag);
        }
    }

    pub fn build(self) -> SyntaxTracker {
        SyntaxTracker {
            root: self.root.into_values().collect(),
            context: self
                .context
                .into_iter()
                .map(|(parent, map)| (parent, map.into_values().collect()))
                .collect(),
        }
    }
}

impl SyntaxMatcher {
    pub fn from_scratch() -> Self {
        Self {
            from_scratch: true,
            syntax: Default::default(),
        }
    }

    pub fn from_syntax(syntax: std::sync::Arc<SyntaxTracker>) -> Self {
        Self {
            from_scratch: false,
            syntax,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyntaxKind {
    Use,
    Def,
    Arc,
    Rel,
}

impl MatchTracker {
    pub fn unmatched_uses(&self) -> impl Iterator<Item = Tag> {
        iter_tag_bitset(&self.unmatched_uses)
    }

    pub fn unmatched_defs(&self) -> impl Iterator<Item = Tag> {
        iter_tag_bitset(&self.unmatched_defs)
    }

    pub fn unmatched_arcs(&self) -> impl Iterator<Item = Tag> {
        iter_tag_bitset(&self.unmatched_arcs)
    }

    pub fn unmatched_rels(&self) -> impl Iterator<Item = Tag> {
        iter_tag_bitset(&self.unmatched_rels)
    }

    pub fn init_diff(&mut self, parent: Option<Tag>) -> InitDiff<'_> {
        let syntax_matcher = self.prev_syntax.clone();

        InitDiff {
            tracker: self,
            syntax_matcher,
            parent,
        }
    }

    pub fn match_use_by_ident<'o>(
        &mut self,
        ident: &Token,
        origin: &'o ProjectedSemModel,
    ) -> Option<(Tag, &'o Use)> {
        let tag = origin.local.lookup.use_by_ident(origin.subdomain, ident)?;

        if self.unmatched_uses.remove(tag.0 as usize) {
            Some((tag, origin.local.uses.get(&tag).unwrap()))
        } else {
            None
        }
    }

    pub fn match_def_by_ident<'o>(
        &mut self,
        ident: &Token,
        origin: &'o ProjectedSemModel,
    ) -> Option<(Tag, &'o Def)> {
        let tag = origin.local.lookup.def_by_ident(origin.subdomain, ident)?;

        if self.unmatched_defs.remove(tag.0 as usize) {
            Some((tag, origin.local.defs.get(&tag).unwrap()))
        } else {
            None
        }
    }

    pub fn match_arc_by_ident<'o>(
        &mut self,
        ident: &Token,
        origin: &'o ProjectedSemModel,
    ) -> Option<(Tag, &'o Arc)> {
        let tag = origin.local.lookup.arc_by_ident(origin.subdomain, ident)?;

        if self.unmatched_arcs.remove(tag.0 as usize) {
            Some((tag, origin.local.arcs.get(&tag).unwrap()))
        } else {
            None
        }
    }

    pub fn match_rel_by_key<'o>(
        &mut self,
        key: &RelKey,
        origin: &'o ProjectedSemModel,
    ) -> Option<(Tag, &'o Rel)> {
        let tag = origin.local.lookup.rels_by_key.get(key)?;

        if self.unmatched_rels.remove(tag.0 as usize) {
            Some((*tag, origin.local.rels.get(tag).unwrap()))
        } else {
            None
        }
    }
}

fn iter_tag_bitset(bitset: &BitSet) -> impl Iterator<Item = Tag> {
    bitset.iter().map(|idx| Tag(idx.try_into().unwrap()))
}

impl ProjectedSemModel {
    pub(crate) fn new_tracker(&self) -> MatchTracker {
        let mut unmatched_uses = BitSet::new();
        let mut unmatched_defs = BitSet::new();
        let mut unmatched_arcs = BitSet::new();
        let mut unmatched_rels = BitSet::new();

        for (tag, u) in &self.local.uses {
            if u.subdomain == Some(self.subdomain) {
                unmatched_uses.insert(tag.0 as usize);
            }
        }

        for (tag, def) in &self.local.defs {
            if def.subdomain == Some(self.subdomain) {
                unmatched_defs.insert(tag.0 as usize);
            }
        }

        for (tag, arc) in &self.local.arcs {
            if arc.subdomain == Some(self.subdomain) {
                unmatched_arcs.insert(tag.0 as usize);
            }
        }

        for (tag, rel) in &self.local.rels {
            if rel.subdomain == Some(self.subdomain) {
                unmatched_rels.insert(tag.0 as usize);
            }
        }

        MatchTracker {
            unmatched_uses,
            unmatched_defs,
            unmatched_arcs,
            unmatched_rels,
            syntax_builder: SyntaxBuilder::default(),
            prev_syntax: self
                .local
                .domain_syntax
                .get(&self.subdomain)
                .cloned()
                .map(SyntaxMatcher::from_syntax)
                .unwrap_or_else(SyntaxMatcher::from_scratch),
            allocator: self.local.allocator.clone(),
        }
    }

    pub(crate) fn merge_tracker(&mut self, tracker: MatchTracker) {
        self.local.allocator = tracker.allocator;

        self.local.domain_syntax.insert(
            self.subdomain,
            std::sync::Arc::new(tracker.syntax_builder.build()),
        );
    }
}

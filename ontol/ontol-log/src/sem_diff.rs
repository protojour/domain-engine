use std::hash::{Hash, Hasher};

use fnv::{FnvHashMap, FnvHasher};
use smallvec::SmallVec;

use crate::{
    error::{OptionExt, SemError},
    sem_match::{MatchTracker, SyntaxKind, SyntaxMatcher},
    sem_model::{ProjectedSemModel, StmtRef},
    tag::Tag,
};

pub struct InitDiff<'t> {
    pub(super) tracker: &'t mut MatchTracker,
    pub(super) syntax_matcher: SyntaxMatcher,
    pub(super) parent: Option<Tag>,
}

pub struct Differ<'d> {
    tracker: &'d mut MatchTracker,
    syntax: &'d [(Tag, SyntaxKind)],
    parent: Option<Tag>,
    orig_hash: FnvHashMap<u64, SmallVec<(Tag, SyntaxKind), 1>>,
}

impl InitDiff<'_> {
    pub fn differ(&mut self, origin: &ProjectedSemModel) -> Differ<'_> {
        let syntax = match self.parent {
            Some(parent) => self
                .syntax_matcher
                .syntax
                .context
                .get(&parent)
                .map(|vec| vec.as_slice())
                .unwrap_or(&[]),
            None => &self.syntax_matcher.syntax.root,
        };

        let mut orig_hash = FnvHashMap::<u64, SmallVec<(Tag, SyntaxKind), 1>>::default();

        for (tag, kind) in syntax {
            let stmt_ref = origin_get(origin, *tag, *kind).unwrap();
            let hash = hash_stmt_ref(stmt_ref);
            orig_hash.entry(hash).or_default().push((*tag, *kind));
        }

        Differ {
            tracker: self.tracker,
            syntax,
            parent: self.parent,
            orig_hash,
        }
    }

    pub fn initial_stage(&self) -> MatchStage {
        if self.syntax_matcher.from_scratch {
            MatchStage::Alloc
        } else {
            MatchStage::Perfect
        }
    }
}

#[derive(Clone, Copy)]
pub enum MatchStage {
    Perfect,
    Ident,
    Kind,
    Alloc,
}

impl MatchStage {
    pub fn bump(&self) -> Option<Self> {
        match self {
            Self::Perfect => Some(Self::Ident),
            Self::Ident => Some(Self::Kind),
            Self::Kind => Some(Self::Alloc),
            Self::Alloc => None,
        }
    }
}

impl Differ<'_> {
    pub(crate) fn try_match<'o>(
        &mut self,
        stage: MatchStage,
        syntax_index: u32,
        stmt_ref: StmtRef,
        origin: &'o ProjectedSemModel,
    ) -> Result<Option<(Tag, Option<StmtRef<'o>>)>, SemError> {
        let (tag, orig) =
            match stage {
                MatchStage::Perfect => {
                    if let Some((tag, orig)) = self.remove_by_hash(stmt_ref, origin) {
                        match orig {
                            StmtRef::Use(_) => {
                                self.tracker.unmatched_uses.remove(tag.0 as usize);
                            }
                            StmtRef::Def(_) => {
                                self.tracker.unmatched_defs.remove(tag.0 as usize);
                            }
                            StmtRef::Arc(_) => {
                                self.tracker.unmatched_arcs.remove(tag.0 as usize);
                            }
                            StmtRef::Rel(_) => {
                                self.tracker.unmatched_rels.remove(tag.0 as usize);
                            }
                        };

                        (tag, Some(orig))
                    } else {
                        return Ok(None);
                    }
                }
                MatchStage::Ident => match stmt_ref {
                    StmtRef::Use(u) => {
                        let Some((tag, u)) = self
                            .tracker
                            .match_use_by_ident(&u.ident.as_ref().stx_err()?.0, origin)
                        else {
                            return Ok(None);
                        };

                        (tag, Some(StmtRef::Use(u)))
                    }
                    StmtRef::Def(def) => {
                        let Some((tag, def)) = self
                            .tracker
                            .match_def_by_ident(&def.ident.as_ref().stx_err()?.0, origin)
                        else {
                            return Ok(None);
                        };

                        (tag, Some(StmtRef::Def(def)))
                    }
                    StmtRef::Arc(arc) => {
                        let Some((tag, arc)) = self
                            .tracker
                            .match_arc_by_ident(&arc.ident.as_ref().stx_err()?.0, origin)
                        else {
                            return Ok(None);
                        };

                        (tag, Some(StmtRef::Arc(arc)))
                    }
                    StmtRef::Rel(rel) => {
                        let Some((tag, rel)) = self
                            .tracker
                            .match_rel_by_key(&rel.key().map_err(SemError::Apply)?, origin)
                        else {
                            return Ok(None);
                        };

                        (tag, Some(StmtRef::Rel(rel)))
                    }
                },
                MatchStage::Kind => {
                    if self.syntax.is_empty() {
                        return Ok(None);
                    }

                    let (syntax_kind, unmatched) = match stmt_ref {
                        StmtRef::Use(_) => (SyntaxKind::Use, &mut self.tracker.unmatched_uses),
                        StmtRef::Def(_) => (SyntaxKind::Def, &mut self.tracker.unmatched_defs),
                        StmtRef::Arc(_) => (SyntaxKind::Arc, &mut self.tracker.unmatched_arcs),
                        StmtRef::Rel(_) => (SyntaxKind::Rel, &mut self.tracker.unmatched_rels),
                    };

                    let Some((tag, _)) = self.syntax.iter().find(|(tag, kind)| {
                        *kind == syntax_kind && unmatched.contains(tag.0 as usize)
                    }) else {
                        return Ok(None);
                    };

                    unmatched.remove(tag.0 as usize);
                    let orig = origin_get(origin, *tag, syntax_kind).unwrap();

                    (*tag, Some(orig))
                }
                MatchStage::Alloc => {
                    // allocate new tag
                    let tag = self.tracker.allocator.next_tag.bump();

                    (tag, None)
                }
            };

        self.tracker.syntax_builder.insert(
            self.parent,
            syntax_index,
            (tag, stmt_ref.syntax_kind()),
        );

        Ok(Some((tag, orig)))
    }

    fn remove_by_hash<'o>(
        &mut self,
        stmt_ref: StmtRef,
        origin: &'o ProjectedSemModel,
    ) -> Option<(Tag, StmtRef<'o>)> {
        let hash = hash_stmt_ref(stmt_ref);

        let bucket = self.orig_hash.get_mut(&hash)?;

        let mut index = None;

        for (idx, (tag, kind)) in bucket.iter().enumerate() {
            let Some(orig) = origin_get(origin, *tag, *kind) else {
                continue;
            };

            if orig == stmt_ref {
                index = Some(idx);
                break;
            }
        }

        if let Some(index) = index {
            let (tag, kind) = bucket.swap_remove(index);
            let origin_ref = origin_get(origin, tag, kind)?;

            Some((tag, origin_ref))
        } else {
            None
        }
    }
}

fn origin_get(origin: &ProjectedSemModel, tag: Tag, kind: SyntaxKind) -> Option<StmtRef<'_>> {
    let local = &origin.local;
    match kind {
        SyntaxKind::Use => local.uses.get(&tag).map(StmtRef::Use),
        SyntaxKind::Def => local.defs.get(&tag).map(StmtRef::Def),
        SyntaxKind::Arc => local.arcs.get(&tag).map(StmtRef::Arc),
        SyntaxKind::Rel => local.rels.get(&tag).map(StmtRef::Rel),
    }
}

fn hash_stmt_ref(s: StmtRef) -> u64 {
    let mut hasher = FnvHasher::default();
    s.hash(&mut hasher);
    hasher.finish()
}

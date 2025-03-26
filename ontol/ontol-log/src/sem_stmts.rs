use std::collections::{BTreeMap, BTreeSet};

use arcstr::ArcStr;
use either::Either;
use thin_vec::{ThinVec, thin_vec};

use crate::{
    NoneIfEmpty,
    diff::{cmp_opt2, diff_btree_maps, diff_type_ref_or_union_or_pattern, diff_type_ref_or_union2},
    log_model::{
        ArcCoord, ArcProp, DefModifier, DefProp, EKind, Pattern, RelCrd, RelProp, TypeRef,
        TypeRefOrUnionOrPattern, TypeUnion, UseProp,
    },
    log_util::{history_arc_props, history_def_props, history_rel_props, history_use_props},
    sem_model::{ApplyError, ApplyResult, SemLookup},
    symbol::SymEntry,
    tables::RelKey,
    tag::Tag,
    token::Token,
    variant_fn,
    with_span::WithSpan,
};

#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct Use {
    pub domain: Option<Tag>,
    pub uri: Option<WithSpan<ArcStr>>,
    pub ident: Option<WithSpan<Token>>,
}

#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct Def {
    pub domain: Option<Tag>,
    pub ident: Option<WithSpan<Token>>,
    pub doc: Option<ArcStr>,
    pub symbol: bool,
    pub modifiers: BTreeSet<WithSpan<DefModifier>>,
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Rel {
    pub domain: Option<Tag>,
    pub rel_type: Option<WithSpan<TypeRef>>,
    pub subj_cardinality: RelCrd,
    pub subj_type: Either<WithSpan<TypeRef>, TypeUnion>,
    pub obj_cardinality: RelCrd,
    pub obj_type: TypeRefOrUnionOrPattern,
}

#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct Arc {
    pub domain: Option<Tag>,
    pub ident: Option<WithSpan<Token>>,
    pub vars: BTreeMap<ArcCoord, WithSpan<Token>>,
    pub params: BTreeMap<ArcCoord, WithSpan<TypeRef>>,
    pub slots: BTreeMap<ArcCoord, WithSpan<Token>>,
}

pub trait ApplyIdent {
    fn ident(&self) -> Option<(Tag, &Token)>;
    fn sym_entry(&self, tag: Tag) -> SymEntry;

    fn apply_ident(
        &mut self,
        tag: Tag,
        lk: &mut SemLookup,
        mut f: impl FnMut(&mut Self),
    ) -> ApplyResult {
        if let Some((domain, ident)) = self.ident() {
            lk.remove_sym_entry(domain, ident)?;
        }
        f(self);
        if let Some((domain, ident)) = self.ident() {
            lk.insert_sym_entry(domain, ident.clone(), self.sym_entry(tag))?;
        }
        Ok(())
    }
}

impl Use {
    pub fn apply(&mut self, props: &[UseProp], tag: Tag, lk: &mut SemLookup) -> ApplyResult {
        for prop in props {
            match prop {
                UseProp::Domain(domain) => self.apply_ident(tag, lk, |zelf| {
                    zelf.domain = Some(*domain);
                })?,
                UseProp::Ident(ident) => self.apply_ident(tag, lk, |zelf| {
                    zelf.ident = Some(ident.clone());
                })?,
                UseProp::Uri(uri) => {
                    self.uri = Some(uri.clone());
                }
            }
        }
        Ok(())
    }

    pub fn unapply(
        &mut self,
        props: &[UseProp],
        tag: Tag,
        lk: &mut SemLookup,
        history: &[EKind],
    ) -> ApplyResult {
        for prop in props.iter().rev() {
            match prop {
                UseProp::Domain(_) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.domain = history_use_props(history, tag)
                            .find_map(variant_fn!(UseProp::Domain(tag)))
                            .cloned();
                    })?;
                }
                UseProp::Ident(..) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.ident = history_use_props(history, tag)
                            .find_map(variant_fn!(UseProp::Ident(new)))
                            .cloned();
                    })?;
                }
                UseProp::Uri(..) => {
                    self.uri = history_use_props(history, tag)
                        .find_map(variant_fn!(UseProp::Uri(new)))
                        .cloned();
                }
            }
        }

        Ok(())
    }

    pub fn diff(&self, origin: &Use) -> Option<ThinVec<UseProp>> {
        let mut props = thin_vec![];

        if let Some(domain) = cmp_opt2(&self.domain, origin.domain.as_ref()) {
            props.push(UseProp::Domain(*domain));
        }

        if let Some(ident) = cmp_opt2(&self.ident, origin.ident.as_ref()) {
            props.push(UseProp::Ident(ident.clone()));
        }

        if let Some(uri) = cmp_opt2(&self.uri, origin.uri.as_ref()) {
            props.push(UseProp::Uri(uri.clone()));
        }

        props.none_if_empty()
    }
}

impl ApplyIdent for Use {
    fn ident(&self) -> Option<(Tag, &Token)> {
        match (self.domain, &self.ident) {
            (Some(domain), Some(ident)) => Some((domain, &ident.0)),
            _ => None,
        }
    }

    fn sym_entry(&self, tag: Tag) -> SymEntry {
        SymEntry::Use(tag)
    }
}

impl Def {
    pub fn apply(&mut self, props: &[DefProp], tag: Tag, lk: &mut SemLookup) -> ApplyResult {
        for prop in props {
            match prop {
                DefProp::Domain(domain) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.domain = Some(*domain);
                    })?;
                }
                DefProp::Ident(ident) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.ident = Some(ident.clone());
                    })?;
                }
                DefProp::SymSet => self.symbol = true,
                DefProp::Doc(_doc) => todo!(),
                DefProp::DocRemove => todo!(),
                DefProp::ModAdd(m) => {
                    self.modifiers.insert(*m);
                }
                DefProp::ModRemove(m) => {
                    self.modifiers.remove(m);
                }
            }
        }

        Ok(())
    }

    pub fn unapply(
        &mut self,
        props: &[DefProp],
        tag: Tag,
        lk: &mut SemLookup,
        history: &[EKind],
    ) -> ApplyResult {
        for prop in props.iter().rev() {
            match prop {
                DefProp::Domain(_) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.domain = history_def_props(history, tag)
                            .find_map(variant_fn!(DefProp::Domain(tag)))
                            .cloned();
                    })?;
                }
                DefProp::Ident(..) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.ident = history_def_props(history, tag)
                            .find_map(variant_fn!(DefProp::Ident(new)))
                            .cloned();
                    })?;
                }
                DefProp::SymSet => {
                    self.symbol = false;
                }
                DefProp::Doc(_doc) => todo!(),
                DefProp::DocRemove => todo!(),
                DefProp::ModAdd(m) => {
                    self.modifiers.remove(m);
                }
                DefProp::ModRemove(m) => {
                    self.modifiers.insert(*m);
                }
            }
        }

        Ok(())
    }

    pub fn diff(&self, origin: &Def) -> Option<ThinVec<DefProp>> {
        let mut props = thin_vec![];

        if let Some(domain) = cmp_opt2(&self.domain, origin.domain.as_ref()) {
            props.push(DefProp::Domain(*domain));
        }

        if let Some(ident) = cmp_opt2(&self.ident, origin.ident.as_ref()) {
            props.push(DefProp::Ident(ident.clone()));
        }

        // TODO: docs

        if self.symbol && self.symbol != origin.symbol {
            props.push(DefProp::SymSet);
        }

        for modifier in self.modifiers.difference(&origin.modifiers) {
            props.push(DefProp::ModAdd(*modifier));
        }

        for modifier in origin.modifiers.difference(&self.modifiers) {
            props.push(DefProp::ModRemove(*modifier));
        }

        props.none_if_empty()
    }
}

impl ApplyIdent for Def {
    fn ident(&self) -> Option<(Tag, &Token)> {
        match (self.domain, &self.ident) {
            (Some(domain), Some(ident)) => Some((domain, &ident.0)),
            _ => None,
        }
    }

    fn sym_entry(&self, tag: Tag) -> SymEntry {
        SymEntry::LocalDef(tag)
    }
}

impl Arc {
    pub fn apply(&mut self, props: &[ArcProp], tag: Tag, lk: &mut SemLookup) -> ApplyResult {
        for prop in props {
            match prop {
                ArcProp::Domain(domain) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.domain = Some(*domain);
                    })?;
                }
                ArcProp::Ident(ident) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.ident = Some(ident.clone());
                    })?;
                }
                ArcProp::SlotSymbol(coord, sym) => {
                    self.slots.insert(*coord, sym.clone());
                }
                ArcProp::Var(coord, sym) => {
                    self.vars.insert(*coord, sym.clone());
                }
                ArcProp::TypeParam(coord, param) => {
                    self.params.insert(*coord, param.clone());
                }
            }
        }

        Ok(())
    }

    pub fn unapply(
        &mut self,
        props: &[ArcProp],
        tag: Tag,
        lk: &mut SemLookup,
        history: &[EKind],
    ) -> ApplyResult {
        for prop in props {
            match prop {
                ArcProp::Domain(_) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.domain = history_arc_props(history, tag)
                            .find_map(variant_fn!(ArcProp::Domain(tag)))
                            .cloned();
                    })?;
                }
                ArcProp::Ident(_) => {
                    self.apply_ident(tag, lk, |zelf| {
                        zelf.ident = history_arc_props(history, tag)
                            .find_map(variant_fn!(ArcProp::Ident(ident)))
                            .cloned();
                    })?;
                }
                ArcProp::SlotSymbol(coord, _) => {
                    self.slots.remove(coord);
                }
                ArcProp::Var(coord, _) => {
                    self.vars.remove(coord);
                }
                ArcProp::TypeParam(coord, _) => {
                    self.params.remove(coord);
                }
            }
        }

        Ok(())
    }

    pub fn diff_sans_params(&self, origin: &Arc) -> Option<ThinVec<ArcProp>> {
        let mut props = thin_vec![];

        if let Some(domain) = cmp_opt2(&self.domain, origin.domain.as_ref()) {
            props.push(ArcProp::Domain(*domain));
        }

        if let Some(ident) = cmp_opt2(&self.ident, origin.ident.as_ref()) {
            props.push(ArcProp::Ident(ident.clone()));
        }

        diff_btree_maps(&self.vars, &origin.vars, |added, coord, token| {
            if added {
                props.push(ArcProp::Var(*coord, token.clone()));
            } else {
                todo!()
            }
        });

        diff_btree_maps(&self.slots, &origin.slots, |added, coord, token| {
            if added {
                props.push(ArcProp::SlotSymbol(*coord, token.clone()));
            } else {
                todo!("remove slot {coord:?} {token:?}")
            }
        });

        props.none_if_empty()
    }
}

impl ApplyIdent for Arc {
    fn ident(&self) -> Option<(Tag, &Token)> {
        match (self.domain, &self.ident) {
            (Some(domain), Some(ident)) => Some((domain, &ident.0)),
            _ => None,
        }
    }

    fn sym_entry(&self, tag: Tag) -> SymEntry {
        SymEntry::LocalEdge(tag)
    }
}

impl Rel {
    pub fn apply<'a>(
        &mut self,
        props: impl Iterator<Item = &'a RelProp>,
        tag: Tag,
        mut lk: Option<&mut SemLookup>,
    ) -> ApplyResult {
        for prop in props {
            match prop {
                RelProp::Domain(domain) => {
                    self.apply_key(tag, &mut lk, |zelf| {
                        zelf.domain = Some(*domain);
                    })?;
                }
                RelProp::DefCtxSet(_tag) => todo!(),
                RelProp::DefCtxRemove => todo!(),
                RelProp::Rel(t) => {
                    self.apply_key(tag, &mut lk, |zelf| {
                        zelf.rel_type = Some(t.clone());
                    })?;
                }
                RelProp::SubjCrd(rel_crd) => {
                    self.subj_cardinality = *rel_crd;
                }
                RelProp::ObjCrd(rel_crd) => {
                    self.obj_cardinality = *rel_crd;
                }
                RelProp::SubjAdd(ty) => {
                    self.subj_type_add(ty.clone(), tag, &mut lk)?;
                }
                RelProp::SubjRm(ty) => {
                    self.subj_type_rm(&ty.0, tag, &mut lk)?;
                }
                RelProp::ObjAdd(ty) => {
                    self.obj_type_add(ty.clone(), tag, &mut lk)?;
                }
                RelProp::ObjRm(ty) => {
                    self.obj_type_rm(&ty.0, tag, &mut lk)?;
                }
                RelProp::ObjPatternSet(pat) => {
                    self.obj_pattern_set(pat.clone(), tag, &mut lk)?;
                }
                RelProp::ObjPatternRm(_pat) => {
                    self.obj_pattern_rm(tag, &mut lk)?;
                }
            }
        }

        Ok(())
    }

    pub fn unapply(
        &mut self,
        props: &[RelProp],
        tag: Tag,
        lk: &mut SemLookup,
        history: &[EKind],
    ) -> ApplyResult {
        for prop in props {
            match prop {
                RelProp::Domain(_domain) => {
                    self.apply_key(tag, &mut Some(lk), |zelf| {
                        zelf.domain = history_rel_props(history, tag)
                            .find_map(variant_fn!(RelProp::Domain(tag)))
                            .cloned();
                    })?;
                }
                RelProp::DefCtxSet(_tag) => todo!(),
                RelProp::DefCtxRemove => todo!(),
                RelProp::Rel(_) => {
                    self.apply_key(tag, &mut Some(lk), |zelf| {
                        zelf.rel_type = history_rel_props(history, tag)
                            .find_map(variant_fn!(RelProp::Rel(tag)))
                            .cloned();
                    })?;
                }
                RelProp::SubjCrd(_) => {
                    self.subj_cardinality = history_rel_props(history, tag)
                        .find_map(variant_fn!(RelProp::SubjCrd(crd)))
                        .copied()
                        .unwrap_or(RelCrd::Unit);
                }
                RelProp::ObjCrd(_) => {
                    self.obj_cardinality = history_rel_props(history, tag)
                        .find_map(variant_fn!(RelProp::ObjCrd(crd)))
                        .copied()
                        .unwrap_or(RelCrd::Unit);
                }
                RelProp::SubjAdd(_) | RelProp::SubjRm(_) => {
                    self.subj_type = Either::Right(Default::default());
                    self.apply(
                        history_rel_props(history, tag).filter(|prop| {
                            matches!(prop, RelProp::SubjAdd(_) | RelProp::SubjRm(_))
                        }),
                        tag,
                        Some(lk),
                    )?;
                }
                RelProp::ObjAdd(_)
                | RelProp::ObjRm(_)
                | RelProp::ObjPatternSet(_)
                | RelProp::ObjPatternRm(_) => {
                    self.obj_type = TypeRefOrUnionOrPattern::Union(Default::default());
                    self.apply(
                        history_rel_props(history, tag).filter(|prop| {
                            matches!(
                                prop,
                                RelProp::ObjAdd(_)
                                    | RelProp::ObjRm(_)
                                    | RelProp::ObjPatternSet(_)
                                    | RelProp::ObjPatternRm(_)
                            )
                        }),
                        tag,
                        Some(lk),
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn key(&self) -> Result<RelKey, ApplyError> {
        Ok(RelKey {
            subject: self
                .subj_type
                .clone()
                .right_or_else(|t| [t].into_iter().collect()),
            relation: self.rel_type.clone().ok_or(ApplyError::RelWithoutKey)?.0,
            object: self.obj_type.clone(),
        })
    }

    pub fn diff(&self, origin: Option<&Rel>) -> Option<ThinVec<RelProp>> {
        let mut props = thin_vec![];

        if let Some(domain) = cmp_opt2(&self.domain, origin.and_then(|o| o.domain).as_ref()) {
            props.push(RelProp::Domain(*domain));
        }

        if let Some(relation) = cmp_opt2(&self.rel_type, origin.and_then(|o| o.rel_type.as_ref())) {
            props.push(RelProp::Rel(relation.clone()));
        }

        if let Some(origin) = origin {
            if self.subj_cardinality != origin.subj_cardinality {
                props.push(RelProp::SubjCrd(self.subj_cardinality));
            }
        } else {
            props.push(RelProp::SubjCrd(self.subj_cardinality));
        }

        if let Some(origin) = origin {
            diff_type_ref_or_union2(
                self.subj_type.as_ref(),
                origin.subj_type.as_ref(),
                |added, t| {
                    if added {
                        props.push(RelProp::SubjAdd(t.clone()));
                    } else {
                        props.push(RelProp::SubjRm(t.clone()));
                    }
                },
            );
        } else {
            for t in self.subj_type.as_ref().map_left(Some).into_iter() {
                props.push(RelProp::SubjAdd(t.clone()));
            }
        }

        if let Some(origin) = origin {
            if self.obj_cardinality != origin.obj_cardinality {
                props.push(RelProp::ObjCrd(self.obj_cardinality));
            }
        } else {
            props.push(RelProp::ObjCrd(self.obj_cardinality));
        }

        if let Some(origin) = origin {
            diff_type_ref_or_union_or_pattern(&self.obj_type, &origin.obj_type, |added, t| {
                props.push(match (added, t) {
                    (true, Either::Left(t)) => RelProp::ObjAdd(t.clone()),
                    (true, Either::Right(p)) => RelProp::ObjPatternSet(p.clone()),
                    (false, Either::Left(t)) => RelProp::ObjRm(t.clone()),
                    (false, Either::Right(p)) => RelProp::ObjPatternRm(p.clone()),
                })
            });
        } else {
            match &self.obj_type {
                TypeRefOrUnionOrPattern::Type(t) => props.push(RelProp::ObjAdd(t.clone())),
                TypeRefOrUnionOrPattern::Union(u) => {
                    for t in u {
                        props.push(RelProp::ObjAdd(t.clone()));
                    }
                }
                TypeRefOrUnionOrPattern::Pattern(pattern) => {
                    props.push(RelProp::ObjPatternSet(pattern.clone()));
                }
            }
        }

        props.none_if_empty()
    }

    fn subj_type_add(
        &mut self,
        ty: WithSpan<TypeRef>,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
    ) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| match &mut zelf.subj_type {
            Either::Right(set) if set.is_empty() => {
                zelf.subj_type = Either::Left(ty);
            }
            Either::Right(set) => {
                set.insert(ty);
            }
            Either::Left(old) => {
                zelf.subj_type = Either::Right([old.clone(), ty].into_iter().collect());
            }
        })
    }

    fn subj_type_rm(
        &mut self,
        ty: &TypeRef,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
    ) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| match &mut zelf.subj_type {
            Either::Left(_old) => {
                zelf.subj_type = Either::Right(Default::default());
            }
            Either::Right(set) => {
                set.remove(ty);
            }
        })
    }

    fn obj_type_add(
        &mut self,
        ty: WithSpan<TypeRef>,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
    ) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| match &mut zelf.obj_type {
            TypeRefOrUnionOrPattern::Type(old) => {
                zelf.obj_type =
                    TypeRefOrUnionOrPattern::Union([old.clone(), ty].into_iter().collect());
            }
            TypeRefOrUnionOrPattern::Union(u) if u.is_empty() => {
                zelf.obj_type = TypeRefOrUnionOrPattern::Type(ty);
            }
            TypeRefOrUnionOrPattern::Union(u) => {
                u.insert(ty);
            }
            TypeRefOrUnionOrPattern::Pattern(_pattern) => {
                zelf.obj_type = TypeRefOrUnionOrPattern::Type(ty);
            }
        })
    }

    fn obj_type_rm(
        &mut self,
        ty: &TypeRef,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
    ) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| match &mut zelf.obj_type {
            TypeRefOrUnionOrPattern::Type(_) => {
                zelf.obj_type = TypeRefOrUnionOrPattern::Union(Default::default());
            }
            TypeRefOrUnionOrPattern::Union(u) => {
                u.remove(ty);
            }
            TypeRefOrUnionOrPattern::Pattern(_) => {}
        })
    }

    fn obj_pattern_set(
        &mut self,
        pat: Pattern,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
    ) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| {
            zelf.obj_type = TypeRefOrUnionOrPattern::Pattern(pat);
        })
    }

    fn obj_pattern_rm(&mut self, tag: Tag, lk: &mut Option<&mut SemLookup>) -> ApplyResult {
        self.apply_key(tag, lk, |zelf| {
            zelf.obj_type = TypeRefOrUnionOrPattern::Union(Default::default());
        })
    }

    pub(super) fn apply_key(
        &mut self,
        tag: Tag,
        lk: &mut Option<&mut SemLookup>,
        f: impl FnOnce(&mut Self),
    ) -> ApplyResult {
        if let Some(lk) = lk {
            if let Ok(key) = self.key() {
                lk.rels_by_key.remove(&key);
            }
            f(self);
            if let Ok(key) = self.key() {
                lk.rels_by_key.insert(key, tag);
            }
        } else {
            f(self);
        }

        Ok(())
    }
}

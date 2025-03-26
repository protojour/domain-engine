use arcstr::ArcStr;
use fnv::{FnvHashMap, FnvHashSet};
use thin_vec::ThinVec;
use ulid::Ulid;

use crate::{
    log_model::{
        DefModifier, DefProp, DomainProp, EKind, FmtProp, RelCrd, RelProp, TypeRef, UseProp,
    },
    tag::{Tag, TagAllocator},
    token::Token,
};

pub struct LiveSnapshot {
    model: SnapshotModel,
    allocator: TagAllocator,
}

/// Represents a point in time
#[derive(Default, Debug)]
pub struct SnapshotModel {
    log_id: Ulid,
    pub domains: FnvHashMap<Tag, Domain>,
    pub imports: FnvHashMap<Tag, Use>,
    pub defs: FnvHashMap<Tag, Def>,
    pub rels: FnvHashMap<Tag, Rel>,
    pub fmts: FnvHashMap<Tag, Fmt>,

    pub string_cache: FnvHashSet<ArcStr>,
    pub import_uris: FnvHashMap<ArcStr, Tag>,
}

#[derive(Debug)]
pub enum Error {
    LogIdNotFound,
    DomainNotFound,
    DomainIdNotFound,
    ImportNotFound,
    DefNotFound,
    RelNotFound,
    FmtNotFound,
}

pub fn build_snapshot<'a>(
    mut kinds: impl Iterator<Item = &'a EKind>,
) -> Result<SnapshotModel, Error> {
    let Some(EKind::Start(ulid)) = kinds.next() else {
        return Err(Error::LogIdNotFound);
    };

    let mut out = SnapshotModel {
        log_id: *ulid.as_ref(),
        domains: Default::default(),
        defs: Default::default(),
        rels: Default::default(),
        imports: Default::default(),
        fmts: Default::default(),
        string_cache: Default::default(),
        import_uris: Default::default(),
    };

    while let Some(kind) = kinds.next() {
        out.add_event(kind)?;
    }

    Ok(out)
}

impl SnapshotModel {
    pub(crate) fn add_event(&mut self, event: &EKind) -> Result<(), Error> {
        match event {
            EKind::Start(id) => {
                self.log_id = *id.as_ref();
            }
            EKind::DomainAdd(tag, props) => {
                let mut domain = Domain {
                    id: Default::default(),
                    doc: None,
                };
                let mut has_id = false;
                domain.set_props(props, &mut has_id);
                if !has_id {
                    return Err(Error::DomainIdNotFound);
                }

                self.domains.insert(*tag, domain);
            }
            EKind::DomainChange(tag, props) => {
                let domain = self.domains.get_mut(tag).ok_or(Error::DomainNotFound)?;
                domain.set_props(props, &mut false);
            }
            EKind::DomainRemove(tag) => {
                self.domains.remove(tag);
            }
            EKind::UseAdd(tag, props) => {
                let mut _use = Use {
                    ident: None,
                    uri: None,
                };
                _use.update(props, *tag, &mut self.import_uris);
                self.imports.insert(*tag, _use);
            }
            EKind::UseChange(tag, props) => {
                self.imports
                    .get_mut(tag)
                    .ok_or(Error::ImportNotFound)?
                    .update(props, *tag, &mut self.import_uris);
            }
            EKind::UseRemove(tag) => {
                if let Some(_use) = self.imports.remove(tag) {
                    if let Some(uri) = _use.uri {
                        self.import_uris.remove(&uri);
                    }
                }
            }
            EKind::DefAdd(tag, props) => {
                let mut def = Def {
                    domain: None,
                    ident: None,
                    symbol: false,
                    doc: None,
                    modifiers: Default::default(),
                };
                def.update(props);
                self.defs.insert(*tag, def);
            }
            EKind::DefChange(tag, props) => {
                self.defs
                    .get_mut(tag)
                    .ok_or(Error::DefNotFound)?
                    .update(props);
            }
            EKind::DefRemove(tag) => {
                self.defs.remove(tag);
            }
            EKind::ArcAdd(tag, props) => {}
            EKind::ArcChange(tag, props) => {}
            EKind::RelAdd(tag, props) => {
                let mut rel = Rel {
                    domain: Tag(0),
                    def_context: None,
                    rel_type: None,
                    subj_cardinality: RelCrd::Unit,
                    subj_type: None,
                    obj_cardinality: RelCrd::OptUnit,
                    obj_type: None,
                };
                rel.update(props);
                self.rels.insert(*tag, rel);
            }
            EKind::RelChange(tag, props) => {
                self.rels
                    .get_mut(tag)
                    .ok_or(Error::RelNotFound)?
                    .update(props);
            }
            EKind::RelRemove(tag) => {
                self.rels.remove(tag);
            }
            EKind::FmtAdd(tag, props) => {
                let mut fmt = Fmt {
                    domain: Tag(0),
                    def_context: None,
                    transitions: Default::default(),
                };
                fmt.update(props);
                self.fmts.insert(*tag, fmt);
            }
            EKind::FmtChange(tag, props) => {
                self.fmts
                    .get_mut(tag)
                    .ok_or(Error::FmtNotFound)?
                    .update(props);
            }
            EKind::FmtRemove(tag) => {
                self.fmts.remove(tag);
            }
        }

        Ok(())
    }
}

fn type_add(target: &mut Option<TypeRef>, ty: &TypeRef) {}

fn type_rm(target: &mut Option<TypeRef>, ty: &TypeRef) {}

enum Literal {
    Text(ArcStr),
    Number(ArcStr),
}

#[derive(Debug)]
pub struct Domain {
    id: Ulid,
    doc: Option<ArcStr>,
    // import_uris: FnvHashMap<Arc>
}

impl Domain {
    fn set_props(&mut self, props: &[DomainProp], has_id: &mut bool) {
        for prop in props {
            match prop {
                DomainProp::Id(ulid) => {
                    self.id = *ulid;
                    *has_id = true;
                }
                DomainProp::Doc(doc) => {
                    self.doc = Some(doc.clone());
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Use {
    pub ident: Option<Token>,
    pub uri: Option<ArcStr>,
}

impl Use {
    fn update(&mut self, props: &[UseProp], tag: Tag, import_uris: &mut FnvHashMap<ArcStr, Tag>) {
        for prop in props {
            match prop {
                UseProp::Ident(ident) => {
                    self.ident = Some(ident.clone());
                }
                UseProp::Uri(uri) => {
                    if let Some(uri) = self.uri.take() {
                        import_uris.remove(&uri);
                    }
                    import_uris.insert(uri.clone(), tag);
                    self.uri = Some(uri.clone());
                }
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct Def {
    pub domain: Option<Tag>,
    pub ident: Option<Token>,
    pub doc: Option<ArcStr>,
    pub symbol: bool,
    pub modifiers: FnvHashSet<DefModifier>,
}

impl Def {
    fn update(&mut self, props: &[DefProp]) {
        for prop in props {
            match prop {
                DefProp::Domain(domain_tag) => {
                    self.domain = Some(*domain_tag);
                }
                DefProp::Ident(ident) | DefProp::ChangeIdent(ident, _) => {
                    self.ident = Some(ident.clone());
                }
                DefProp::SymSet => {
                    self.symbol = true;
                }
                DefProp::Doc(doc) => {
                    self.doc = Some(doc.clone());
                }
                DefProp::DocRemove => {
                    self.doc = None;
                }
                DefProp::ModAdd(m) => {
                    self.modifiers.insert(*m);
                }
                DefProp::ModRemove(m) => {
                    self.modifiers.remove(&m);
                }
            }
        }
    }
}

pub fn diff_def_modifier(
    existing: FnvHashSet<DefModifier>,
    modifiers: impl Iterator<Item = DefModifier>,
    out: &mut ThinVec<DefProp>,
) {
    for m in modifiers {
        if !existing.contains(&m) {
            out.push(DefProp::ModAdd(m));
        }
    }
}

#[derive(Debug)]
pub struct Rel {
    domain: Tag,
    def_context: Option<Tag>,
    rel_type: Option<TypeRef>,
    subj_cardinality: RelCrd,
    subj_type: Option<TypeRef>,
    obj_cardinality: RelCrd,
    obj_type: Option<TypeRef>,
}

impl Rel {
    fn update(&mut self, props: &[RelProp]) {
        for prop in props {
            match prop {
                RelProp::Domain(domain_tag) => {
                    self.domain = *domain_tag;
                }
                RelProp::DefCtxSet(def_tag) => {
                    self.def_context = Some(*def_tag);
                }
                RelProp::DefCtxRemove => {
                    self.def_context = None;
                }
                RelProp::Rel(type_ref) => {
                    self.rel_type = Some(type_ref.clone());
                }
                RelProp::SubjCrd(c) => {
                    self.subj_cardinality = *c;
                }
                RelProp::ObjCrd(c) => {
                    self.obj_cardinality = *c;
                }
                RelProp::SubjAdd(type_ref) => {
                    type_add(&mut self.subj_type, type_ref);
                }
                RelProp::SubjRm(type_ref) => {
                    type_rm(&mut self.subj_type, type_ref);
                }
                RelProp::ObjAdd(type_ref) => {
                    type_add(&mut self.obj_type, type_ref);
                }
                RelProp::ObjRm(type_ref) => {
                    type_rm(&mut self.obj_type, type_ref);
                }
                RelProp::ObjPatternSet(_pat) => {
                    todo!()
                }
                RelProp::ObjPatternRm => {
                    todo!()
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Fmt {
    domain: Tag,
    def_context: Option<Tag>,
    transitions: ThinVec<TypeRef>,
}

impl Fmt {
    fn update(&mut self, props: &[FmtProp]) {
        for prop in props {
            match prop {
                FmtProp::Domain(domain_tag) => {
                    self.domain = *domain_tag;
                }
                FmtProp::DefCtxSet(tag) => {
                    self.def_context = Some(*tag);
                }
                FmtProp::DefCtxRemove => {
                    self.def_context = None;
                }
                FmtProp::Transitions(transitions) => {
                    self.transitions = transitions.clone();
                }
            }
        }
    }
}

//! Semantic model combining syntax and log

use std::collections::{BTreeMap, hash_map::Entry};

use either::Either;
use fnv::FnvHashMap;
use ontol_core::{DomainId, LogRef, OntologyDomainId, span::U32Span};
use ontol_parser::{lexer::kind::Kind, topology::OntolHeaderData};
use tracing::debug;
use ulid::Ulid;

use crate::{
    error::ApplyError,
    log_model::{EKind, Log, TypeRefOrUnionOrPattern, ValueCrd},
    log_util::{Operation, traverse_reversible},
    lookup::{LookupScope, LookupTable},
    sem_match::{SyntaxKind, SyntaxTracker},
    sem_stmts::{ApplyIdent, Arc, Def, Rel, Use},
    symbol::{SymEntry, SymEntryRef},
    tables::{RelKey, ontol_table},
    tag::{Tag, TagAllocator},
    token::Token,
};

pub struct GlobalSemModel {
    logs: FnvHashMap<LogRef, SemModel>,
    ontol: FnvHashMap<&'static str, SymEntry>,
}

/// Semantic model projected for one subdomain (in one log)
pub struct ProjectedSemModel {
    local_log: LogRef,
    pub(crate) subdomain: Tag,
    pub(crate) local: SemModel,
    foreign: FnvHashMap<LogRef, SemModel>,
    ontol: FnvHashMap<&'static str, SymEntry>,
}

/// Semantic model for one log
#[derive(Default, Debug)]
pub struct SemModel {
    pub(crate) ulid: Ulid,
    pub(crate) ulid_stable: bool,

    pub uses: BTreeMap<Tag, Use>,
    pub defs: BTreeMap<Tag, Def>,
    pub arcs: BTreeMap<Tag, Arc>,
    pub rels: BTreeMap<Tag, Rel>,
    pub(crate) lookup: SemLookup,

    pub(crate) allocator: TagAllocator,

    pub(crate) domain_syntax: FnvHashMap<Tag, std::sync::Arc<SyntaxTracker>>,
}

#[derive(Default, Debug)]
pub(crate) struct SemLookup {
    /// Identifiers by (domain, token)
    idents: FnvHashMap<(Tag, Token), SymEntry>,
    pub(crate) rels_by_key: BTreeMap<RelKey, Tag>,
}

pub(crate) enum StmtNode {
    Use(Use),
    Def(Def),
    Arc(Arc),
    Rel(Rel),
    Todo,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StmtRef<'a> {
    Use(&'a Use),
    Def(&'a Def),
    Arc(&'a Arc),
    Rel(&'a Rel),
}

impl StmtRef<'_> {
    pub fn syntax_kind(self) -> SyntaxKind {
        match self {
            Self::Use(_) => SyntaxKind::Use,
            Self::Def(_) => SyntaxKind::Def,
            Self::Arc(_) => SyntaxKind::Arc,
            Self::Rel(_) => SyntaxKind::Rel,
        }
    }
}

pub(crate) type ApplyResult = Result<(), ApplyError>;

impl Default for GlobalSemModel {
    fn default() -> Self {
        Self {
            logs: Default::default(),
            ontol: ontol_table(),
        }
    }
}

impl GlobalSemModel {
    pub fn project(mut self, log: LogRef, subdomain: Tag) -> ProjectedSemModel {
        let local = self.logs.remove(&log).unwrap_or_default();

        ProjectedSemModel {
            local_log: log,
            subdomain,
            local,
            foreign: self.logs,
            ontol: self.ontol,
        }
    }

    pub fn new_subdomain(&mut self, log: LogRef) -> Tag {
        let model = self.logs.entry(log).or_default();
        let subdomain = model.allocator.next_tag.bump();

        model.lookup.idents.insert(
            (subdomain, Token::new(Kind::Symbol, "ontol")),
            SymEntry::Ontol,
        );

        subdomain
    }

    pub fn get_model(&self, log: LogRef) -> Option<&SemModel> {
        self.logs.get(&log)
    }

    pub fn get_model_mut(&mut self, log: LogRef) -> Option<&mut SemModel> {
        self.logs.get_mut(&log)
    }

    pub fn subdomain_header_data(
        &self,
        log_ref: LogRef,
        subdomain: Tag,
        _with_docs: ontol_parser::topology::WithDocs,
        _errors: &mut Vec<impl ontol_parser::topology::MakeParseError>,
    ) -> OntolHeaderData {
        let domain_id = match self.get_model(log_ref) {
            Some(model) => OntologyDomainId {
                id: DomainId {
                    ulid: model.ulid,
                    subdomain: subdomain.0,
                },
                stable: model.ulid_stable,
            },
            None => OntologyDomainId {
                id: DomainId {
                    ulid: Ulid::new(),
                    subdomain: subdomain.0,
                },
                stable: false,
            },
        };

        OntolHeaderData {
            domain_docs: None,
            domain_id: (domain_id, U32Span::default()),
            name: ("".to_string(), U32Span::default()),
            deps: vec![],
        }
    }
}

impl ProjectedSemModel {
    pub fn unproject(self) -> GlobalSemModel {
        let mut logs = self.foreign;
        logs.insert(self.local_log, self.local);

        GlobalSemModel {
            logs,
            ontol: self.ontol,
        }
    }

    pub fn subdomain(&self) -> Tag {
        self.subdomain
    }
}

impl SemModel {
    pub fn apply_log(&mut self, log: &mut Log) -> ApplyResult {
        for event in log.all() {
            self.apply_one(event)?;
        }

        Ok(())
    }

    /// Temporary hacky method for testing
    pub fn set_uid(&mut self, id: OntologyDomainId) {
        self.ulid = id.id.ulid;
        self.ulid_stable = id.stable;
    }

    /// increase log sync point atomically
    pub fn sync_to_log(&mut self, log: &mut Log) -> ApplyResult {
        traverse_reversible(log.unsynced(), |op, index, event| match op {
            Operation::Do => self.apply_one(event),
            Operation::Undo => self.unapply_one(event, log.history_slice(index)),
        })?;

        log.lock_sync();

        Ok(())
    }

    pub fn apply_one(&mut self, event: &EKind) -> ApplyResult {
        debug!("apply one {event:?}");

        match event {
            EKind::Start(ulid) => {
                self.ulid = *ulid.as_ref();
                self.ulid_stable = true;
            }
            EKind::SubAdd(_tag, _props) => {}
            EKind::SubChange(_tag, _props) => {}
            EKind::SubRemove(_tag) => {}
            EKind::UseAdd(tag, props) => {
                let mut u = Use::default();
                u.apply(props, *tag, &mut self.lookup)?;
                self.uses.insert(*tag, u);
            }
            EKind::UseChange(tag, props) => {
                let u = self.uses.get_mut(tag).ok_or(ApplyError::UseNotFound)?;
                u.apply(props, *tag, &mut self.lookup)?;
            }
            EKind::UseRemove(tag) => {
                let u = self.uses.get_mut(tag).ok_or(ApplyError::UseNotFound)?;
                u.apply_ident(*tag, &mut self.lookup, |u| {
                    u.subdomain = None;
                })?;
                self.uses.remove(tag);
            }
            EKind::DefAdd(tag, props) => {
                let mut def = Def::default();
                def.apply(props, *tag, &mut self.lookup)?;
                self.defs.insert(*tag, def);
            }
            EKind::DefChange(tag, props) => {
                let def = self.defs.get_mut(tag).ok_or(ApplyError::DefNotFound)?;
                def.apply(props, *tag, &mut self.lookup)?;
            }
            EKind::DefRemove(tag) => {
                let def = self.defs.get_mut(tag).ok_or(ApplyError::DefNotFound)?;
                def.apply_ident(*tag, &mut self.lookup, |def| {
                    def.subdomain = None;
                })?;
                self.defs.remove(tag);
            }
            EKind::ArcAdd(tag, props) => {
                let mut arc = Arc::default();
                arc.apply(props, *tag, &mut self.lookup)?;
                self.arcs.insert(*tag, arc);
            }
            EKind::ArcChange(tag, props) => {
                let arc = self.arcs.get_mut(tag).ok_or(ApplyError::ArcNotFound)?;
                arc.apply(props, *tag, &mut self.lookup)?;
            }
            EKind::ArcRemove(tag) => {
                let arc = self.arcs.get_mut(tag).ok_or(ApplyError::ArcNotFound)?;
                arc.apply_ident(*tag, &mut self.lookup, |arc| {
                    arc.subdomain = None;
                })?;
                self.arcs.remove(tag);
            }
            EKind::RelAdd(tag, props) => {
                let mut rel = Rel {
                    subdomain: None,
                    parent_rel: None,
                    rel_type: None,
                    opt: false,
                    subj_cardinality: ValueCrd::Unit,
                    subj_type: Either::Right(Default::default()),
                    obj_cardinality: ValueCrd::Unit,
                    obj_type: TypeRefOrUnionOrPattern::Union(Default::default()),
                };
                rel.apply(props.iter(), *tag, None)?;
                self.lookup.rels_by_key.insert(rel.key()?, *tag);
                self.rels.insert(*tag, rel);
            }
            EKind::RelChange(tag, props) => {
                let rel = self.rels.get_mut(tag).ok_or(ApplyError::RelNotFound)?;
                rel.apply(props.iter(), *tag, Some(&mut self.lookup))?;
            }
            EKind::RelRemove(tag) => {
                let rel = self.rels.get_mut(tag).ok_or(ApplyError::RelNotFound)?;
                rel.apply_key(*tag, &mut Some(&mut self.lookup), |rel| {
                    rel.subdomain = None;
                })?;
                self.rels.remove(tag);
            }
            EKind::FmtAdd(_tag, _props) => todo!(),
            EKind::FmtChange(_tag, _props) => todo!(),
            EKind::FmtRemove(_tag) => todo!(),
        }

        Ok(())
    }

    pub fn unapply_one(&mut self, event: &EKind, history: &[EKind]) -> ApplyResult {
        match event {
            EKind::Start(_ulid) => {}
            EKind::SubAdd(_tag, _props) => {}
            EKind::SubChange(_tag, _props) => {}
            EKind::SubRemove(_tag) => {}
            EKind::UseAdd(tag, _props) => {
                self.apply_one(&EKind::UseRemove(*tag))?;
            }
            EKind::UseChange(tag, props) => {
                let u = self.uses.get_mut(tag).ok_or(ApplyError::UseNotFound)?;
                u.unapply(props, *tag, &mut self.lookup, history)?;
            }
            EKind::UseRemove(tag) => {
                self.apply_many(history.iter().filter(|e| match e {
                    EKind::UseAdd(t, _) | EKind::UseChange(t, _) if t == tag => true,
                    _ => false,
                }))?;
            }
            EKind::DefAdd(tag, _props) => {
                self.apply_one(&EKind::DefRemove(*tag))?;
            }
            EKind::DefChange(tag, props) => {
                let def = self.defs.get_mut(tag).ok_or(ApplyError::DefNotFound)?;
                def.unapply(props, *tag, &mut self.lookup, history)?;
            }
            EKind::DefRemove(tag) => {
                self.apply_many(history.iter().filter(|e| match e {
                    EKind::DefAdd(t, _) | EKind::DefChange(t, _) if t == tag => true,
                    _ => false,
                }))?;
            }
            EKind::ArcAdd(tag, _props) => {
                self.apply_one(&EKind::ArcRemove(*tag))?;
            }
            EKind::ArcChange(tag, props) => {
                let arc = self.arcs.get_mut(tag).ok_or(ApplyError::ArcNotFound)?;
                arc.unapply(props, *tag, &mut self.lookup, history)?;
            }
            EKind::ArcRemove(tag) => {
                self.apply_many(history.iter().filter(|e| match e {
                    EKind::ArcAdd(t, _) | EKind::ArcChange(t, _) if t == tag => true,
                    _ => false,
                }))?;
            }
            EKind::RelAdd(tag, _props) => {
                self.apply_one(&EKind::RelRemove(*tag))?;
            }
            EKind::RelChange(tag, props) => {
                let rel = self.rels.get_mut(tag).ok_or(ApplyError::RelNotFound)?;
                rel.unapply(props, *tag, &mut self.lookup, history)?;
            }
            EKind::RelRemove(tag) => {
                self.apply_many(history.iter().filter(|e| match e {
                    EKind::RelAdd(t, _) | EKind::RelChange(t, _) if t == tag => true,
                    _ => false,
                }))?;
            }
            EKind::FmtAdd(_tag, _props) => todo!(),
            EKind::FmtChange(_tag, _props) => todo!(),
            EKind::FmtRemove(_tag) => todo!(),
        }

        Ok(())
    }

    fn apply_many<'e>(&mut self, events: impl Iterator<Item = &'e EKind>) -> ApplyResult {
        for kind in events {
            self.apply_one(kind)?;
        }

        Ok(())
    }
}

impl LookupTable for ProjectedSemModel {
    fn lookup_root(
        &self,
        token: &ontol_syntax::rowan::GreenToken,
    ) -> Option<crate::symbol::SymEntryRef<'_>> {
        self.local
            .lookup
            .idents
            .get(&(self.subdomain, Token(token.clone())))
            .or_else(|| self.ontol.get(token.text()))
            .map(SymEntry::entry_ref)
    }

    fn lookup_next<'s>(
        &'s self,
        scope: crate::lookup::LookupScope,
        prev: crate::symbol::SymEntryRef<'s>,
        token: &ontol_syntax::rowan::GreenToken,
    ) -> Option<crate::symbol::SymEntryRef<'s>> {
        let (sem_model, _domain) = match scope {
            LookupScope::Local => (&self.local, self.subdomain),
            LookupScope::Foreign(log_ref, domain_tag) => (self.foreign.get(&log_ref)?, domain_tag),
        };

        match prev {
            SymEntryRef::Use(_tag) => todo!("foreign lookup"),
            SymEntryRef::LocalDef(_tag) => None,
            SymEntryRef::LocalEdge(edge_tag) => sem_model.arcs.get(&edge_tag).and_then(|arc| {
                arc.slots
                    .iter()
                    .find(|(_coord, sym)| &sym.0.0 == token)
                    .map(|(coord, _sym)| SymEntryRef::EdgeSymbol(edge_tag, *coord))
            }),
            SymEntryRef::Ontol => self.ontol.get(token.text()).map(SymEntry::entry_ref),
            SymEntryRef::OntolModule(table) => table.get(token.text()).map(SymEntry::entry_ref),
            SymEntryRef::EdgeSymbol(..) | SymEntryRef::OntolDef(_) => None,
        }
    }
}

impl SemLookup {
    pub fn use_by_ident(&self, domain: Tag, ident: &Token) -> Option<Tag> {
        self.idents
            .get(&(domain, ident.clone()))?
            .entry_ref()
            .use_tag()
    }

    pub fn def_by_ident(&self, domain: Tag, ident: &Token) -> Option<Tag> {
        self.idents
            .get(&(domain, ident.clone()))?
            .entry_ref()
            .local_def()
    }

    pub fn arc_by_ident(&self, domain: Tag, ident: &Token) -> Option<Tag> {
        self.idents
            .get(&(domain, ident.clone()))?
            .entry_ref()
            .local_arc()
    }

    pub fn insert_sym_entry(&mut self, domain: Tag, ident: Token, entry: SymEntry) -> ApplyResult {
        match self.idents.entry((domain, ident)) {
            Entry::Vacant(vacant) => {
                vacant.insert(entry);
                Ok(())
            }
            Entry::Occupied(_) => Err(ApplyError::DuplicateIdent),
        }
    }

    pub fn remove_sym_entry(&mut self, domain: Tag, ident: &Token) -> ApplyResult {
        match self.idents.entry((domain, ident.clone())) {
            Entry::Occupied(occ) => {
                occ.remove();
                Ok(())
            }
            Entry::Vacant(_) => Err(ApplyError::IdentNotRemovable),
        }
    }
}

#[cfg(test)]
mod tests {
    use ontol_macros::test;
    use thin_vec::thin_vec;

    use crate::{
        log_model::{DefProp, EKind, Log},
        sem_model::ApplyError,
        symbol::SymEntry,
        tag::Tag,
        token::Token,
        with_span::SetSpan,
    };

    use super::SemModel;

    #[test]
    fn test_undo1() {
        let mut log = Log::default();
        let mut model = SemModel::default();
        let domain = Tag(0);
        log.stage(EKind::DefAdd(
            Tag(1),
            thin_vec![
                DefProp::Sub(domain),
                DefProp::Ident(Token::symbol("foo").no_span())
            ],
        ));

        model.sync_to_log(&mut log).unwrap();

        log.stage(EKind::DefAdd(
            Tag(666),
            thin_vec![
                DefProp::Sub(domain),
                DefProp::Ident(Token::symbol("foo").no_span())
            ],
        ));

        assert!(matches!(
            model.sync_to_log(&mut log),
            Err(ApplyError::DuplicateIdent)
        ));

        assert_eq!(log.unsynced().len(), 1);

        assert!(matches!(
            model.lookup.idents.get(&(domain, Token::symbol("foo"))),
            Some(SymEntry::LocalDef(Tag(1))),
        ));
    }
}

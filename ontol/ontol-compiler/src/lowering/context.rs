//! utilities for lowering ontol-parser output

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
};

use arcstr::ArcStr;
use fnv::FnvHashMap;
use indexmap::map::Entry;
use ontol_parser::{
    cst::view::{NodeView, TokenView},
    ParserError, U32Span,
};
use ontol_runtime::{
    property::{PropertyCardinality, ValueCardinality},
    var::{Var, VarAllocator},
    DefId, PackageId,
};
use tracing::debug;

use crate::{
    def::{Def, DefKind, TypeDef, TypeDefFlags},
    fmt::FmtChain,
    namespace::Space,
    package::ONTOL_PKG,
    pattern::{Pattern, PatternKind},
    relation::{DefRelTag, RelId, RelParams, Relationship},
    CompileError, Compiler, SourceId, SourceSpan,
};

pub struct LoweringCtx<'c, 'm> {
    pub compiler: &'c mut Compiler<'m>,
    pub pkg_def_id: DefId,
    pub package_id: PackageId,
    pub source_id: SourceId,
    pub anonymous_unions: FnvHashMap<BTreeSet<DefId>, DefId>,
    pub outcome: LoweringOutcome,
}

pub struct CstLowering<'c, 'm, V: NodeView> {
    pub(super) ctx: LoweringCtx<'c, 'm>,
    pub(super) _phantom: PhantomData<V>,
}

pub type LoweringError = (CompileError, U32Span);
pub type Res<T> = Result<T, LoweringError>;
pub type LoweredRel = (DefRelTag, Relationship, SourceSpan, Option<ArcStr>);

#[derive(Default)]
pub struct LoweringOutcome {
    pub root_defs: Vec<DefId>,
    pub rels: BTreeMap<PackageId, BTreeMap<u16, Vec<LoweredRel>>>,
    pub fmt_chains: Vec<(DefId, FmtChain)>,
}

impl LoweringOutcome {
    pub fn predefine_rel(
        &mut self,
        rel_id: RelId,
        relationship: Relationship,
        span: SourceSpan,
        docs: Option<ArcStr>,
    ) {
        let RelId(DefId(pkg_id, def_tag), rel_tag) = rel_id;

        self.rels
            .entry(pkg_id)
            .or_default()
            .entry(def_tag)
            .or_default()
            .push((rel_tag, relationship, span, docs));
    }
}

pub type RootDefs = Vec<DefId>;

pub enum Coinage {
    New,
    Used,
}

pub struct Open(pub Option<U32Span>);
pub struct Private(pub Option<U32Span>);
pub struct Extern(pub Option<U32Span>);
pub struct Macro(pub Option<U32Span>);

#[derive(Clone, Copy)]
pub enum RelationKey {
    Named(DefId),
    Builtin(DefId),
    Indexed,
}

pub struct SetElement {
    pub iter: bool,
    pub rel: Option<Pattern>,
    pub val: Pattern,
}

#[derive(Default)]
pub struct MapVarTable {
    variables: HashMap<std::string::String, Var>,
}

impl MapVarTable {
    pub fn get_or_create_var(&mut self, ident: String) -> Var {
        let length = self.variables.len();

        *self
            .variables
            .entry(ident)
            .or_insert_with(|| Var(length as u32))
    }

    /// Create an allocator for allocating the successive variables
    /// after the explicit ones
    pub fn into_allocator(self) -> VarAllocator {
        VarAllocator::from(Var(self.variables.len() as u32))
    }
}

pub enum BlockContext<'a> {
    NoContext,
    SubDef(&'a dyn Fn() -> DefId),
    RelParams {
        def_fn: &'a dyn Fn() -> DefId,
        relation_modifiers: &'a mut Vec<(Relationship, SourceSpan)>,
    },
    FmtLeading,
}

impl<'a> BlockContext<'a> {
    pub fn def_func(&self) -> Option<&'a dyn Fn() -> DefId> {
        match self {
            BlockContext::NoContext => None,
            BlockContext::SubDef(f) => Some(*f),
            BlockContext::RelParams { def_fn, .. } => Some(*def_fn),
            BlockContext::FmtLeading => None,
        }
    }
}

impl<'c, 'm> LoweringCtx<'c, 'm> {
    pub fn source_span(&self, span: U32Span) -> SourceSpan {
        SourceSpan {
            source_id: self.source_id,
            span,
        }
    }

    pub fn lookup_ident(&mut self, ident: &str, span: U32Span) -> Result<DefId, LoweringError> {
        // A single ident looks in both ONTOL_PKG and the current package
        match self
            .compiler
            .namespaces
            .lookup(&[self.package_id, ONTOL_PKG], Space::Type, ident)
        {
            Some(def_id) => Ok(def_id),
            None => Err((CompileError::DefinitionNotFound, span)),
        }
    }

    pub fn lookup_path<V: NodeView>(
        &mut self,
        segments: impl Iterator<Item = V::Token>,
        path_span: U32Span,
    ) -> Result<DefId, LoweringError> {
        let mut segment_iter = segments.peekable();

        let Some(mut current) = segment_iter.next() else {
            return Err((CompileError::TODO("empty path"), path_span));
        };

        if segment_iter.peek().is_some() {
            // a path is fully qualified

            // a path is fully qualified
            let mut namespace = self
                .compiler
                .namespaces
                .namespaces
                .get(&self.package_id)
                .unwrap();

            let mut def_id;

            loop {
                let segment = self.compiler.str_ctx.intern(current.slice());
                def_id = namespace.space(Space::Type).get(segment);
                if segment_iter.peek().is_some() {
                    match def_id {
                        Some(def_id) => match self.compiler.defs.def_kind(*def_id) {
                            DefKind::Package(package_id) => {
                                namespace =
                                    self.compiler.namespaces.namespaces.get(package_id).unwrap();
                            }
                            other => {
                                debug!("namespace not found. def kind was {other:?}");
                                return Err((CompileError::NamespaceNotFound, current.span()));
                            }
                        },
                        None => return Err((CompileError::NamespaceNotFound, current.span())),
                    }
                }

                match segment_iter.next() {
                    Some(next) => current = next,
                    None => break,
                }
            }

            match def_id {
                Some(def_id) => match self.compiler.defs.def_kind(*def_id) {
                    DefKind::Type(TypeDef { flags, .. })
                        if !flags.contains(TypeDefFlags::PUBLIC) =>
                    {
                        Err((CompileError::PrivateDefinition, path_span))
                    }
                    _ => Ok(*def_id),
                },
                None => Err((CompileError::DefinitionNotFound, path_span)),
            }
        } else {
            self.lookup_ident(current.slice(), current.span())
        }
    }

    pub fn coin_type_definition(
        &mut self,
        ident: &str,
        ident_span: U32Span,
        private: Private,
        open: Open,
        extern_: Extern,
        macro_: Macro,
    ) -> Result<DefId, LoweringError> {
        let (def_id, coinage) = self.named_def_id(Space::Type, ident, ident_span)?;
        if matches!(coinage, Coinage::New) {
            let ident = self.compiler.str_ctx.intern(ident);
            debug!("{def_id:?}: `{}`", ident);

            let kind = if macro_.0.is_some() {
                DefKind::Macro(ident)
            } else if extern_.0.is_some() {
                DefKind::Extern(ident)
            } else {
                let mut flags = TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC;

                if private.0.is_some() {
                    flags.remove(TypeDefFlags::PUBLIC);
                }

                if open.0.is_some() {
                    flags.insert(TypeDefFlags::OPEN);
                }

                DefKind::Type(TypeDef {
                    ident: Some(ident),
                    rel_type_for: None,
                    flags,
                })
            };

            self.set_def_kind(def_id, kind, ident_span);
        }

        Ok(def_id)
    }

    pub fn coin_symbol(
        &mut self,
        ident: &str,
        ident_span: U32Span,
    ) -> Result<DefId, LoweringError> {
        let (def_id, coinage) = self.named_def_id(Space::Type, ident, ident_span)?;
        if matches!(coinage, Coinage::New) {
            let ident = self.compiler.str_ctx.intern(ident);
            debug!("{def_id:?}: `{}`", ident);

            let kind = DefKind::Type(TypeDef {
                ident: Some(ident),
                rel_type_for: None,
                flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
            });

            self.set_def_kind(def_id, kind, ident_span);

            let span = self.source_span(ident_span);
            let ident_literal = self
                .compiler
                .defs
                .def_text_literal(ident, &mut self.compiler.str_ctx);

            let rel_id = self.compiler.rel_ctx.alloc_rel_id(def_id);
            self.outcome.predefine_rel(
                rel_id,
                Relationship {
                    relation_def_id: self.compiler.primitives.relations.is,
                    edge_projection: None,
                    relation_span: span,
                    subject: (def_id, span),
                    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                    object: (ident_literal, span),
                    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                    rel_params: RelParams::Unit,
                    macro_source: None,
                    modifiers: vec![],
                },
                self.source_span(ident_span),
                None,
            );
        }

        Ok(def_id)
    }

    pub fn define_relation_if_undefined(&mut self, key: RelationKey) -> DefId {
        match key {
            RelationKey::Named(def_id) => def_id,
            RelationKey::Builtin(def_id) => def_id,
            RelationKey::Indexed => self.compiler.primitives.relations.indexed,
        }
    }

    pub fn named_def_id(
        &mut self,
        space: Space,
        ident: &str,
        span: U32Span,
    ) -> Result<(DefId, Coinage), LoweringError> {
        let ident = self.compiler.str_ctx.intern(ident);
        match self
            .compiler
            .namespaces
            .get_namespace_mut(self.package_id, space)
            .entry(ident)
        {
            Entry::Occupied(occupied) => {
                if occupied.get().package_id() == self.package_id {
                    Ok((*occupied.get(), Coinage::Used))
                } else {
                    Err((
                        CompileError::TODO("definition of external identifier"),
                        span,
                    ))
                }
            }
            Entry::Vacant(vacant) => {
                let def_id = self.compiler.defs.alloc_def_id(self.package_id);
                vacant.insert(def_id);
                Ok((def_id, Coinage::New))
            }
        }
    }

    pub fn define_anonymous_type(&mut self, type_def: TypeDef<'m>, span: U32Span) -> DefId {
        let anonymous_def_id = self.compiler.defs.alloc_def_id(self.package_id);
        self.set_def_kind(anonymous_def_id, DefKind::Type(type_def), span);
        debug!("{anonymous_def_id:?}: <anonymous>");
        anonymous_def_id
    }

    pub fn define_anonymous(&mut self, kind: DefKind<'m>, span: U32Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id(self.package_id);
        self.set_def_kind(def_id, kind, span);
        def_id
    }

    pub fn set_def_kind(&mut self, def_id: DefId, kind: DefKind<'m>, span: U32Span) {
        self.compiler.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                package: self.package_id,
                kind,
                span: self.source_span(span),
            },
        );
    }

    pub fn mk_pattern(&mut self, kind: PatternKind, span: U32Span) -> Pattern {
        Pattern {
            id: self.compiler.patterns.alloc_pat_id(),
            kind,
            span: self.source_span(span),
        }
    }

    pub(super) fn unescape(&mut self, result: Result<String, Vec<ParserError>>) -> Option<String> {
        match result {
            Ok(string) => Some(string),
            Err(unescape_errors) => {
                for error in unescape_errors {
                    CompileError::TODO(error.msg).span_report(error.span, self);
                }

                None
            }
        }
    }
}

impl CompileError {
    pub(super) fn span_report(self, span: U32Span, ctx: &mut LoweringCtx) {
        self.span(ctx.source_span(span)).report(ctx.compiler);
    }
}

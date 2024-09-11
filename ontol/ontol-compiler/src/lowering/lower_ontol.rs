use std::{collections::BTreeMap, marker::PhantomData, rc::Rc, str::FromStr};

use ontol_parser::cst::{
    inspect::{self as insp},
    view::{NodeView, TokenView},
};
use ontol_runtime::{ontology::domain::DomainId, tuple::CardinalIdx, DefId};
use tracing::debug_span;
use ulid::Ulid;

use crate::{
    edge::{CardinalKind, EdgeId},
    namespace::{DocId, Space},
    topology::DomainUrl,
    CompileError, Compiler, Src,
};

use super::{
    context::{
        BlockContext, CstLowering, Extern, LoweringCtx, LoweringOutcome, Macro, Open, Private,
        RootDefs,
    },
    lower_misc::ReportError,
};

enum PreDefinedStmt<V> {
    Domain(insp::DomainStatement<V>),
    Def(DefId, insp::DefStatement<V>),
    Sym(Vec<DefId>),
    Rel(insp::RelStatement<V>),
    Arc {
        edge_id: EdgeId,
        root_defs: RootDefs,
        params: BTreeMap<CardinalIdx, insp::ArcTypeParam<V>>,
    },
    Fmt(insp::FmtStatement<V>),
    Map(insp::MapStatement<V>),
}

/// General section of an ONTOL document
enum Section {
    Domain,
    Use,
    Body,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub fn new(
        url: Rc<DomainUrl>,
        domain_def_id: DefId,
        src: Src,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                url,
                domain_def_id,
                domain_index: src.domain_index,
                source_id: src.id,
                anonymous_unions: Default::default(),
                outcome: Default::default(),
                domain_url_parser: Default::default(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn finish(self) -> LoweringOutcome {
        self.ctx.outcome
    }

    /// Entry point for lowering a top-level ONTOL domain syntax node
    pub fn lower_ontol(mut self, ontol: insp::Node<V>) -> Self {
        let insp::Node::Ontol(ontol) = ontol else {
            // This is a pre-reported parser error, so don't report here
            return self;
        };

        let mut pre_defined_statements = vec![];
        let mut section = Section::Domain;

        // first pass: Register all named definitions into the namespace
        for statement in ontol.statements() {
            if let Some(pre_defined) = self.pre_define_statement(statement, &mut section) {
                pre_defined_statements.push(pre_defined);
            }
        }

        // second pass: Handle inner bodies, etc
        for stmt in pre_defined_statements {
            if let Some(mut defs) = self.lower_pre_defined(stmt, BlockContext::NoContext) {
                self.ctx.outcome.root_defs.append(&mut defs);
            }
        }

        self
    }

    pub(super) fn lower_statement(
        &mut self,
        statement: insp::Statement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match statement {
            insp::Statement::DomainStatement(stmt) => {
                self.append_documentation(DocId::Def(self.ctx.domain_def_id), stmt.0.clone());

                let domain_id = stmt.domain_id()?;

                let Some(domain_ulid) = domain_id
                    .try_concat_ulid()
                    .and_then(|ulid| Ulid::from_str(&ulid).ok())
                else {
                    CompileError::TODO("misformatted domain id")
                        .span_report(domain_id.view().span(), &mut self.ctx);
                    return None;
                };

                if self
                    .ctx
                    .compiler
                    .domain_ids
                    .values()
                    .any(|domain_id| domain_id.ulid == domain_ulid)
                {
                    CompileError::TODO("domain has already been compiled")
                        .span_report(domain_id.view().span(), &mut self.ctx);
                    return None;
                }

                self.ctx.compiler.domain_ids.insert(
                    self.ctx.domain_index,
                    DomainId {
                        ulid: domain_ulid,
                        stable: true,
                    },
                );

                None
            }
            insp::Statement::UseStatement(_use_stmt) => None,
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, macro_) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                        macro_,
                    )
                })?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.lower_def_body(def_id, def_stmt)?);
                Some(root_defs)
            }
            insp::Statement::SymStatement(_) => None,
            insp::Statement::ArcStatement(_) => None,
            insp::Statement::RelStatement(rel_stmt) => {
                self.lower_rel_statement(rel_stmt, block_context)
            }
            insp::Statement::FmtStatement(fmt_stmt) => {
                self.lower_fmt_statement(fmt_stmt, block_context)
            }
            insp::Statement::MapStatement(map_stmt) => {
                let def_id = self.lower_map_statement(map_stmt, block_context)?;
                Some(vec![def_id])
            }
        }
    }

    fn lower_def_body(&mut self, def_id: DefId, stmt: insp::DefStatement<V>) -> Option<RootDefs> {
        self.append_documentation(DocId::Def(def_id), stmt.0.clone());

        let mut root_defs: RootDefs = [def_id].into();

        if let Some(body) = stmt.body() {
            let _entered = debug_span!("def", id = ?def_id).entered();

            // The inherent relation block on the type uses the just defined
            // type as its context
            let context_fn = move || def_id;

            for statement in body.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::SubDef(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }
        }

        Some(root_defs)
    }

    fn pre_define_statement(
        &mut self,
        statement: insp::Statement<V>,
        section: &mut Section,
    ) -> Option<PreDefinedStmt<V>> {
        match &statement {
            insp::Statement::DomainStatement(stmt) => {
                if !matches!(section, Section::Domain) {
                    CompileError::TODO("misplaced domain header")
                        .span_report(stmt.view().span(), &mut self.ctx);
                }

                *section = Section::Use;
            }
            insp::Statement::UseStatement(stmt) => {
                if !matches!(section, Section::Domain | Section::Use) {
                    CompileError::TODO("misplaced use statement")
                        .span_report(stmt.view().span(), &mut self.ctx);
                }
                *section = Section::Use;
            }
            _ => {
                *section = Section::Body;
            }
        }

        match statement {
            insp::Statement::DomainStatement(stmt) => Some(PreDefinedStmt::Domain(stmt)),
            insp::Statement::UseStatement(use_stmt) => {
                let uri = use_stmt.uri()?;
                let uri_text = uri.text().and_then(|result| self.ctx.unescape(result))?;

                let Ok(reference) = self.ctx.domain_url_parser.parse(&uri_text) else {
                    return None;
                };

                let url = self.ctx.url.join(&reference);

                let Some(used_package_def_id) = self.ctx.compiler.loaded.by_url.get(&url) else {
                    CompileError::DomainNotFound(url).span_report(uri.0.span(), &mut self.ctx);
                    return None;
                };

                // insert entry into domain dep graph
                self.ctx
                    .compiler
                    .domain_dep_graph
                    .entry(self.ctx.domain_index)
                    .or_default()
                    .insert(used_package_def_id.domain_index());

                let type_namespace = self
                    .ctx
                    .compiler
                    .namespaces
                    .get_namespace_mut(self.ctx.domain_def_id, Space::Def);

                let symbol = use_stmt.ident_path()?.symbols().next()?;

                let as_ident = self.ctx.compiler.str_ctx.intern(symbol.slice());
                type_namespace.insert(as_ident, *used_package_def_id);

                None
            }
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, macro_) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                        macro_,
                    )
                })?;

                Some(PreDefinedStmt::Def(def_id, def_stmt))
            }
            insp::Statement::SymStatement(sym_stmt) => {
                let root_defs = self.lower_sym_statement(sym_stmt);
                Some(PreDefinedStmt::Sym(root_defs))
            }
            insp::Statement::ArcStatement(edge_stmt) => {
                let (root_defs, edge_id, params) = self.lower_arc_statement(edge_stmt)?;

                Some(PreDefinedStmt::Arc {
                    root_defs,
                    edge_id,
                    params,
                })
            }
            insp::Statement::RelStatement(rel_stmt) => Some(PreDefinedStmt::Rel(rel_stmt)),
            insp::Statement::FmtStatement(fmt_stmt) => Some(PreDefinedStmt::Fmt(fmt_stmt)),
            insp::Statement::MapStatement(map_stmt) => Some(PreDefinedStmt::Map(map_stmt)),
        }
    }

    fn lower_pre_defined(
        &mut self,
        stmt: PreDefinedStmt<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match stmt {
            PreDefinedStmt::Domain(stmt) => self.lower_statement(stmt.into(), block_context),
            PreDefinedStmt::Def(def_id, def_stmt) => self.lower_def_body(def_id, def_stmt),
            PreDefinedStmt::Sym(root_defs) => Some(root_defs),
            PreDefinedStmt::Rel(rel_stmt) => self.lower_statement(rel_stmt.into(), block_context),
            PreDefinedStmt::Arc {
                edge_id,
                mut root_defs,
                params,
            } => {
                for (cardinal_idx, edge_type_param) in params {
                    let Some(ident_path) = edge_type_param.ident_path() else {
                        continue;
                    };
                    let Some(param_def_id) = self.lookup_path(&ident_path, ReportError::Yes) else {
                        continue;
                    };

                    let edge = self
                        .ctx
                        .compiler
                        .edge_ctx
                        .symbolic_edges
                        .get_mut(&edge_id)
                        .unwrap();

                    let cardinal = edge.cardinals.get_mut(&cardinal_idx).unwrap();
                    let CardinalKind::Parameter { def_id } = &mut cardinal.kind else {
                        panic!("not a parameter cardinal");
                    };

                    *def_id = param_def_id;
                    root_defs.push(param_def_id);
                }

                Some(root_defs)
            }
            PreDefinedStmt::Fmt(fmt_stmt) => self.lower_statement(fmt_stmt.into(), block_context),
            PreDefinedStmt::Map(map_stmt) => self.lower_statement(map_stmt.into(), block_context),
        }
    }

    fn lower_sym_statement(&mut self, sym_stmt: insp::SymStatement<V>) -> RootDefs {
        let mut root_defs = vec![];

        for sym_relation in sym_stmt.sym_relations() {
            let Some(sym_decl) = sym_relation.decl() else {
                continue;
            };

            let Some(symbol) = sym_decl.symbol() else {
                continue;
            };

            let opt_def_id = self.catch(|zelf| {
                zelf.ctx
                    .coin_symbol(zelf.ctx.domain_def_id, symbol.slice(), symbol.span())
            });
            root_defs.extend(opt_def_id);
        }

        root_defs
    }

    pub(super) fn read_def_modifiers(
        &mut self,
        modifiers: impl Iterator<Item = V::Token>,
    ) -> (Private, Open, Extern, Macro) {
        let mut private = Private(None);
        let mut open = Open(None);
        let mut extern_ = Extern(None);
        let mut macro_ = Macro(None);

        for modifier in modifiers {
            match modifier.slice() {
                "@private" => {
                    private.0 = Some(modifier.span());
                }
                "@open" => {
                    open.0 = Some(modifier.span());
                }
                "@extern" => {
                    extern_.0 = Some(modifier.span());
                }
                "@macro" => {
                    macro_.0 = Some(modifier.span());
                }
                _ => {
                    CompileError::InvalidModifier.span_report(modifier.span(), &mut self.ctx);
                }
            }
        }

        (private, open, extern_, macro_)
    }
}

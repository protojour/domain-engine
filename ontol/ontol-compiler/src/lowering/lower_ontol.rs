use std::{collections::BTreeMap, marker::PhantomData};

use ontol_core::{tag::DomainIndex, url::DomainUrl};
use ontol_parser::{
    basic_syntax::extract_domain_headerdata,
    cst::{
        inspect::{self as insp},
        view::{NodeView, TokenView, TypedView},
    },
    source::SourceId,
    topology::{OntolHeaderData, WithDocs},
};
use ontol_runtime::{DefId, tuple::CardinalIdx};
use tracing::debug_span;

use crate::{
    CompileError, Compiler,
    edge::{CardinalKind, EdgeId},
    namespace::{DocId, Space},
};

use super::{
    context::{BlockContext, CstLowering, DefModifiers, LoweringCtx, LoweringOutcome, RootDefs},
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
#[derive(Clone, Copy)]
enum Section {
    Domain,
    Use,
    Body,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub fn new(
        url: DomainUrl,
        domain_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                url,
                domain_def_id,
                domain_index: domain_index,
                source_id: source_id,
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
                pre_defined_statements.push((pre_defined, section));
            }
        }

        // second pass: Handle inner bodies, etc
        for (stmt, section) in pre_defined_statements {
            if let Some(mut defs) = self.lower_pre_defined(stmt, BlockContext::NoContext, section) {
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
                let mut errors = vec![];
                let header_data =
                    extract_domain_headerdata(stmt.clone(), WithDocs(true), &mut errors);

                for (error, span) in errors {
                    CompileError::from(error).span_report(span, &mut self.ctx);
                }

                self.ctx.outcome.header_data = Some(header_data);

                None
            }
            insp::Statement::UseStatement(_use_stmt) => None,
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let modifiers = self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        modifiers,
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

            // The inherent relation block on the type uses the parent def as its context
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
                let modifiers = self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        modifiers,
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
        section: Section,
    ) -> Option<RootDefs> {
        if matches!(section, Section::Body) && self.ctx.outcome.header_data.is_none() {
            self.ctx.outcome.header_data = Some(OntolHeaderData::autogenerated());
        }

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
        modifiers: impl Iterator<Item = insp::Modifier<V>>,
    ) -> DefModifiers {
        let mut m = DefModifiers::default();

        for modifier in modifiers {
            let Some(token) = modifier.token() else {
                continue;
            };

            match token.slice() {
                "@private" => {
                    m.private = Some(token.span());
                }
                "@open" => {
                    m.open = Some(token.span());
                }
                "@extern" => {
                    m.r#extern = Some(token.span());
                }
                "@macro" => {
                    m.r#macro = Some(token.span());
                }
                "@crdt" => {
                    m.crdt = Some(token.span());
                }
                _ => {
                    CompileError::InvalidModifier.span_report(token.span(), &mut self.ctx);
                }
            }
        }

        m
    }
}

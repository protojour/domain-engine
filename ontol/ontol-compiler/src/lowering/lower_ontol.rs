use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    marker::PhantomData,
    str::FromStr,
};

use fnv::FnvHashMap;
use ontol_parser::{
    cst::{
        inspect as insp,
        view::{NodeView, TokenView},
    },
    U32Span,
};
use ontol_runtime::{ontology::domain::DomainId, tuple::CardinalIdx, DefId, EdgeId};
use tracing::debug_span;
use ulid::Ulid;

use crate::{
    edge::{Slot, SymbolicEdge, SymbolicEdgeVariable},
    namespace::{DocId, Space},
    package::PackageReference,
    CompileError, Compiler, Src,
};

use super::context::{
    BlockContext, CstLowering, Extern, LoweringCtx, LoweringOutcome, Open, Private, RootDefs,
};

enum PreDefinedStmt<V> {
    Domain(insp::DomainStatement<V>),
    Def(DefId, insp::DefStatement<V>),
    Sym(Vec<DefId>),
    Rel(insp::RelStatement<V>),
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
    pub fn new(pkg_def_id: DefId, src: Src, compiler: &'c mut Compiler<'m>) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                pkg_def_id,
                package_id: src.package_id,
                source_id: src.id,
                anonymous_unions: Default::default(),
                outcome: Default::default(),
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
                self.append_documentation(DocId::Def(self.ctx.pkg_def_id), stmt.0.clone());

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
                    self.ctx.package_id,
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
                let (private, open, extern_) = self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                    )
                })?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.lower_def_body(def_id, def_stmt)?);
                Some(root_defs)
            }
            insp::Statement::SymStatement(_) => None,
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
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
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
                let name = use_stmt.name()?;
                let name_text = name.text().and_then(|result| self.ctx.unescape(result))?;

                let reference = PackageReference::Named(name_text);
                let Some(used_package_def_id) =
                    self.ctx.compiler.packages.loaded_packages.get(&reference)
                else {
                    CompileError::PackageNotFound(reference)
                        .span_report(name.0.span(), &mut self.ctx);
                    return None;
                };

                // insert entry into domain dep graph
                self.ctx
                    .compiler
                    .domain_dep_graph
                    .entry(self.ctx.package_id)
                    .or_default()
                    .insert(used_package_def_id.package_id());

                let type_namespace = self
                    .ctx
                    .compiler
                    .namespaces
                    .get_namespace_mut(self.ctx.package_id, Space::Type);

                let symbol = use_stmt.ident_path()?.symbols().next()?;

                let as_ident = self.ctx.compiler.str_ctx.intern(symbol.slice());
                type_namespace.insert(as_ident, *used_package_def_id);

                None
            }
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_) = self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                    )
                })?;

                Some(PreDefinedStmt::Def(def_id, def_stmt))
            }
            insp::Statement::SymStatement(sym_stmt) => {
                let root_defs = self.lower_sym_statement(sym_stmt);
                Some(PreDefinedStmt::Sym(root_defs))
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
            PreDefinedStmt::Fmt(fmt_stmt) => self.lower_statement(fmt_stmt.into(), block_context),
            PreDefinedStmt::Map(map_stmt) => self.lower_statement(map_stmt.into(), block_context),
        }
    }

    fn lower_sym_statement(&mut self, sym_stmt: insp::SymStatement<V>) -> RootDefs {
        let mut root_defs = vec![];

        let mut opt_edge_builder: Option<Option<EdgeBuilder>> = None;

        for sym_relation in sym_stmt.sym_relations() {
            let mut item_iter = sym_relation.items().peekable();

            match self.next_sym_item(&mut item_iter, sym_relation.view().span()) {
                Some(insp::SymItem::SymDecl(sym_decl)) => {
                    if matches!(opt_edge_builder, Some(Some(_))) {
                        CompileError::SymCannotMixStandaloneSymbolsAndSymbolicEdge
                            .span_report(sym_decl.view().span(), &mut self.ctx);
                    }

                    // not in "edge mode"
                    opt_edge_builder = Some(None);

                    let Some(symbol) = sym_decl.symbol() else {
                        continue;
                    };

                    let opt_def_id =
                        self.catch(|zelf| zelf.ctx.coin_symbol(symbol.slice(), symbol.span()));
                    root_defs.extend(opt_def_id);

                    if let Some(next) = item_iter.next() {
                        CompileError::SymStandaloneTrailingItems
                            .span_report(next.view().span(), &mut self.ctx);
                    }
                }
                Some(insp::SymItem::SymVar(sym_var)) => {
                    let edge_builder = match &mut opt_edge_builder {
                        None => {
                            let edge_id = self
                                .ctx
                                .compiler
                                .edge_ctx
                                .alloc_edge_id(self.ctx.package_id);

                            opt_edge_builder = Some(Some(EdgeBuilder {
                                edge_id,
                                slots: Default::default(),
                                variables: Default::default(),
                                var_name_table: Default::default(),
                            }));

                            opt_edge_builder
                                .as_mut()
                                .and_then(|builder| builder.as_mut())
                                .unwrap()
                        }
                        Some(Some(builder)) => builder,
                        Some(None) => {
                            CompileError::SymCannotMixStandaloneSymbolsAndSymbolicEdge
                                .span_report(sym_var.view().span(), &mut self.ctx);
                            continue;
                        }
                    };

                    let Some(mut prev_cardinal) = self.add_sym_variable(sym_var, edge_builder)
                    else {
                        continue;
                    };

                    loop {
                        if let Some((sym_id, next_cardinal)) = self.add_sym_decl_then_variable(
                            &mut item_iter,
                            prev_cardinal,
                            edge_builder,
                            sym_stmt.view().span(),
                        ) {
                            self.ctx
                                .compiler
                                .edge_ctx
                                .symbols
                                .insert(sym_id, edge_builder.edge_id);

                            root_defs.push(sym_id);
                            prev_cardinal = next_cardinal;
                        } else {
                            break;
                        }

                        if item_iter.peek().is_none() {
                            break;
                        }
                    }
                }
                None => {}
            }
        }

        if let Some(Some(edge_builder)) = opt_edge_builder {
            self.ctx.compiler.edge_ctx.symbolic_edges.insert(
                edge_builder.edge_id,
                SymbolicEdge {
                    symbols: edge_builder.slots,
                    variables: edge_builder.variables,
                },
            );
        }

        root_defs
    }

    fn next_sym_item(
        &mut self,
        item_iter: &mut impl Iterator<Item = insp::SymItem<V>>,
        sym_relation_span: U32Span,
    ) -> Option<insp::SymItem<V>> {
        let item = item_iter.next();

        if item.is_none() {
            CompileError::SymEdgeExpectedTrailingItem.span_report(sym_relation_span, &mut self.ctx);
        }

        item
    }

    fn add_sym_decl_then_variable(
        &mut self,
        item_iter: &mut impl Iterator<Item = insp::SymItem<V>>,
        prev_cardinal_idx: CardinalIdx,
        edge_builder: &mut EdgeBuilder,
        sym_relation_span: U32Span,
    ) -> Option<(DefId, CardinalIdx)> {
        let next_item = self.next_sym_item(item_iter, sym_relation_span)?;

        let insp::SymItem::SymDecl(sym_decl) = next_item else {
            CompileError::SymEdgeExpectedSymbol.span_report(next_item.view().span(), &mut self.ctx);
            return None;
        };

        let symbol = sym_decl.symbol()?;
        let opt_def_id = self.catch(|zelf| zelf.ctx.coin_symbol(symbol.slice(), symbol.span()));
        let next_item = self.next_sym_item(item_iter, sym_relation_span)?;

        let insp::SymItem::SymVar(sym_var) = next_item else {
            CompileError::SymEdgeExpectedVariable
                .span_report(next_item.view().span(), &mut self.ctx);
            return None;
        };

        let cardinal_idx = self.add_sym_variable(sym_var, edge_builder);

        match (opt_def_id, cardinal_idx) {
            (Some(def_id), Some(cardinal_idx)) => {
                edge_builder.slots.insert(
                    def_id,
                    Slot {
                        left: prev_cardinal_idx,
                        depth: 0,
                        right: cardinal_idx,
                    },
                );

                Some((def_id, cardinal_idx))
            }
            _ => None,
        }
    }

    fn add_sym_variable(
        &mut self,
        sym_var: insp::SymVar<V>,
        edge_builder: &mut EdgeBuilder,
    ) -> Option<CardinalIdx> {
        let var_symbol = sym_var.symbol()?;
        let len = edge_builder.variables.len();

        match edge_builder
            .var_name_table
            .entry(var_symbol.slice().to_string())
        {
            Entry::Occupied(occupied) => Some(*occupied.get()),
            Entry::Vacant(vacant) => {
                let Ok(cardinal_idx): Result<u8, _> = len.try_into() else {
                    CompileError::SymEdgeArityOverflow
                        .span_report(sym_var.view().span(), &mut self.ctx);

                    return None;
                };
                let cardinal_idx = CardinalIdx(cardinal_idx);

                edge_builder.variables.insert(
                    cardinal_idx,
                    SymbolicEdgeVariable {
                        span: self.ctx.source_span(var_symbol.span()),
                        members: Default::default(),
                        one_to_one_count: 0,
                    },
                );

                vacant.insert(cardinal_idx);
                Some(cardinal_idx)
            }
        }
    }

    pub(super) fn read_def_modifiers(
        &mut self,
        modifiers: impl Iterator<Item = V::Token>,
    ) -> (Private, Open, Extern) {
        let mut private = Private(None);
        let mut open = Open(None);
        let mut extern_ = Extern(None);

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
                _ => {
                    CompileError::InvalidModifier.span_report(modifier.span(), &mut self.ctx);
                }
            }
        }

        (private, open, extern_)
    }
}

struct EdgeBuilder {
    edge_id: EdgeId,
    var_name_table: HashMap<String, CardinalIdx>,
    slots: FnvHashMap<DefId, Slot>,
    variables: BTreeMap<CardinalIdx, SymbolicEdgeVariable>,
}

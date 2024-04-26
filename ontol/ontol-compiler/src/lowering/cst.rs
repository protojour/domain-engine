#![allow(unused)]

use std::{marker::PhantomData, ops::Range};

use ontol_parser::{
    cst::{
        inspect::{
            DefStatement, FmtStatement, IdentPath, Location, MapStatement, Node, RelStatement,
            Statement, TypeMod, TypeRef,
        },
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    Span, UnescapeError,
};
use ontol_runtime::DefId;
use tracing::debug_span;

use crate::{
    def::{TypeDef, TypeDefFlags},
    lowering::context::LoweringCtx,
    namespace::Space,
    package::PackageReference,
    CompileError, Compiler, SpannedCompileError, Src,
};

use super::context::{BlockContext, Extern, Open, Private, RootDefs, Symbol};

pub struct CstLowering<'c, 'm, 's, V: NodeView<'s>> {
    ctx: LoweringCtx<'c, 'm>,
    _phantom: PhantomData<&'s V>,
}

type LoweringError = (CompileError, Span);
type Res<T> = Result<T, LoweringError>;

enum PreDefinedStmt<V> {
    Def(DefId, DefStatement<V>),
    Rel(RelStatement<V>),
    Fmt(FmtStatement<V>),
    Map(MapStatement<V>),
}

impl<'c, 'm, 's, V: NodeView<'s>> CstLowering<'c, 'm, 's, V> {
    pub fn new(compiler: &'c mut Compiler<'m>, src: Src) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                package_id: src.package_id,
                source_id: src.id,
                root_defs: Default::default(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn lower_ontol(mut self, ontol: Node<V>) -> Self {
        let Node::Ontol(ontol) = ontol else {
            return self;
        };

        let mut pre_defined_statements = vec![];

        // first pass: Register all named definitions into the namespace
        for statement in ontol.statements() {
            if let Some(pre_defined) = self.pre_define_statement(statement) {
                pre_defined_statements.push(pre_defined);
            }
        }

        // second pass: Handle inner bodies, etc
        for stmt in pre_defined_statements {
            if let Some(mut defs) = self.lower_pre_defined(stmt, BlockContext::NoContext) {
                self.ctx.root_defs.append(&mut defs);
            }
        }

        self
    }

    pub fn finish(self) -> Vec<DefId> {
        self.ctx.root_defs
    }

    fn pre_define_statement(&mut self, statement: Statement<V>) -> Option<PreDefinedStmt<V>> {
        match statement {
            Statement::UseStatement(use_stmt) => {
                let location = use_stmt.location()?;
                let location_text = location.text().and_then(|result| self.unescape(result))?;

                let reference = PackageReference::Named(location_text);
                let Some(used_package_def_id) =
                    self.ctx.compiler.packages.loaded_packages.get(&reference)
                else {
                    self.report_error((
                        CompileError::PackageNotFound(reference),
                        location.view.span(),
                    ));
                    return None;
                };

                let type_namespace = self
                    .ctx
                    .compiler
                    .namespaces
                    .get_namespace_mut(self.ctx.package_id, Space::Type);

                let symbol = use_stmt.ident_path()?.symbols().next()?;

                let as_ident = self.ctx.compiler.strings.intern(symbol.slice());
                type_namespace.insert(as_ident, *used_package_def_id);

                None
            }
            Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        &ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;

                Some(PreDefinedStmt::Def(def_id, def_stmt))
            }
            Statement::RelStatement(rel_stmt) => Some(PreDefinedStmt::Rel(rel_stmt)),
            Statement::FmtStatement(fmt_stmt) => Some(PreDefinedStmt::Fmt(fmt_stmt)),
            Statement::MapStatement(map_stmt) => Some(PreDefinedStmt::Map(map_stmt)),
        }
    }

    fn lower_pre_defined(
        &mut self,
        stmt: PreDefinedStmt<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match stmt {
            PreDefinedStmt::Def(def_id, def_stmt) => self.lower_def_body(def_id, def_stmt),
            PreDefinedStmt::Rel(rel_stmt) => {
                self.lower_statement(Statement::RelStatement(rel_stmt), block_context)
            }
            PreDefinedStmt::Fmt(fmt_stmt) => {
                self.lower_statement(Statement::FmtStatement(fmt_stmt), block_context)
            }
            PreDefinedStmt::Map(map_stmt) => {
                self.lower_statement(Statement::MapStatement(map_stmt), block_context)
            }
        }
    }

    fn lower_statement(
        &mut self,
        statement: Statement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match statement {
            Statement::UseStatement(use_stmt) => None,
            Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        &ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.lower_def_body(def_id, def_stmt)?);
                Some(root_defs)
            }
            Statement::RelStatement(rel_stmt) => self.lower_rel_statement(rel_stmt, block_context),
            Statement::FmtStatement(fmt_stmt) => todo!(),
            Statement::MapStatement(map_stmt) => todo!(),
        }
    }

    fn lower_def_body(&mut self, def_id: DefId, stmt: DefStatement<V>) -> Option<RootDefs> {
        // self.ctx
        //     .compiler
        //     .namespaces
        //     .docs
        //     .entry(def_id)
        //     .or_default()
        //     .extend(docs);

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

    fn lower_rel_statement(
        &mut self,
        stmt: RelStatement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        let subject = stmt.subject()?;
        let fwd_set = stmt.fwd_set()?;
        let backwd_set = stmt.backwd_set();
        let object = stmt.object()?;

        let mut root_defs = RootDefs::default();

        let subject_def_id = self.resolve_type_reference(
            subject.type_mod()?.type_ref()?,
            &block_context,
            Some(&mut root_defs),
        )?;

        None
    }

    fn resolve_type_reference(
        &mut self,
        type_ref: TypeRef<V>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
    ) -> Option<DefId> {
        match (type_ref, block_context) {
            (TypeRef::IdentPath(path), _) => self.lookup_path(path),
            (TypeRef::This(_), BlockContext::Context(func)) => Some(func()),
            (TypeRef::This(this), BlockContext::NoContext) => {
                self.report_error((CompileError::WildcardNeedsContextualBlock, this.view.span()));
                None
            }
            (TypeRef::DefBody(body), _) => {
                if body.statements().next().is_none() {
                    return Some(self.ctx.compiler.primitives.unit);
                }

                let Some(root_defs) = root_defs else {
                    self.report_error((
                        CompileError::TODO("Anonymous struct not allowed here"),
                        body.view.span(),
                    ));
                    return None;
                };

                let def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::CONCRETE,
                    },
                    &body.view.span(),
                );

                // This type needs to be part of the anonymous part of the namespace
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.package_id, def_id);
                root_defs.push(def_id);

                let context_fn = || def_id;

                for statement in body.statements() {
                    if let Some(mut defs) =
                        self.lower_statement(statement, BlockContext::Context(&context_fn))
                    {
                        root_defs.append(&mut defs);
                    }
                }

                Some(def_id)
            }
        }
    }

    fn lookup_path(&mut self, ident_path: IdentPath<V>) -> Option<DefId> {
        self.catch(|zelf| {
            zelf.ctx.lookup_path(
                ident_path
                    .symbols()
                    .map(|token| (token.slice(), token.span())),
                &ident_path.view.span(),
            )
        })
    }

    fn unescape(&mut self, result: Result<String, Vec<UnescapeError>>) -> Option<String> {
        match result {
            Ok(string) => Some(string),
            Err(unescape_errors) => {
                for error in unescape_errors {
                    self.report_error((CompileError::TODO(error.msg), error.span));
                }

                None
            }
        }
    }

    fn read_def_modifiers(
        &mut self,
        modifiers: impl Iterator<Item = V::Token>,
    ) -> (Private, Open, Extern, Symbol) {
        let mut private = Private(None);
        let mut open = Open(None);
        let mut extern_ = Extern(None);
        let mut symbol = Symbol(None);

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
                "@symbol" => {
                    symbol.0 = Some(modifier.span());
                }
                _ => {
                    self.report_error((CompileError::TODO("illegal modifier"), modifier.span()));
                }
            }
        }

        (private, open, extern_, symbol)
    }

    fn catch<T>(&mut self, f: impl FnOnce(&mut Self) -> Res<T>) -> Option<T> {
        match f(self) {
            Ok(value) => Some(value),
            Err((err, span)) => {
                self.report_error((err, span));
                None
            }
        }
    }

    fn report_error(&mut self, (error, span): (CompileError, Range<usize>)) {
        self.ctx
            .compiler
            .push_error(error.spanned(&self.ctx.source_span(&span)));
    }
}

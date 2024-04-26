#![allow(unused)]

use std::{marker::PhantomData, ops::Range};

use ontol_parser::{
    cst::{
        inspect::{FmtStatement, Location, MapStatement, Node, RelStatement, Statement},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    Span, UnescapeError,
};
use ontol_runtime::DefId;

use crate::{
    lowering::context::LoweringCtx, namespace::Space, package::PackageReference, CompileError,
    Compiler, SpannedCompileError, Src,
};

pub struct CstLowering<'c, 'm, 's, V: NodeView<'s>> {
    ctx: LoweringCtx<'c, 'm>,
    _phantom: PhantomData<&'s V>,
}

type LoweringError = (CompileError, Span);
type Res<T> = Result<T, LoweringError>;

enum PreDefinedStmt<V> {
    Def(DefId, Statement<V>),
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

        // first pass: Register all named definitions into the namespace
        for statement in ontol.statements() {
            self.pre_define_statement(statement);
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
                // TODO
                None
            }
            Statement::RelStatement(rel_stmt) => Some(PreDefinedStmt::Rel(rel_stmt)),
            Statement::FmtStatement(fmt_stmt) => Some(PreDefinedStmt::Fmt(fmt_stmt)),
            Statement::MapStatement(map_stmt) => Some(PreDefinedStmt::Map(map_stmt)),
        }
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

    fn report_error(&mut self, (error, span): (CompileError, Range<usize>)) {
        self.ctx
            .compiler
            .push_error(error.spanned(&self.ctx.source_span(&span)));
    }
}

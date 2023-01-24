use std::collections::HashMap;

use smartstring::alias::String;

use crate::{
    compile::error::{CompileError, SpannedCompileError},
    def::{Def, DefId, DefKind},
    env::Env,
    expr::{Expr, ExprId, ExprKind},
    parse::{ast, Span},
    source::{CompileSrc, CORE_PKG},
};

pub struct Lowering<'s, 'm> {
    env: &'s mut Env<'m>,
    src: &'s CompileSrc,
    root_defs: Vec<DefId>,
}

type Res<T> = Result<T, SpannedCompileError>;

impl<'s, 'm> Lowering<'s, 'm> {
    pub fn new(env: &'s mut Env<'m>, src: &'s CompileSrc) -> Self {
        Self {
            env,
            src,
            root_defs: Default::default(),
        }
    }

    pub fn finish(self) -> Vec<DefId> {
        self.root_defs
    }

    pub fn lower_ast(&mut self, ast: (ast::Ast, Span)) -> Res<()> {
        let root_def = self.ast_to_def(ast)?;
        self.root_defs.push(root_def);
        Ok(())
    }

    fn ast_to_def(&mut self, (ast, span): (ast::Ast, Span)) -> Res<DefId> {
        match ast {
            ast::Ast::Type((ident, _)) => {
                let def_id = self.named_def_id(&ident);
                self.set_def(def_id, DefKind::Type(ident), &span);
                Ok(def_id)
            }
            ast::Ast::Rel(ast::Rel {
                subject,
                ident,
                object,
            }) => {
                // This syntax just defines the relation first time it's used
                let rel_def_id = self.define_rel_if_undefined(ident.0);
                panic!();
            }
            ast::Ast::Data(ast::Data {
                ident,
                ty: (ast_ty, ty_span),
            }) => {
                let def_id = self.named_def_id(&ident.0);
                let type_def_id = self.ast_type_to_def((ast_ty, ty_span.clone()))?;
                let field_def_id = self.def(DefKind::AnonField { type_def_id }, &ty_span);

                self.set_def(def_id, DefKind::Constructor(ident.0, field_def_id), &span);

                Ok(def_id)
            }
            ast::Ast::Eq(ast::Eq {
                params: _,
                first,
                second,
            }) => {
                let mut var_table = VarTable::default();
                let first = self.lower_root_expr(first, &mut var_table)?;
                let second = self.lower_root_expr(second, &mut var_table)?;
                Ok(self.def(DefKind::Equivalence(first, second), &span))
            }
            _ => panic!(),
        }
    }

    fn ast_type_to_def(&mut self, (ast_ty, span): (ast::Type, Span)) -> Res<DefId> {
        match ast_ty {
            ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, &span)),
            ast::Type::Sym(ident) => {
                let Some(type_def_id) = self.env.namespaces.lookup(&[self.src.package, CORE_PKG], &ident) else {
                return Err(self.error(CompileError::TypeNotFound, &span));
            };
                Ok(type_def_id)
            }
            ast::Type::Record(ast_record) => {
                let mut record_defs = vec![];
                for (ast_field, span) in ast_record.fields {
                    let type_def_id = self.ast_type_to_def(ast_field.ty)?;
                    record_defs.push(self.def(
                        DefKind::NamedField {
                            ident: ast_field.ident.0,
                            type_def_id,
                        },
                        &span,
                    ));
                }

                Ok(self.def(
                    DefKind::Record {
                        field_defs: record_defs,
                    },
                    &span,
                ))
            }
        }
    }

    fn lower_root_expr(&mut self, ast: (ast::Expr, Span), var_table: &mut VarTable) -> Res<ExprId> {
        let expr = self.lower_expr(ast, var_table)?;
        let expr_id = self.env.defs.alloc_expr_id();
        self.env.expressions.insert(expr_id, expr);
        Ok(expr_id)
    }

    fn lower_expr(
        &mut self,
        (ast_expr, span): (ast::Expr, Span),
        var_table: &mut VarTable,
    ) -> Res<Expr> {
        match ast_expr {
            ast::Expr::Literal(ast::Literal::Number(num)) => {
                let num = num
                    .parse()
                    .map_err(|_| self.error(CompileError::InvalidNumber, &span))?;
                Ok(self.expr(ExprKind::Constant(num), &span))
            }
            ast::Expr::Call((ident, sym_span), ast_args) => {
                let args = ast_args
                    .into_iter()
                    .map(|ast_arg| self.lower_expr(ast_arg, var_table))
                    .collect::<Result<_, _>>()?;

                let Some(def_id) = self.env.namespaces.lookup(&[self.src.package, CORE_PKG], &ident) else {
                    return Err(self.error(CompileError::TypeNotFound, &sym_span));
                };

                Ok(self.expr(ExprKind::Call(def_id, args), &span))
            }
            ast::Expr::Sym(var_ident) => {
                let id = var_table.var_id(var_ident, self.env);
                Ok(self.expr(ExprKind::Variable(id), &span))
            }
            expr => panic!(
                "BUG: Unable to lower expression {expr:?} at {:?}",
                self.src.span(&span)
            ),
        }
    }

    fn define_rel_if_undefined(&mut self, ident: ast::RelIdent) -> DefId {
        match ident {
            ast::RelIdent::Named(ident) => self
                .env
                .namespaces
                .get_mut(self.src.package)
                .entry(ident)
                .or_insert_with(|| self.env.defs.alloc_def_id())
                .clone(),
            ast::RelIdent::Unnamed => self.env.defs.alloc_def_id(),
        }
    }

    fn named_def_id(&mut self, ident: &String) -> DefId {
        let def_id = self.env.defs.alloc_def_id();
        self.env
            .namespaces
            .get_mut(self.src.package)
            .insert(ident.clone(), def_id);
        def_id
    }

    fn set_def(&mut self, def_id: DefId, kind: DefKind, span: &Span) {
        self.env.defs.map.insert(
            def_id,
            Def {
                package: self.src.package,
                kind,
                span: self.src.span(&span),
            },
        );
    }

    fn def(&mut self, kind: DefKind, span: &Span) -> DefId {
        let def_id = self.env.defs.alloc_def_id();
        self.set_def(def_id, kind, span);
        def_id
    }

    fn expr(&self, kind: ExprKind, span: &Span) -> Expr {
        Expr {
            id: ExprId(0),
            kind,
            span: self.src.span(&span),
        }
    }

    fn error(&self, compile_error: CompileError, span: &Span) -> SpannedCompileError {
        compile_error.spanned(&self.env.sources, &self.src.span(span))
    }
}

#[derive(Default)]
struct VarTable {
    variables: HashMap<String, ExprId>,
}

impl VarTable {
    fn var_id(&mut self, ident: String, env: &mut Env) -> ExprId {
        self.variables
            .entry(ident)
            .or_insert_with(|| env.defs.alloc_expr_id())
            .clone()
    }
}

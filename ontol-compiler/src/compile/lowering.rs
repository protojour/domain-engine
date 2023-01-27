use std::collections::{hash_map::Entry, HashMap};

use smartstring::alias::String;

use crate::{
    compile::error::{CompileError, SpannedCompileError},
    compiler::Compiler,
    def::{Def, DefId, DefKind, Relation, Relationship},
    expr::{Expr, ExprId, ExprKind},
    namespace::Space,
    parse::{ast, Span},
    source::{CompileSrc, CORE_PKG},
};

pub struct Lowering<'s, 'm> {
    compiler: &'s mut Compiler<'m>,
    src: &'s CompileSrc,
    root_defs: Vec<DefId>,
}

type Res<T> = Result<T, SpannedCompileError>;

impl<'s, 'm> Lowering<'s, 'm> {
    pub fn new(compiler: &'s mut Compiler<'m>, src: &'s CompileSrc) -> Self {
        Self {
            compiler,
            src,
            root_defs: Default::default(),
        }
    }

    pub fn finish(self) -> Vec<DefId> {
        self.root_defs
    }

    pub fn lower_ast(&mut self, ast: (ast::Ast, Span)) -> Res<()> {
        if let Some(root_def) = self.ast_to_def(ast)? {
            self.root_defs.push(root_def);
        }
        Ok(())
    }

    fn ast_to_def(&mut self, (ast, span): (ast::Ast, Span)) -> Res<Option<DefId>> {
        match ast {
            ast::Ast::Import(_) => panic!("import not supported yet"),
            ast::Ast::Type((ident, _)) => {
                let def_id = self.named_def_id(Space::Type, &ident);
                self.set_def(def_id, DefKind::Type(ident), &span);
                Ok(Some(def_id))
            }
            ast::Ast::Rel(ast::Rel {
                subject,
                ident: (ident, ident_span),
                object,
            }) => {
                // This syntax just defines the relation the first time it's used
                let relation_def_id = match self.define_relation_if_undefined(ident.clone()) {
                    ImplicitDefId::New(def_id) => {
                        self.set_def(
                            def_id,
                            DefKind::Relation(Relation {
                                subject_prop: None,
                                ident,
                                object_prop: None,
                            }),
                            &ident_span,
                        );
                        def_id
                    }
                    ImplicitDefId::Reused(def_id) => def_id,
                };

                let subject = self.ast_type_to_def(subject)?;
                let object = self.ast_type_to_def(object)?;

                Ok(Some(self.def(
                    DefKind::Relationship(Relationship {
                        relation_def_id,
                        subject,
                        object,
                    }),
                    &span,
                )))
            }
            ast::Ast::Data(ast::Data {
                ident,
                ty: (ast_ty, ty_span),
            }) => {
                let def_id = self.named_def_id(Space::Type, &ident.0);
                let type_def_id = self.ast_type_to_def((ast_ty, ty_span.clone()))?;
                let field_def_id = self.def(DefKind::AnonField { type_def_id }, &ty_span);

                self.set_def(def_id, DefKind::Constructor(ident.0, field_def_id), &span);

                Ok(Some(def_id))
            }
            ast::Ast::Eq(ast::Eq {
                params: _,
                first,
                second,
            }) => {
                let mut var_table = VarTable::default();
                let first = self.lower_root_expr(first, &mut var_table)?;
                let second = self.lower_root_expr(second, &mut var_table)?;
                Ok(Some(self.def(DefKind::Equivalence(first, second), &span)))
            }
            ast::Ast::Comment(_) => Ok(None), // _ => panic!(),
        }
    }

    fn ast_type_to_def(&mut self, (ast_ty, span): (ast::Type, Span)) -> Res<DefId> {
        match ast_ty {
            ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, &span)),
            ast::Type::Sym(ident) => {
                match self.compiler.namespaces.lookup(
                    &[self.src.package, CORE_PKG],
                    Space::Type,
                    &ident,
                ) {
                    Some(type_def_id) => Ok(type_def_id),
                    None => Err(self.error(CompileError::TypeNotFound, &span)),
                }
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
        let expr_id = self.compiler.defs.alloc_expr_id();
        self.compiler.expressions.insert(expr_id, expr);
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

                match self.compiler.namespaces.lookup(
                    &[self.src.package, CORE_PKG],
                    Space::Type,
                    &ident,
                ) {
                    Some(def_id) => Ok(self.expr(ExprKind::Call(def_id, args), &span)),
                    None => Err(self.error(CompileError::TypeNotFound, &sym_span)),
                }
            }
            ast::Expr::Sym(var_ident) => {
                let id = var_table.var_id(var_ident, self.compiler);
                Ok(self.expr(ExprKind::Variable(id), &span))
            }
            expr => panic!(
                "BUG: Unable to lower expression {expr:?} at {:?}",
                self.src.span(&span)
            ),
        }
    }

    fn define_relation_if_undefined(&mut self, ident: Option<String>) -> ImplicitDefId {
        match ident {
            Some(ident) => match self
                .compiler
                .namespaces
                .get_mut(self.src.package, Space::Rel)
                .entry(ident)
            {
                Entry::Vacant(vacant) => {
                    ImplicitDefId::New(vacant.insert(self.compiler.defs.alloc_def_id()).clone())
                }
                Entry::Occupied(occupied) => ImplicitDefId::Reused(occupied.get().clone()),
            },
            None => ImplicitDefId::New(self.compiler.defs.alloc_def_id()),
        }
    }

    fn named_def_id(&mut self, space: Space, ident: &String) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id();
        self.compiler
            .namespaces
            .get_mut(self.src.package, space)
            .insert(ident.clone(), def_id);
        def_id
    }

    fn set_def(&mut self, def_id: DefId, kind: DefKind, span: &Span) {
        self.compiler.defs.map.insert(
            def_id,
            Def {
                package: self.src.package,
                kind,
                span: self.src.span(&span),
            },
        );
    }

    fn def(&mut self, kind: DefKind, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id();
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
        compile_error.spanned(&self.compiler.sources, &self.src.span(span))
    }
}

enum ImplicitDefId {
    New(DefId),
    Reused(DefId),
}

#[derive(Default)]
struct VarTable {
    variables: HashMap<String, ExprId>,
}

impl VarTable {
    fn var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        self.variables
            .entry(ident)
            .or_insert_with(|| compiler.defs.alloc_expr_id())
            .clone()
    }
}

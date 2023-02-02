use std::collections::{hash_map::Entry, HashMap};

use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    def::{Def, DefKind, Relation, Relationship, Variables},
    error::{CompileError, SpannedCompileError},
    expr::{Expr, ExprId, ExprKind, TypePath},
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

    pub fn lower_ast(&mut self, ast: (ast::Ast, Span)) -> Result<(), ()> {
        match self.ast_to_def(ast) {
            Ok(Some(root_def)) => {
                self.root_defs.push(root_def);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => {
                self.compiler.push_error(error);
                Err(())
            }
        }
    }

    fn ast_to_def(&mut self, (ast, span): (ast::Ast, Span)) -> Res<Option<DefId>> {
        match ast {
            ast::Ast::Import(_) => panic!("import not supported yet"),
            ast::Ast::Type((ident, _)) => {
                let def_id = self.named_def_id(Space::Type, &ident);
                self.set_def(def_id, DefKind::DomainType(ident), &span);
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
            ast::Ast::Eq(ast::Eq {
                params,
                first,
                second,
            }) => {
                let mut var_table = VarTable::default();
                let mut variables = vec![];
                for (param, _span) in params.into_iter() {
                    variables.push(var_table.new_var_id(param, self.compiler));
                }
                let first = self.lower_root_expr(first, &mut var_table)?;
                let second = self.lower_root_expr(second, &mut var_table)?;
                Ok(Some(self.def(
                    DefKind::Equivalence(Variables(variables.into()), first, second),
                    &span,
                )))
            }
            ast::Ast::Comment(_) => Ok(None), // _ => panic!(),
        }
    }

    fn ast_type_to_def(&mut self, (ast_ty, span): (ast::Type, Span)) -> Res<DefId> {
        match ast_ty {
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
            ast::Type::Literal(ast::Literal::String(lit)) => {
                Ok(self.compiler.defs.def_string_literal(lit))
            }
            ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, &span)),
            ast::Type::Tuple(elements) => {
                let element_defs = elements
                    .into_iter()
                    .map(|element| self.ast_type_to_def(element))
                    .collect::<Result<_, _>>()?;

                Ok(self.compiler.defs.def_tuple(element_defs))
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
        var_table: &VarTable,
    ) -> Res<Expr> {
        match ast_expr {
            ast::Expr::Literal(ast::Literal::Int(int)) => {
                let int = int
                    .parse()
                    .map_err(|_| self.error(CompileError::InvalidInteger, &span))?;
                Ok(self.expr(ExprKind::Constant(int), &span))
            }
            ast::Expr::Call((ident, ident_span), ast_args) => {
                let args = ast_args
                    .into_iter()
                    .map(|ast_arg| self.lower_expr(ast_arg, var_table))
                    .collect::<Result<_, _>>()?;

                let def_id = self.lookup_ident(&ident, &ident_span)?;
                Ok(self.expr(ExprKind::Call(def_id, args), &span))
            }
            ast::Expr::Obj((ident, ident_span), ast_attributes) => {
                let attributes = ast_attributes
                    .into_iter()
                    .map(|(ast_attr, _)| {
                        self.lower_expr(ast_attr.value, var_table).map(|expr| {
                            (
                                (
                                    match ast_attr.property.0 {
                                        ast::Property::Named(name) => Some(name),
                                        ast::Property::Wildcard => None,
                                    },
                                    self.src.span(&ast_attr.property.1),
                                ),
                                expr,
                            )
                        })
                    })
                    .collect::<Result<_, _>>()?;

                let type_def_id = self.lookup_ident(&ident, &ident_span)?;
                Ok(self.expr(
                    ExprKind::Obj(
                        TypePath {
                            def_id: type_def_id,
                            span: self.src.span(&ident_span),
                        },
                        attributes,
                    ),
                    &span,
                ))
            }
            ast::Expr::Variable(var_ident) => {
                let id = var_table
                    .get_var_id(var_ident.as_str())
                    .ok_or_else(|| self.error(CompileError::UndeclaredVariable, &span))?;
                Ok(self.expr(ExprKind::Variable(id), &span))
            }
            expr => panic!(
                "BUG: Unable to lower expression {expr:?} at {:?}",
                self.src.span(&span)
            ),
        }
    }

    fn lookup_ident(&mut self, ident: &str, span: &Span) -> Result<DefId, SpannedCompileError> {
        match self
            .compiler
            .namespaces
            .lookup(&[self.src.package, CORE_PKG], Space::Type, ident)
        {
            Some(def_id) => Ok(def_id),
            None => Err(self.error(CompileError::TypeNotFound, &span)),
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
            None => ImplicitDefId::Reused(self.compiler.defs.anonymous_relation()),
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
    fn new_var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        self.variables
            .entry(ident)
            .or_insert_with(|| compiler.defs.alloc_expr_id())
            .clone()
    }

    fn get_var_id(&self, ident: &str) -> Option<ExprId> {
        self.variables.get(ident).cloned()
    }
}

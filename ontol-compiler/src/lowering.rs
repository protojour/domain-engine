use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
};

use ontol_runtime::{DefId, RelationId};
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    def::{
        Def, DefKind, PropertyCardinality, RelParams, Relation, RelationIdent, Relationship,
        ValueCardinality, Variables,
    },
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
                let ident = self.compiler.strings.intern(&ident);
                self.set_def(def_id, DefKind::DomainType(ident), &span);
                Ok(Some(def_id))
            }
            ast::Ast::Entity((ident, _)) => {
                let def_id = self.named_def_id(Space::Type, &ident);
                let ident = self.compiler.strings.intern(&ident);
                self.set_def(def_id, DefKind::DomainEntity(ident), &span);
                Ok(Some(def_id))
            }
            ast::Ast::Relationship(rel) => {
                let ast::Relationship {
                    subject,
                    relation:
                        ast::Relation {
                            ty: relation_ty,
                            subject_cardinality,
                            object_prop_ident,
                            object_cardinality,
                            rel_params,
                        },
                    object,
                } = *rel;

                let subject_def = self.ast_type_to_def(subject.0, &subject.1)?;
                let object_def = self.ast_type_to_def(object.0, &object.1)?;

                let (relation_ident, ident_span, index_range_rel_params): (
                    _,
                    _,
                    Option<Range<Option<u16>>>,
                ) = match relation_ty {
                    (ast::RelationType::Type((ast_ty, span)), _) => {
                        let def_id = self.ast_type_to_def(ast_ty, &span)?;

                        match self.compiler.defs.get_def_kind(def_id) {
                            Some(DefKind::StringLiteral(_)) => {
                                (RelationIdent::Named(def_id), span, None)
                            }
                            _ => (RelationIdent::Typed(def_id), span, None),
                        }
                    }
                    (ast::RelationType::IntRange(range), span) => {
                        (RelationIdent::Indexed, span, Some(range))
                    }
                };

                let has_object_prop = object_prop_ident.is_some();

                // This syntax just defines the relation the first time it's used
                let relation_id = match self.define_relation_if_undefined(relation_ident) {
                    ImplicitRelationId::New(relation_id) => {
                        let object_prop =
                            object_prop_ident.map(|ident| self.compiler.strings.intern(&ident.0));

                        self.set_def(
                            relation_id.0,
                            DefKind::Relation(Relation {
                                ident: relation_ident,
                                subject_prop: None,
                                object_prop,
                            }),
                            &ident_span,
                        );
                        relation_id
                    }
                    ImplicitRelationId::Reused(relation_id) => relation_id,
                };

                let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
                    if let Some((_, span)) = rel_params {
                        return Err(self.error(
                            CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes,
                            &span,
                        ));
                    }

                    RelParams::IndexRange(index_range_rel_params)
                } else {
                    match rel_params {
                        Some(rel_params) => {
                            RelParams::Type(self.ast_type_to_def(rel_params.0, &rel_params.1)?)
                        }
                        None => RelParams::Unit,
                    }
                };

                Ok(Some(
                    self.def(
                        DefKind::Relationship(Relationship {
                            relation_id,
                            subject: (subject_def, self.src.span(&subject.1)),
                            subject_cardinality: subject_cardinality
                                .map(convert_cardinality)
                                .unwrap_or((PropertyCardinality::Mandatory, ValueCardinality::One)),
                            object: (object_def, self.src.span(&object.1)),
                            object_cardinality: object_cardinality
                                .map(convert_cardinality)
                                .unwrap_or_else(|| {
                                    if has_object_prop {
                                        // i.e. no syntax sugar: The object prop is explicit,
                                        // therefore the object cardinality is explicit.
                                        (PropertyCardinality::Mandatory, ValueCardinality::One)
                                    } else {
                                        // The syntactic sugar case, which is the default behaviour:
                                        // Many incoming edges to the same object:
                                        (PropertyCardinality::Optional, ValueCardinality::Many)
                                    }
                                }),
                            rel_params,
                        }),
                        &span,
                    ),
                ))
            }
            ast::Ast::Eq(ast::Eq {
                variables: ast_variables,
                first,
                second,
            }) => {
                let mut var_table = VarTable::default();
                let mut variables = vec![];
                for (param, span) in ast_variables.into_iter() {
                    variables.push((
                        var_table.new_var_id(param, self.compiler),
                        self.src.span(&span),
                    ));
                }
                let first = self.lower_root_expr(first, &mut var_table)?;
                let second = self.lower_root_expr(second, &mut var_table)?;
                Ok(Some(self.def(
                    DefKind::Equation(Variables(variables.into()), first, second),
                    &span,
                )))
            }
            ast::Ast::Comment(_) => Ok(None), // _ => panic!(),
        }
    }

    fn ast_type_to_def(&mut self, ast_ty: ast::Type, span: &Span) -> Res<DefId> {
        match ast_ty {
            ast::Type::Sym(ident) => {
                match self.compiler.namespaces.lookup(
                    &[self.src.package, CORE_PKG],
                    Space::Type,
                    &ident,
                ) {
                    Some(type_def_id) => Ok(type_def_id),
                    None => Err(self.error(CompileError::TypeNotFound, span)),
                }
            }
            ast::Type::Literal(ast::Literal::String(lit)) => match lit.as_str() {
                "" => Ok(self.compiler.defs.empty_string()),
                _ => {
                    let lit = self.compiler.strings.intern(&lit);
                    Ok(self.compiler.defs.def_string_literal(lit))
                }
            },
            ast::Type::Unit => Ok(self.compiler.defs.unit()),
            ast::Type::EmptySequence => Ok(self.compiler.defs.empty_sequence()),
            ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, span)),
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
                        let key = self.ast_type_to_def(ast_attr.ty.0, &ast_attr.ty.1)?;

                        self.lower_expr(ast_attr.value, var_table)
                            .map(|expr| ((key, self.src.span(&ast_attr.ty.1)), expr))
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
            None => Err(self.error(CompileError::TypeNotFound, span)),
        }
    }

    fn define_relation_if_undefined(&mut self, kind: RelationIdent) -> ImplicitRelationId {
        match kind {
            RelationIdent::Named(def_id) | RelationIdent::Typed(def_id) => {
                match self.compiler.relations.relations.entry(def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(
                        *vacant.insert(RelationId(self.compiler.defs.alloc_def_id())),
                    ),
                    Entry::Occupied(occupied) => ImplicitRelationId::Reused(*occupied.get()),
                }
            }
            RelationIdent::Indexed => {
                ImplicitRelationId::Reused(RelationId(self.compiler.defs.indexed_relation()))
            }
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

    fn set_def(&mut self, def_id: DefId, kind: DefKind<'m>, span: &Span) {
        self.compiler.defs.map.insert(
            def_id,
            self.compiler.defs.mem.bump.alloc(Def {
                id: def_id,
                package: self.src.package,
                kind,
                span: self.src.span(span),
            }),
        );
    }

    fn def(&mut self, kind: DefKind<'m>, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id();
        self.set_def(def_id, kind, span);
        def_id
    }

    fn expr(&self, kind: ExprKind, span: &Span) -> Expr {
        Expr {
            id: ExprId(0),
            kind,
            span: self.src.span(span),
        }
    }

    fn error(&self, compile_error: CompileError, span: &Span) -> SpannedCompileError {
        compile_error.spanned(&self.compiler.sources, &self.src.span(span))
    }
}

enum ImplicitRelationId {
    New(RelationId),
    Reused(RelationId),
}

#[derive(Default)]
struct VarTable {
    variables: HashMap<String, ExprId>,
}

impl VarTable {
    fn new_var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        *self
            .variables
            .entry(ident)
            .or_insert_with(|| compiler.defs.alloc_expr_id())
    }

    fn get_var_id(&self, ident: &str) -> Option<ExprId> {
        self.variables.get(ident).cloned()
    }
}

fn convert_cardinality(
    ast_cardinality: ast::Cardinality,
) -> (PropertyCardinality, ValueCardinality) {
    match ast_cardinality {
        ast::Cardinality::Optional => (PropertyCardinality::Optional, ValueCardinality::One),
        ast::Cardinality::Many => (PropertyCardinality::Mandatory, ValueCardinality::Many),
        ast::Cardinality::OptionalMany => (PropertyCardinality::Optional, ValueCardinality::Many),
    }
}

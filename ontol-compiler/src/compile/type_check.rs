use std::collections::HashMap;

use crate::{
    compiler::Compiler,
    def::{Def, DefId, DefKind, Defs, Primitive, Relation},
    expr::{Expr, ExprId, ExprKind},
    mem::Intern,
    relation::{Relations, Role, SubjectProperties},
    source::{SourceSpan, Sources},
    types::{DefTypes, Type, TypeRef, Types},
};

use super::error::{CompileError, CompileErrors};

pub struct TypeCheck<'c, 'm> {
    types: &'c mut Types<'m>,
    def_types: &'c mut DefTypes<'m>,
    relations: &'c mut Relations,
    errors: &'c mut CompileErrors,
    expressions: &'c HashMap<ExprId, Expr>,
    defs: &'c Defs,
    sources: &'c Sources,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_def(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_types.map.get(&def_id) {
            return type_ref;
        }

        let def = self
            .defs
            .map
            .get(&def_id)
            .expect("BUG: definition not found");

        match &def.kind {
            DefKind::Type(_) => {
                let ty = self.types.intern(Type::Domain(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Relationship(relationship) => {
                let relation = match self.defs.map.get(&relationship.relation_def_id) {
                    Some(Def {
                        kind: DefKind::Relation(relation),
                        ..
                    }) => relation,
                    other => panic!("TODO: relation not found, got {other:?}"),
                };

                self.check_property(
                    def_id,
                    relation,
                    relationship.subject,
                    Role::Subject,
                    relationship.object,
                    &def.span,
                );
                self.check_property(
                    def_id,
                    relation,
                    relationship.object,
                    Role::Object,
                    relationship.subject,
                    &def.span,
                );

                self.types.intern(Type::Tautology)
            }
            DefKind::Primitive(Primitive::Number) => self.types.intern(Type::Number),
            DefKind::Equivalence(first_id, second_id) => {
                let first = self.check_expr_id(*first_id);
                let second = self.check_expr_id(*second_id);
                self.types.intern(Type::Tautology)
            }
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }

    fn check_expr_id(&mut self, expr_id: ExprId) -> TypeRef<'m> {
        match self.expressions.get(&expr_id) {
            Some(expr) => self.check_expr(expr),
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    fn check_expr(&mut self, expr: &Expr) -> TypeRef<'m> {
        match &expr.kind {
            ExprKind::Call(def_id, args) => match self.def_types.map.get(&def_id) {
                Some(Type::Function { params, output }) => {
                    if args.len() != params.len() {
                        return self.error(CompileError::WrongNumberOfArguments, &expr.span);
                    }
                    for (arg, param_ty) in args.iter().zip(*params) {
                        self.check_expr(arg);
                    }
                    *output
                }
                _ => self.error(CompileError::NotCallable, &expr.span),
            },
            ExprKind::Obj(def_id, attributes) => {
                panic!()
            }
            ExprKind::Constant(_) => self.types.intern(Type::Number),
            ExprKind::Variable(id) => {
                panic!()
            }
        }
    }

    fn check_property(
        &mut self,
        relationship_id: DefId,
        relation: &Relation,
        role_def_id: DefId,
        role: Role,
        inverse_role_def_id: DefId,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        /*
        let Ok(role_def_id) = self.expect_domain_type(role_def_id, span) else {
            return self.types.intern(Type::Error);
        };
        */

        let property_codomain = self.check_def(inverse_role_def_id);
        let property_id = self.relations.new_property(relationship_id, role);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(role_def_id);
        match role {
            Role::Subject => match (&relation.ident, &mut properties.subject) {
                (None, SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Anonymous(property_id);
                }
                (None, SubjectProperties::Anonymous(_)) => {
                    return self.error(CompileError::DuplicateAnonymousRelation, span);
                }
                (None, SubjectProperties::Named(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Named([property_id].into());
                }
                (Some(_), SubjectProperties::Anonymous(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Named(properties)) => {
                    properties.insert(property_id);
                }
            },
            Role::Object => {
                properties.object.insert(property_id);
            }
        }

        property_codomain
    }

    fn expect_domain_type(&mut self, def_id: DefId, span: &SourceSpan) -> Result<DefId, ()> {
        match self.check_def(def_id) {
            Type::Domain(type_def_id) => Ok(*type_def_id),
            _ => {
                self.errors
                    .push(CompileError::DomainTypeExpected.spanned(&self.sources, span));
                Err(())
            }
        }
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(&self.sources, span));
        self.types.intern(Type::Error)
    }
}

impl<'m> Compiler<'m> {
    pub fn type_check(&mut self) -> TypeCheck<'_, 'm> {
        TypeCheck {
            types: &mut self.types,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
            relations: &mut self.relations,
            expressions: &self.expressions,
            defs: &self.defs,
            sources: &self.sources,
        }
    }
}

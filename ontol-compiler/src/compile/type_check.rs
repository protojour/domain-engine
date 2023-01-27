use crate::{
    compiler::Compiler,
    def::{Def, DefId, DefKind, Defs, Primitive, Relation},
    expr::{Expr, ExprKind},
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
            DefKind::Equivalence(_, _) => self.types.intern(Type::Tautology),
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }

    fn check_expr(&mut self, expr: &Expr) -> TypeRef<'m> {
        match &expr.kind {
            ExprKind::Constant(_) => self.types.intern(Type::Number),
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
                Some(Type::Data(data_def_id, field_id)) => {
                    if args.len() != 1 {
                        self.error(CompileError::WrongNumberOfArguments, &expr.span);
                    }

                    match self.def_types.map.get(data_def_id) {
                        Some(ty) => ty,
                        None => self.types.intern(Type::Error),
                    }
                }
                _ => {
                    self.errors
                        .push(CompileError::NotCallable.spanned(&self.sources, &expr.span));
                    self.types.intern(Type::Error)
                }
            },
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
            defs: &self.defs,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
            relations: &mut self.relations,
            sources: &self.sources,
        }
    }
}

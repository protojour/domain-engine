use std::{collections::HashMap, ops::Deref};

use ontol_runtime::DefId;

use crate::{
    compiler::Compiler,
    compiler_queries::GetPropertyMeta,
    def::{Def, DefKind, Defs, Primitive, Relation},
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
            ExprKind::Obj(type_path, attributes) => {
                let domain_type = self.check_def(type_path.def_id);
                let Type::Domain(_) = domain_type else {
                    return self.error(CompileError::DomainTypeExpected, &type_path.span);
                };

                let subject_properties = self
                    .relations
                    .properties_by_type(type_path.def_id)
                    .map(|props| &props.subject);

                match subject_properties {
                    Some(SubjectProperties::Unit) | None => {
                        if !attributes.is_empty() {
                            return self.error(CompileError::NoPropertiesExpected, &expr.span);
                        }
                    }
                    Some(SubjectProperties::Value(_)) => match attributes.deref() {
                        [((property, _), _)] if property.is_none() => {}
                        _ => {
                            return self.error(CompileError::AnonymousPropertyExpected, &expr.span)
                        }
                    },
                    Some(SubjectProperties::Map(property_set)) => {
                        struct MatchProperty {
                            object_def: DefId,
                            used: bool,
                        }
                        let mut match_properties = property_set
                            .iter()
                            .map(|property_id| {
                                let (_, relationship, relation) = self
                                    .get_property_meta(*property_id)
                                    .expect("BUG: problem getting property meta");
                                let property_name = relation
                                    .subject_prop()
                                    .expect("BUG: Expected named subject property");

                                (
                                    property_name.clone(),
                                    MatchProperty {
                                        object_def: relationship.object,
                                        used: false,
                                    },
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        for ((attr_prop, prop_span), value) in attributes.iter() {
                            let attr_prop = match attr_prop {
                                Some(attr_prop) => attr_prop,
                                None => {
                                    self.error(CompileError::NamedPropertyExpected, &prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop.as_str())
                            {
                                Some(match_properties) => match_properties,
                                None => {
                                    self.error(CompileError::UnknownProperty, &prop_span);
                                    continue;
                                }
                            };
                            if match_property.used {
                                self.error(CompileError::DuplicateProperty, &prop_span);
                                continue;
                            }
                            match_property.used = true;

                            let object_ty = self.check_def(match_property.object_def);
                            self.check_expr_expect(value, object_ty);
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.error(CompileError::MissingProperty(prop_name), &expr.span);
                            }
                        }
                    }
                }

                domain_type
            }
            ExprKind::Constant(_) => self.types.intern(Type::Number),
            ExprKind::Variable(_) => self.types.intern(Type::Variable),
        }
    }

    fn check_expr_expect(&mut self, expr: &Expr, expected: TypeRef) {
        let ty = self.check_expr(expr);
        match (ty, expected) {
            (Type::Error, _) => {}
            (_, Type::Error) => {}
            (Type::Variable, Type::Variable) => {
                panic!("FIXME: equate variable with variable?");
            }
            (Type::Variable, expected) => {}
            _ => {
                self.error(CompileError::MismatchedType, &expr.span);
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
        let property_codomain = self.check_def(inverse_role_def_id);
        let property_id = self.relations.new_property(relationship_id, role);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(role_def_id);
        match role {
            Role::Subject => match (&relation.ident, &mut properties.subject) {
                (None, SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Value(property_id);
                }
                (None, SubjectProperties::Value(_)) => {
                    return self.error(CompileError::DuplicateAnonymousRelation, span);
                }
                (None, SubjectProperties::Map(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Map([property_id].into());
                }
                (Some(_), SubjectProperties::Value(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Map(properties)) => {
                    properties.insert(property_id);
                }
            },
            Role::Object => {
                properties.object.insert(property_id);
            }
        }

        property_codomain
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(&self.sources, span));
        self.types.intern(Type::Error)
    }
}

impl<'c, 'm> AsRef<Defs> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Defs {
        &self.defs
    }
}

impl<'c, 'm> AsRef<Relations> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        &self.relations
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

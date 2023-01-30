use std::{collections::HashMap, ops::Deref};

use ontol_runtime::DefId;

use crate::{
    compiler_queries::GetPropertyMeta,
    error::CompileError,
    expr::{Expr, ExprId, ExprKind},
    mem::Intern,
    relation::SubjectProperties,
    typed_expr::TypedExprTable,
    types::{Type, TypeRef},
};

use super::{
    inference::{Inference, UnifyValue},
    TypeCheck, TypeError,
};

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub typed_expr_table: TypedExprTable<'m>,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn check_expr_id(
        &mut self,
        expr_id: ExprId,
        ctx: &mut CheckExprContext<'m>,
    ) -> TypeRef<'m> {
        match self.expressions.get(&expr_id) {
            Some(expr) => self.check_expr(expr, ctx),
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    fn check_expr(&mut self, expr: &Expr, ctx: &mut CheckExprContext<'m>) -> TypeRef<'m> {
        match &expr.kind {
            ExprKind::Call(def_id, args) => match self.def_types.map.get(&def_id) {
                Some(Type::Function { params, output }) => {
                    if args.len() != params.len() {
                        return self.error(CompileError::WrongNumberOfArguments, &expr.span);
                    }
                    for (arg, param_ty) in args.iter().zip(*params) {
                        self.check_expr_expect(arg, param_ty, ctx);
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
                    Some(SubjectProperties::Value(property_id)) => match attributes.deref() {
                        [((ast_prop, _), value)] if ast_prop.is_none() => {
                            let (_, relationship, _) = self
                                .get_property_meta(*property_id)
                                .expect("BUG: problem getting anonymous property meta");

                            let object_ty = self.check_def(relationship.object);
                            self.check_expr_expect(value, object_ty, ctx);
                        }
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
                            self.check_expr_expect(value, object_ty, ctx);
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
            ExprKind::Variable(expr_id) => self
                .types
                .intern(Type::Infer(ctx.inference.new_type_variable(*expr_id))),
        }
    }

    fn check_expr_expect(
        &mut self,
        expr: &Expr,
        expected: TypeRef<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) {
        let ty = self.check_expr(expr, ctx);
        match (ty, expected) {
            (Type::Error, _) => {}
            (_, Type::Error) => {}
            (Type::Infer(..), Type::Infer(..)) => {
                panic!("FIXME: equate variable with variable?");
            }
            (Type::Infer(type_var), expected) => {
                if let Err(error) = ctx
                    .inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(expected))
                {
                    self.type_error(error, &expr.span);
                }
            }
            (ty, expected) if ty != expected => {
                self.type_error(
                    TypeError::Mismatch {
                        actual: ty,
                        expected,
                    },
                    &expr.span,
                );
            }
            _ => {
                // OK
            }
        }
    }
}

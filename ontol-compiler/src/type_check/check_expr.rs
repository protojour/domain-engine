use std::{collections::HashMap, ops::Deref};

use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, DefId, RelationId};
use tracing::warn;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind},
    mem::Intern,
    relation::SubjectProperties,
    typed_expr::{ExprRef, TypedExpr, TypedExprKind, TypedExprTable, ERROR_NODE},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    inference::{Inference, UnifyValue},
    TypeCheck, TypeError,
};

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub typed_expr_table: TypedExprTable<'m>,
    pub bound_variables: HashMap<ExprId, ExprRef>,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn check_expr_id(
        &mut self,
        expr_id: ExprId,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, ExprRef) {
        match self.expressions.get(&expr_id) {
            Some(expr) => self.check_expr(expr, ctx),
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    fn check_expr(
        &mut self,
        expr: &Expr,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, ExprRef) {
        match &expr.kind {
            ExprKind::Call(def_id, args) => {
                match (self.defs.map.get(def_id), self.def_types.map.get(def_id)) {
                    (
                        Some(Def {
                            kind: DefKind::CoreFn(proc),
                            ..
                        }),
                        Some(Type::Function { params, output }),
                    ) => {
                        if args.len() != params.len() {
                            return self.expr_error(
                                CompileError::IncorrectNumberOfArguments {
                                    expected: u8::try_from(params.len()).unwrap(),
                                    actual: u8::try_from(args.len()).unwrap(),
                                },
                                &expr.span,
                            );
                        }

                        let mut param_expr_refs = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let (_, typed_expr_ref) = self.check_expr_expect(arg, param_ty, ctx);
                            param_expr_refs.push(typed_expr_ref);
                        }

                        let call_expr_ref = ctx.typed_expr_table.add_expr(TypedExpr {
                            kind: TypedExprKind::Call(*proc, param_expr_refs.into()),
                            ty: output,
                            span: expr.span,
                        });

                        (*output, call_expr_ref)
                    }
                    _ => self.expr_error(CompileError::NotCallable, &expr.span),
                }
            }
            ExprKind::Obj(type_path, attributes) => {
                let domain_type = self.check_def(type_path.def_id);
                let subject_id = match domain_type {
                    Type::Domain(subject_id) | Type::DomainEntity(subject_id) => subject_id,
                    _ => return self.expr_error(CompileError::DomainTypeExpected, &type_path.span),
                };

                let subject_properties = self
                    .relations
                    .properties_by_type(type_path.def_id)
                    .map(|props| &props.subject);

                let typed_expr_ref = match subject_properties {
                    Some(SubjectProperties::Empty) | None => {
                        if !attributes.is_empty() {
                            return self.expr_error(CompileError::NoPropertiesExpected, &expr.span);
                        }
                        ctx.typed_expr_table.add_expr(TypedExpr {
                            kind: TypedExprKind::Unit,
                            ty: domain_type,
                            span: expr.span,
                        })
                    }
                    Some(SubjectProperties::Value(relationship_id, _, _)) => {
                        match attributes.deref() {
                            [((prop_ident, _), value)] if prop_ident.is_none() => {
                                let (relationship, _) = self
                                    .get_relationship_meta(*relationship_id)
                                    .expect("BUG: problem getting anonymous property meta");

                                let object_ty = self.check_def(relationship.object);
                                let typed_expr_ref =
                                    self.check_expr_expect(value, object_ty, ctx).1;

                                ctx.typed_expr_table.add_expr(TypedExpr {
                                    ty: domain_type,
                                    kind: TypedExprKind::ValueObjPattern(typed_expr_ref),
                                    span: expr.span,
                                })
                            }
                            _ => {
                                return self.expr_error(
                                    CompileError::AnonymousPropertyExpected,
                                    &expr.span,
                                )
                            }
                        }
                    }
                    Some(SubjectProperties::ValueUnion(_property_set)) => {
                        todo!()
                    }
                    Some(SubjectProperties::Map(property_set)) => {
                        struct MatchProperty {
                            relation_id: RelationId,
                            cardinality: Cardinality,
                            object_def: DefId,
                            used: bool,
                        }
                        let mut match_properties = property_set
                            .iter()
                            .map(|(relation_id, _cardinality)| {
                                let (relationship, relation) = self
                                    .get_subject_property_meta(*subject_id, *relation_id)
                                    .expect("BUG: problem getting property meta");
                                let property_name = relation
                                    .subject_prop()
                                    .expect("BUG: Expected named subject property");

                                (
                                    property_name,
                                    MatchProperty {
                                        relation_id: *relation_id,
                                        cardinality: relationship.subject_cardinality,
                                        object_def: relationship.object,
                                        used: false,
                                    },
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        let mut typed_properties = IndexMap::new();

                        for ((attr_prop, prop_span), value) in attributes.iter() {
                            let attr_prop = match attr_prop {
                                Some(attr_prop) => attr_prop,
                                None => {
                                    self.error(CompileError::NamedPropertyExpected, prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop.as_str())
                            {
                                Some(match_properties) => match_properties,
                                None => {
                                    self.error(CompileError::UnknownProperty, prop_span);
                                    continue;
                                }
                            };
                            if match_property.used {
                                self.error(CompileError::DuplicateProperty, prop_span);
                                continue;
                            }
                            match_property.used = true;

                            let object_ty = self.check_def(match_property.object_def);
                            let object_ty = match match_property.cardinality {
                                (PropertyCardinality::Mandatory, ValueCardinality::One) => {
                                    object_ty
                                }
                                (PropertyCardinality::Optional, ValueCardinality::One) => {
                                    self.types.intern(Type::Option(object_ty))
                                }
                                (_, ValueCardinality::Many) => {
                                    self.types.intern(Type::Array(object_ty))
                                }
                                (_, ValueCardinality::ManyInRange(_, _)) => todo!(),
                            };
                            let (_, typed_expr_ref) = self.check_expr_expect(value, object_ty, ctx);

                            typed_properties.insert(
                                PropertyId::subject(match_property.relation_id),
                                typed_expr_ref,
                            );
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.error(
                                    CompileError::MissingProperty(prop_name.into()),
                                    &expr.span,
                                );
                            }
                        }

                        ctx.typed_expr_table.add_expr(TypedExpr {
                            ty: domain_type,
                            kind: TypedExprKind::MapObjPattern(typed_properties),
                            span: expr.span,
                        })
                    }
                    Some(SubjectProperties::Tuple(_)) => todo!(),
                };

                (domain_type, typed_expr_ref)
            }
            ExprKind::Constant(k) => {
                let ty = self.def_types.map.get(&self.defs.int()).unwrap();
                (
                    ty,
                    ctx.typed_expr_table.add_expr(TypedExpr {
                        ty,
                        kind: TypedExprKind::Constant(*k),
                        span: expr.span,
                    }),
                )
            }
            ExprKind::Variable(expr_id) => {
                let ty = self
                    .types
                    .intern(Type::Infer(ctx.inference.new_type_variable(*expr_id)));
                let var_ref = ctx
                    .bound_variables
                    .get(expr_id)
                    .expect("variable not found");

                (
                    ty,
                    ctx.typed_expr_table.add_expr(TypedExpr {
                        ty,
                        kind: TypedExprKind::VariableRef(*var_ref),
                        span: expr.span,
                    }),
                )
            }
        }
    }

    fn check_expr_expect(
        &mut self,
        expr: &Expr,
        expected: TypeRef<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, ExprRef) {
        let (ty, typed_expr_ref) = self.check_expr(expr, ctx);
        match (ty, expected) {
            (Type::Error, _) => (ty, ERROR_NODE),
            (_, Type::Error) => (expected, ERROR_NODE),
            (Type::Infer(..), Type::Infer(..)) => {
                panic!("FIXME: equate variable with variable?");
            }
            (Type::Infer(type_var), expected) => {
                if let Err(TypeError::Mismatch { actual, expected }) = ctx
                    .inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(expected))
                {
                    self.translate_if_possible(ctx, expr, typed_expr_ref, actual, expected)
                } else {
                    warn!("TODO: resolve var?");
                    (ty, typed_expr_ref)
                }
            }
            (ty, expected) if ty != expected => {
                self.translate_if_possible(ctx, expr, typed_expr_ref, ty, expected)
            }
            _ => {
                // Ok
                (ty, typed_expr_ref)
            }
        }
    }

    fn translate_if_possible(
        &mut self,
        ctx: &mut CheckExprContext<'m>,
        expr: &Expr,
        typed_expr_ref: ExprRef,
        actual: TypeRef<'m>,
        expected: TypeRef<'m>,
    ) -> (TypeRef<'m>, ExprRef) {
        match (actual, expected) {
            (Type::Domain(_), Type::Domain(_)) => {
                let translation_node = ctx.typed_expr_table.add_expr(TypedExpr {
                    ty: expected,
                    kind: TypedExprKind::Translate(typed_expr_ref, actual),
                    span: expr.span,
                });
                (expected, translation_node)
            }
            _ => {
                let err = self.type_error(TypeError::Mismatch { actual, expected }, &expr.span);
                (err, ERROR_NODE)
            }
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, ExprRef) {
        self.errors.push(error.spanned(self.sources, span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

use std::ops::Deref;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, DefId, RelationId, Role};
use tracing::warn;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, DefReference, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, TypePath},
    mem::Intern,
    relation::Constructor,
    typed_expr::{
        BindDepth, ExprRef, SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable, ERROR_NODE,
    },
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    inference::{Inference, UnifyValue},
    TypeCheck, TypeEquation, TypeError,
};

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub expressions: TypedExprTable<'m>,
    pub bound_variables: FnvHashMap<ExprId, ExprRef>,
    bind_depth: BindDepth,
}

impl<'m> CheckExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            expressions: TypedExprTable::default(),
            bound_variables: Default::default(),
            bind_depth: BindDepth(0),
        }
    }

    pub fn enter_bind_level<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.bind_depth.0 += 1;
        let ret = f(self);
        self.bind_depth.0 -= 1;
        ret
    }

    pub fn syntax_var(&self, id: u16) -> SyntaxVar {
        SyntaxVar(id, self.bind_depth)
    }
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

                        let call_expr_ref = ctx.expressions.add(TypedExpr {
                            kind: TypedExprKind::Call(*proc, param_expr_refs.into()),
                            ty: output,
                            span: expr.span,
                        });

                        (*output, call_expr_ref)
                    }
                    _ => self.expr_error(CompileError::NotCallable, &expr.span),
                }
            }
            ExprKind::Struct(type_path, attributes) => {
                self.check_struct(expr, type_path, attributes, ctx)
            }
            // right: {7} StructPattern(
            //     {6} (rel 1, 11) SequenceMap(
            //         {3->0} Variable(0 d=0),
            //         SyntaxVar(0 d=1),
            //         {5} Translate(
            //             {4} Variable(0 d=1),
            //         ),
            //     ),
            // )
            ExprKind::Seq(inner) => ctx.enter_bind_level(|ctx| {
                warn!("FIXME: Check Seq");
                let (inner_ty, inner_ref) = self.check_expr(inner, ctx);

                let array_ty = self.types.intern(Type::Array(inner_ty));

                let iter_var = ctx.syntax_var(0);
                let _iter_ref = ctx.expressions.add(TypedExpr {
                    ty: self.types.intern(Type::Tautology),
                    kind: TypedExprKind::Variable(iter_var),
                    span: expr.span,
                });

                let map_sequence_ref = ctx.expressions.add(TypedExpr {
                    ty: array_ty,
                    kind: TypedExprKind::MapSequence(inner_ref, iter_var, inner_ref, inner_ty),
                    span: expr.span,
                });

                (array_ty, map_sequence_ref)
            }),
            ExprKind::Constant(k) => {
                let ty = self.def_types.map.get(&self.primitives.int).unwrap();
                (
                    ty,
                    ctx.expressions.add(TypedExpr {
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
                    ctx.expressions.add(TypedExpr {
                        ty,
                        kind: TypedExprKind::VariableRef(*var_ref),
                        span: expr.span,
                    }),
                )
            }
        }
    }

    fn check_struct(
        &mut self,
        expr: &Expr,
        type_path: &TypePath,
        attributes: &[((DefReference, SourceSpan), Expr)],
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, ExprRef) {
        let domain_type = self.check_def(type_path.def_id);
        let subject_id = match domain_type {
            Type::Domain(subject_id) => subject_id,
            _ => return self.expr_error(CompileError::DomainTypeExpected, &type_path.span),
        };

        let properties = self.relations.properties_by_type(type_path.def_id);

        let typed_expr_ref = match properties.map(|props| &props.constructor) {
            Some(Constructor::Struct) | None => {
                match properties.and_then(|props| props.map.as_ref()) {
                    Some(property_set) => {
                        struct MatchProperty {
                            relation_id: RelationId,
                            cardinality: Cardinality,
                            object_def: DefId,
                            used: bool,
                        }
                        let mut match_properties = property_set
                            .iter()
                            .filter_map(|(property_id, _cardinality)| match property_id.role {
                                Role::Subject => {
                                    let (relationship, relation) = self
                                        .property_meta_by_subject(
                                            *subject_id,
                                            property_id.relation_id,
                                        )
                                        .expect("BUG: problem getting property meta");
                                    let property_name = relation
                                        .subject_prop(self.defs)
                                        .expect("BUG: Expected named subject property");

                                    Some((
                                        property_name,
                                        MatchProperty {
                                            relation_id: property_id.relation_id,
                                            cardinality: relationship.subject_cardinality,
                                            object_def: relationship.object.0.def_id,
                                            used: false,
                                        },
                                    ))
                                }
                                Role::Object => None,
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut typed_properties = IndexMap::new();

                        for ((def, prop_span), expr) in attributes.iter() {
                            let attr_prop = match self.defs.get_def_kind(def.def_id) {
                                Some(DefKind::StringLiteral(lit)) => lit,
                                _ => {
                                    self.error(CompileError::NamedPropertyExpected, prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop) {
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
                            };
                            let (_, typed_expr_ref) = self.check_expr_expect(expr, object_ty, ctx);

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

                        ctx.expressions.add(TypedExpr {
                            ty: domain_type,
                            kind: TypedExprKind::StructPattern(typed_properties),
                            span: expr.span,
                        })
                    }
                    None => {
                        if !attributes.is_empty() {
                            return self.expr_error(CompileError::NoPropertiesExpected, &expr.span);
                        }
                        ctx.expressions.add(TypedExpr {
                            kind: TypedExprKind::Unit,
                            ty: domain_type,
                            span: expr.span,
                        })
                    }
                }
            }
            Some(Constructor::Value(relationship_id, _, _)) => match attributes.deref() {
                [((def, _), value)] if def.def_id == DefId::unit() => {
                    let (relationship, _) = self
                        .get_relationship_meta(*relationship_id)
                        .expect("BUG: problem getting anonymous property meta");

                    let object_ty = self.check_def(relationship.object.0.def_id);
                    let typed_expr_ref = self.check_expr_expect(value, object_ty, ctx).1;

                    ctx.expressions.add(TypedExpr {
                        ty: domain_type,
                        kind: TypedExprKind::ValuePattern(typed_expr_ref),
                        span: expr.span,
                    })
                }
                _ => return self.expr_error(CompileError::AnonymousPropertyExpected, &expr.span),
            },
            Some(Constructor::Intersection(_)) => {
                todo!()
            }
            Some(Constructor::Union(_property_set)) => {
                return self.expr_error(CompileError::CannotMapUnion, &expr.span)
            }
            Some(Constructor::Sequence(_)) => todo!(),
            Some(Constructor::StringFmt(_)) => todo!(),
        };

        (domain_type, typed_expr_ref)
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
                match ctx
                    .inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(expected))
                {
                    Ok(_) => {
                        warn!("TODO: resolve var?");
                        (ty, typed_expr_ref)
                    }
                    Err(TypeError::Mismatch(type_eq)) => self
                        .map_if_possible(ctx, expr, typed_expr_ref, type_eq)
                        .unwrap_or_else(|_| {
                            (
                                self.type_error(TypeError::Mismatch(type_eq), &expr.span),
                                ERROR_NODE,
                            )
                        }),
                    Err(err) => (self.type_error(err, &expr.span), ERROR_NODE),
                }
            }
            (ty, expected) if ty != expected => {
                let type_eq = TypeEquation {
                    actual: ty,
                    expected,
                };
                self.map_if_possible(ctx, expr, typed_expr_ref, type_eq)
                    .unwrap_or_else(|_| {
                        (
                            self.type_error(TypeError::Mismatch(type_eq), &expr.span),
                            ERROR_NODE,
                        )
                    })
            }
            _ => {
                // Ok
                (ty, typed_expr_ref)
            }
        }
    }

    fn map_if_possible(
        &mut self,
        ctx: &mut CheckExprContext<'m>,
        expr: &Expr,
        typed_expr_ref: ExprRef,
        type_eq: TypeEquation<'m>,
    ) -> Result<(TypeRef<'m>, ExprRef), ()> {
        match (type_eq.actual, type_eq.expected) {
            (Type::Domain(_), Type::Domain(_)) => {
                let map_node = ctx.expressions.add(TypedExpr {
                    ty: type_eq.expected,
                    kind: TypedExprKind::MapValue(typed_expr_ref, type_eq.actual),
                    span: expr.span,
                });
                Ok((type_eq.expected, map_node))
            }
            (Type::Array(actual_item), Type::Array(expected_item)) => ctx.enter_bind_level(|ctx| {
                let iter_var = ctx.syntax_var(0);
                let iter_ref = ctx.expressions.add(TypedExpr {
                    ty: self.types.intern(Type::Tautology),
                    kind: TypedExprKind::Variable(iter_var),
                    span: expr.span,
                });

                let (_, map_expr_ref) = self.map_if_possible(
                    ctx,
                    expr,
                    iter_ref,
                    TypeEquation {
                        actual: actual_item,
                        expected: expected_item,
                    },
                )?;
                let map_sequence_node = ctx.expressions.add(TypedExpr {
                    ty: type_eq.expected,
                    kind: TypedExprKind::MapSequence(
                        typed_expr_ref,
                        iter_var,
                        map_expr_ref,
                        type_eq.actual,
                    ),
                    span: expr.span,
                });

                Ok((type_eq.expected, map_sequence_node))
            }),
            _ => Err(()),
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, ExprRef) {
        self.errors.push(error.spanned(span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

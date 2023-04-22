use std::ops::Deref;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, DefId, RelationId, Role};
use tracing::{debug, warn};

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
    pub bound_variables: FnvHashMap<ExprId, BoundVariable>,
    pub aggr_variables: FnvHashMap<ExprId, ExprRef>,
    pub aggr_forest: AggregationForest,
    bind_depth: BindDepth,
    syntax_var_allocations: Vec<u16>,
}

pub struct BoundVariable {
    pub syntax_var: SyntaxVar,
    pub expr_ref: ExprRef,
    pub aggr_group: Option<AggregationGroup>,
}

impl<'m> CheckExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            expressions: TypedExprTable::default(),
            bound_variables: Default::default(),
            aggr_variables: Default::default(),
            aggr_forest: Default::default(),
            bind_depth: BindDepth(0),
            syntax_var_allocations: vec![0],
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_aggregation<T>(&mut self, f: impl FnOnce(&mut Self, SyntaxVar) -> T) -> T {
        // There is a unique bind depth for the aggregation variable:
        let aggregation_var = SyntaxVar(0, BindDepth(self.bind_depth.0 + 1));

        self.bind_depth.0 += 2;
        self.syntax_var_allocations.push(0);
        self.syntax_var_allocations.push(0);

        let ret = f(self, aggregation_var);

        self.bind_depth.0 -= 2;
        self.syntax_var_allocations.pop();
        self.syntax_var_allocations.pop();

        ret
    }

    pub fn alloc_syntax_var(&mut self) -> SyntaxVar {
        let alloc = self
            .syntax_var_allocations
            .get_mut(self.bind_depth.0 as usize)
            .unwrap();
        let syntax_var = SyntaxVar(*alloc, self.bind_depth);
        *alloc += 1;
        syntax_var
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AggregationGroup {
    pub expr_ref: ExprRef,
    pub bind_depth: BindDepth,
}

/// Tracks which aggregations are children of other aggregations
#[derive(Default)]
pub struct AggregationForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<ExprRef, ExprRef>,
}

impl AggregationForest {
    pub fn insert(&mut self, aggr: ExprRef, parent: Option<ExprRef>) {
        self.map.insert(aggr, parent.unwrap_or(aggr));
    }

    pub fn find_parent(&self, aggr: ExprRef) -> Option<ExprRef> {
        let parent = self.map.get(&aggr).unwrap();
        if parent == &aggr {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut aggr: ExprRef) -> ExprRef {
        loop {
            let parent = self.map.get(&aggr).unwrap();
            if parent == &aggr {
                return aggr;
            }
            aggr = *parent;
        }
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
            ExprKind::Seq(aggr_id, inner) => {
                debug!("Seq(aggr_id = {aggr_id:?})");

                // The variables outside the aggregation refer to the aggregated object (an array).
                let seq_inner_ty_var = self
                    .types
                    .intern(Type::Infer(ctx.inference.new_type_variable(*aggr_id)));
                let array_ty = self.types.intern(Type::Array(seq_inner_ty_var));

                let seq_ref = ctx
                    .aggr_variables
                    .get(aggr_id)
                    .expect("aggregated reference not found");
                let seq_variable_ref = ctx.expressions.add(TypedExpr {
                    ty: array_ty,
                    kind: TypedExprKind::VariableRef(*seq_ref),
                    span: expr.span,
                });

                ctx.enter_aggregation(|ctx, aggr_var| {
                    // The inner variables represent each element being aggregated
                    let (element_ty, element_ref) = self.check_expr(inner, ctx);

                    let array_ty = self.types.intern(Type::Array(element_ty));

                    let _iter_ref = ctx.expressions.add(TypedExpr {
                        ty: self.types.intern(Type::Tautology),
                        kind: TypedExprKind::Variable(aggr_var),
                        span: expr.span,
                    });

                    let map_sequence_ref = ctx.expressions.add(TypedExpr {
                        ty: array_ty,
                        kind: TypedExprKind::MapSequence(
                            seq_variable_ref,
                            aggr_var,
                            element_ref,
                            element_ty,
                        ),
                        span: expr.span,
                    });

                    (array_ty, map_sequence_ref)
                })
            }
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
                let bound_variable = ctx
                    .bound_variables
                    .get(expr_id)
                    .expect("variable not found");

                (
                    ty,
                    ctx.expressions.add(TypedExpr {
                        ty,
                        kind: TypedExprKind::VariableRef(bound_variable.expr_ref),
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
            (Type::Array(actual_item), Type::Array(expected_item)) => {
                ctx.enter_aggregation(|ctx, iter_var| {
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
                })
            }
            _ => Err(()),
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, ExprRef) {
        self.errors.push(error.spanned(span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

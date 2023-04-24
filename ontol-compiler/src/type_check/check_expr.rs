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
    ir_node::{
        BindDepth, IrKind, IrNode, IrNodeId, IrNodeTable, MapBody, MapBodyId, SyntaxVar, ERROR_NODE,
    },
    mem::Intern,
    relation::Constructor,
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    inference::{Inference, UnifyValue},
    TypeCheck, TypeEquation, TypeError,
};

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub bodies: Vec<MapBody>,
    pub nodes: IrNodeTable<'m>,
    pub bound_variables: FnvHashMap<ExprId, BoundVariable>,
    pub aggr_body_map: FnvHashMap<ExprId, MapBodyId>,

    pub aggr_forest: AggregationForest,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    bind_depth: BindDepth,
    syntax_var_allocations: Vec<u16>,
    body_id_counter: u32,
}

impl<'m> CheckExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            bodies: Default::default(),
            nodes: IrNodeTable::default(),
            bound_variables: Default::default(),
            aggr_body_map: Default::default(),
            aggr_forest: Default::default(),
            arm: Arm::First,
            bind_depth: BindDepth(0),
            syntax_var_allocations: vec![0],
            body_id_counter: 0,
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

    pub fn alloc_map_body_id(&mut self) -> MapBodyId {
        let next = self.body_id_counter;
        self.body_id_counter += 1;
        self.bodies.push(MapBody::default());
        MapBodyId(next)
    }

    pub fn map_body_mut(&mut self, id: MapBodyId) -> &mut MapBody {
        self.bodies.get_mut(id.0 as usize).unwrap()
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

pub struct BoundVariable {
    pub syntax_var: SyntaxVar,
    pub node_id: IrNodeId,
    pub aggr_group: Option<AggregationGroup>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AggregationGroup {
    pub body_id: MapBodyId,
    pub bind_depth: BindDepth,
}

/// Tracks which aggregations are children of other aggregations
#[derive(Default)]
pub struct AggregationForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<MapBodyId, MapBodyId>,
}

impl AggregationForest {
    pub fn insert(&mut self, aggr: MapBodyId, parent: Option<MapBodyId>) {
        self.map.insert(aggr, parent.unwrap_or(aggr));
    }

    pub fn find_parent(&self, aggr: MapBodyId) -> Option<MapBodyId> {
        let parent = self.map.get(&aggr).unwrap();
        if parent == &aggr {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut aggr: MapBodyId) -> MapBodyId {
        loop {
            let parent = self.map.get(&aggr).unwrap();
            if parent == &aggr {
                return aggr;
            }
            aggr = *parent;
        }
    }
}

#[derive(Clone, Copy)]
pub enum Arm {
    First,
    Second,
}

impl Arm {
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn check_expr_id(
        &mut self,
        expr_id: ExprId,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, IrNodeId) {
        match self.expressions.get(&expr_id) {
            Some(expr) => self.check_expr(expr, ctx),
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    fn check_expr(
        &mut self,
        expr: &Expr,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, IrNodeId) {
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

                        let mut param_node_ids = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let (_, node_id) = self.check_expr_expect(arg, param_ty, ctx);
                            param_node_ids.push(node_id);
                        }

                        let call_id = ctx.nodes.add(IrNode {
                            kind: IrKind::Call(*proc, param_node_ids.into()),
                            ty: output,
                            span: expr.span,
                        });

                        (*output, call_id)
                    }
                    _ => self.expr_error(CompileError::NotCallable, &expr.span),
                }
            }
            ExprKind::Struct(type_path, attributes) => {
                self.check_struct(expr, type_path, attributes, ctx)
            }
            ExprKind::Seq(aggr_expr_id, inner) => {
                // The variables outside the aggregation refer to the aggregated object (an array).
                let aggr_body_id = *ctx.aggr_body_map.get(aggr_expr_id).unwrap();

                debug!("Seq(aggr_expr_id = {aggr_expr_id:?}) body_id={aggr_body_id:?}");

                let (element_ty, element_node_id) = self.check_expr(inner, ctx);
                let array_ty = self.types.intern(Type::Array(element_ty));

                match ctx.arm {
                    Arm::First => {
                        ctx.map_body_mut(aggr_body_id).first = element_node_id;
                    }
                    Arm::Second => {
                        ctx.map_body_mut(aggr_body_id).second = element_node_id;
                    }
                }

                let aggr_node_id = ctx.nodes.add(IrNode {
                    ty: array_ty,
                    kind: IrKind::Aggr(aggr_body_id),
                    span: expr.span,
                });
                (array_ty, aggr_node_id)
            }
            ExprKind::Constant(k) => {
                let ty = self.def_types.map.get(&self.primitives.int).unwrap();
                (
                    ty,
                    ctx.nodes.add(IrNode {
                        ty,
                        kind: IrKind::Constant(*k),
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
                    ctx.nodes.add(IrNode {
                        ty,
                        kind: IrKind::VariableRef(bound_variable.node_id),
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
    ) -> (TypeRef<'m>, IrNodeId) {
        let domain_type = self.check_def(type_path.def_id);
        let subject_id = match domain_type {
            Type::Domain(subject_id) => subject_id,
            _ => return self.expr_error(CompileError::DomainTypeExpected, &type_path.span),
        };

        let properties = self.relations.properties_by_type(type_path.def_id);

        let node_id = match properties.map(|props| &props.constructor) {
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
                            let (_, node_id) = self.check_expr_expect(expr, object_ty, ctx);

                            typed_properties
                                .insert(PropertyId::subject(match_property.relation_id), node_id);
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.error(
                                    CompileError::MissingProperty(prop_name.into()),
                                    &expr.span,
                                );
                            }
                        }

                        ctx.nodes.add(IrNode {
                            ty: domain_type,
                            kind: IrKind::StructPattern(typed_properties),
                            span: expr.span,
                        })
                    }
                    None => {
                        if !attributes.is_empty() {
                            return self.expr_error(CompileError::NoPropertiesExpected, &expr.span);
                        }
                        ctx.nodes.add(IrNode {
                            kind: IrKind::Unit,
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
                    let node_id = self.check_expr_expect(value, object_ty, ctx).1;

                    ctx.nodes.add(IrNode {
                        ty: domain_type,
                        kind: IrKind::ValuePattern(node_id),
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

        (domain_type, node_id)
    }

    fn check_expr_expect(
        &mut self,
        expr: &Expr,
        expected: TypeRef<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, IrNodeId) {
        let (ty, node_id) = self.check_expr(expr, ctx);
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
                        (ty, node_id)
                    }
                    Err(TypeError::Mismatch(type_eq)) => self
                        .map_if_possible(ctx, expr, node_id, type_eq)
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
                self.map_if_possible(ctx, expr, node_id, type_eq)
                    .unwrap_or_else(|_| {
                        (
                            self.type_error(TypeError::Mismatch(type_eq), &expr.span),
                            ERROR_NODE,
                        )
                    })
            }
            _ => {
                // Ok
                (ty, node_id)
            }
        }
    }

    fn map_if_possible(
        &mut self,
        ctx: &mut CheckExprContext<'m>,
        expr: &Expr,
        node_id: IrNodeId,
        type_eq: TypeEquation<'m>,
    ) -> Result<(TypeRef<'m>, IrNodeId), ()> {
        match (type_eq.actual, type_eq.expected) {
            (Type::Domain(_), Type::Domain(_)) => {
                let map_node = ctx.nodes.add(IrNode {
                    ty: type_eq.expected,
                    kind: IrKind::MapCall(node_id, type_eq.actual),
                    span: expr.span,
                });
                Ok((type_eq.expected, map_node))
            }
            (Type::Array(actual_item), Type::Array(expected_item)) => {
                ctx.enter_aggregation(|ctx, iter_var| {
                    let iter_id = ctx.nodes.add(IrNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: IrKind::Variable(iter_var),
                        span: expr.span,
                    });

                    let (_, map_node_id) = self.map_if_possible(
                        ctx,
                        expr,
                        iter_id,
                        TypeEquation {
                            actual: actual_item,
                            expected: expected_item,
                        },
                    )?;
                    let map_sequence_node = ctx.nodes.add(IrNode {
                        ty: type_eq.expected,
                        kind: IrKind::MapSequence(node_id, iter_var, map_node_id, type_eq.actual),
                        span: expr.span,
                    });

                    Ok((type_eq.expected, map_sequence_node))
                })
            }
            _ => Err(()),
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, IrNodeId) {
        self.errors.push(error.spanned(span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

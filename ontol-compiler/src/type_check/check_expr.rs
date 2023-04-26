use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, value::PropertyId, DefId, RelationId, Role};
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, DefReference, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, TypePath},
    hir_node::{
        BindDepth, HirBodyIdx, HirIdx, HirKind, HirNode, HirNodeTable, HirVariable, ERROR_NODE,
    },
    mem::Intern,
    relation::Constructor,
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    inference::{InferBestEffort, InferExpect, InferRec, Inference, UnifyValue},
    TypeCheck, TypeEquation, TypeError,
};

#[derive(Default)]
pub struct ExprBody<'m> {
    pub first: Option<ExprRoot<'m>>,
    pub second: Option<ExprRoot<'m>>,
}

pub struct ExprRoot<'m> {
    pub id: ExprId,
    pub expr: Expr,
    pub expected_ty: Option<TypeRef<'m>>,
}

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub bodies: Vec<ExprBody<'m>>,
    pub nodes: HirNodeTable<'m>,
    pub bound_variables: FnvHashMap<ExprId, BoundVariable>,
    pub aggr_variables: FnvHashMap<HirBodyIdx, HirIdx>,
    pub aggr_body_map: FnvHashMap<ExprId, HirBodyIdx>,

    pub aggr_forest: AggregationForest,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    bind_depth: BindDepth,
    hir_var_allocations: Vec<u16>,
    body_id_counter: u32,
}

impl<'m> CheckExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            bodies: Default::default(),
            nodes: HirNodeTable::default(),
            bound_variables: Default::default(),
            aggr_variables: Default::default(),
            aggr_body_map: Default::default(),
            aggr_forest: Default::default(),
            arm: Arm::First,
            bind_depth: BindDepth(0),
            hir_var_allocations: vec![0],
            body_id_counter: 0,
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_aggregation<T>(&mut self, f: impl FnOnce(&mut Self, HirVariable) -> T) -> T {
        // There is a unique bind depth for the aggregation variable:
        let aggregation_var = self.alloc_hir_variable();

        self.bind_depth.0 += 1;
        let ret = f(self, aggregation_var);
        self.bind_depth.0 -= 1;

        ret
    }

    pub fn alloc_map_body_id(&mut self) -> HirBodyIdx {
        let next = self.body_id_counter;
        self.body_id_counter += 1;
        self.bodies.push(ExprBody::default());
        HirBodyIdx(next)
    }

    pub fn expr_body_mut(&mut self, id: HirBodyIdx) -> &mut ExprBody<'m> {
        self.bodies.get_mut(id.0 as usize).unwrap()
    }

    pub fn alloc_hir_variable(&mut self) -> HirVariable {
        let alloc = self.hir_var_allocations.get_mut(0).unwrap();
        let hir_var = HirVariable(*alloc);
        *alloc += 1;
        hir_var
    }
}

pub struct BoundVariable {
    pub syntax_var: HirVariable,
    pub node_id: HirIdx,
    pub aggr_group: Option<AggregationGroup>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AggregationGroup {
    pub body_id: HirBodyIdx,
    pub bind_depth: BindDepth,
}

/// Tracks which aggregations are children of other aggregations
#[derive(Default)]
pub struct AggregationForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<HirBodyIdx, HirBodyIdx>,
}

impl AggregationForest {
    pub fn insert(&mut self, aggr: HirBodyIdx, parent: Option<HirBodyIdx>) {
        self.map.insert(aggr, parent.unwrap_or(aggr));
    }

    pub fn find_parent(&self, aggr: HirBodyIdx) -> Option<HirBodyIdx> {
        let parent = self.map.get(&aggr).unwrap();
        if parent == &aggr {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut aggr: HirBodyIdx) -> HirBodyIdx {
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
    pub(super) fn consume_expr(&mut self, expr_id: ExprId) -> ExprRoot<'m> {
        match self.expressions.map.remove(&expr_id) {
            Some(expr) => ExprRoot {
                id: expr_id,
                expr,
                expected_ty: None,
            },
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    pub(super) fn check_expr_root(
        &mut self,
        root: ExprRoot<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, HirIdx) {
        let (ty, hir_idx) = self.check_expr(root.expr, root.expected_ty, ctx);

        // Save typing result for the final type unification:
        match ty {
            Type::Error | Type::Infer(_) => {}
            _ => {
                let type_var = ctx.inference.new_type_variable(root.id);
                debug!("Check expr root type result: {ty:?}");
                ctx.inference
                    .eq_relations
                    .unify_var_value(type_var, UnifyValue::Known(ty))
                    .unwrap();
            }
        }

        (ty, hir_idx)
    }

    fn check_expr(
        &mut self,
        expr: Expr,
        expected_ty: Option<TypeRef<'m>>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, HirIdx) {
        match (expr.kind, expected_ty) {
            (ExprKind::Call(def_id, args), Some(_expected_output)) => {
                match (self.defs.map.get(&def_id), self.def_types.map.get(&def_id)) {
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
                        for (arg, param_ty) in args.into_vec().into_iter().zip(*params) {
                            let (_, node_id) = self.check_expr_expect(arg, param_ty, ctx);
                            param_node_ids.push(node_id);
                        }

                        let call_id = ctx.nodes.add(HirNode {
                            kind: HirKind::Call(*proc, param_node_ids.into()),
                            ty: output,
                            span: expr.span,
                        });

                        (*output, call_id)
                    }
                    _ => self.expr_error(CompileError::NotCallable, &expr.span),
                }
            }
            (ExprKind::Struct(type_path, attributes), _expected_ty) => {
                self.check_struct(type_path, attributes, expr.span, ctx)
            }
            (ExprKind::Seq(aggr_expr_id, inner), expected_ty) => {
                // The variables outside the aggregation refer to the aggregated object (an array).
                let aggr_body_idx = *ctx.aggr_body_map.get(&aggr_expr_id).unwrap();

                let expr_id = self.expressions.alloc_expr_id();

                let elem_ty = match expected_ty {
                    Some(Type::Array(elem_ty)) => elem_ty,
                    Some(_) => {
                        return self.expr_error(
                            CompileError::TODO(smart_format!("Must be a sequence")),
                            &expr.span,
                        )
                    }
                    None => self
                        .types
                        .intern(Type::Infer(ctx.inference.new_type_variable(expr_id))),
                };

                debug!("Seq(aggr_expr_id = {aggr_expr_id:?}) body_id={aggr_body_idx:?}");

                // This expression id is used as an inference variable
                let expr_root = ExprRoot {
                    id: expr_id,
                    expr: *inner,
                    expected_ty: Some(elem_ty),
                };

                {
                    let arm = ctx.arm;
                    let expr_body = ctx.expr_body_mut(aggr_body_idx);

                    match arm {
                        Arm::First => {
                            expr_body.first = Some(expr_root);
                        }
                        Arm::Second => {
                            expr_body.second = Some(expr_root);
                        }
                    }
                }

                let aggr_variable = ctx
                    .aggr_variables
                    .get(&aggr_body_idx)
                    .expect("No aggregation variable");

                let array_ty = self.types.intern(Type::Array(elem_ty));

                let aggr_node_id = ctx.nodes.add(HirNode {
                    ty: array_ty,
                    kind: HirKind::Aggr(*aggr_variable, aggr_body_idx),
                    span: expr.span,
                });
                (array_ty, aggr_node_id)
            }
            (ExprKind::Constant(k), Some(expected_ty)) => {
                if matches!(expected_ty, Type::Int(_)) {
                    (
                        expected_ty,
                        ctx.nodes.add(HirNode {
                            ty: expected_ty,
                            kind: HirKind::Constant(k),
                            span: expr.span,
                        }),
                    )
                } else {
                    self.expr_error(
                        CompileError::TODO(smart_format!("Expected integer type")),
                        &expr.span,
                    )
                }
            }
            (ExprKind::Variable(expr_id), expected_ty) => {
                let type_var = ctx.inference.new_type_variable(expr_id);
                let bound_variable = ctx
                    .bound_variables
                    .get(&expr_id)
                    .expect("variable not found");

                if let Some(expected_ty) = expected_ty {
                    let variable_ref = ctx.nodes.add(HirNode {
                        ty: expected_ty,
                        kind: HirKind::VariableRef(bound_variable.node_id),
                        span: expr.span,
                    });

                    match ctx
                        .inference
                        .eq_relations
                        .unify_var_value(type_var, UnifyValue::Known(expected_ty))
                    {
                        // Variables are the same type, no mapping necessary:
                        Ok(_) => (expected_ty, variable_ref),
                        // Need to map:
                        Err(TypeError::Mismatch(type_eq)) => (
                            expected_ty,
                            ctx.nodes.add(HirNode {
                                ty: expected_ty,
                                kind: HirKind::MapCall(variable_ref, type_eq.actual),
                                span: expr.span,
                            }),
                        ),
                        Err(err) => todo!("Report unification error: {err:?}"),
                    }
                } else {
                    todo!("Variable without expected type")
                }
            }
            (kind, ty) => self.expr_error(
                CompileError::TODO(smart_format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                )),
                &expr.span,
            ),
        }
    }

    fn check_struct(
        &mut self,
        type_path: TypePath,
        attributes: Box<[((DefReference, SourceSpan), Expr)]>,
        span: SourceSpan,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, HirIdx) {
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

                        for ((def, prop_span), expr) in attributes.into_vec().into_iter() {
                            let attr_prop = match self.defs.get_def_kind(def.def_id) {
                                Some(DefKind::StringLiteral(lit)) => lit,
                                _ => {
                                    self.error(CompileError::NamedPropertyExpected, &prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop) {
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
                                self.error(CompileError::MissingProperty(prop_name.into()), &span);
                            }
                        }

                        ctx.nodes.add(HirNode {
                            ty: domain_type,
                            kind: HirKind::StructPattern(typed_properties),
                            span,
                        })
                    }
                    None => {
                        if !attributes.is_empty() {
                            return self.expr_error(CompileError::NoPropertiesExpected, &span);
                        }
                        ctx.nodes.add(HirNode {
                            kind: HirKind::Unit,
                            ty: domain_type,
                            span,
                        })
                    }
                }
            }
            Some(Constructor::Value(relationship_id, _, _)) => {
                let mut attributes = attributes.into_vec().into_iter();
                match attributes.next() {
                    Some(((def, _), value)) if def.def_id == DefId::unit() => {
                        let (relationship, _) = self
                            .get_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting anonymous property meta");

                        let object_ty = self.check_def(relationship.object.0.def_id);
                        let node_id = self.check_expr_expect(value, object_ty, ctx).1;

                        ctx.nodes.add(HirNode {
                            ty: domain_type,
                            kind: HirKind::ValuePattern(node_id),
                            span,
                        })
                    }
                    _ => return self.expr_error(CompileError::AnonymousPropertyExpected, &span),
                }
            }
            Some(Constructor::Intersection(_)) => {
                todo!()
            }
            Some(Constructor::Union(_property_set)) => {
                return self.expr_error(CompileError::CannotMapUnion, &span)
            }
            Some(Constructor::Sequence(_)) => todo!(),
            Some(Constructor::StringFmt(_)) => todo!(),
        };

        (domain_type, node_id)
    }

    fn check_expr_expect(
        &mut self,
        expr: Expr,
        expected_ty: TypeRef<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, HirIdx) {
        let span = expr.span;
        let (ty, node_idx) = self.check_expr(expr, Some(expected_ty), ctx);
        let inferred_ty = InferBestEffort {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
        }
        .infer_rec(ty, expected_ty)
        .unwrap();

        write_back_types(node_idx, inferred_ty, ctx);

        debug!(
            "Infer best effort: {ty:?} => {inferred_ty:?} node_id: {node_idx:?} node: {:?}",
            ctx.nodes[node_idx]
        );

        let type_eq = TypeEquation {
            actual: inferred_ty,
            expected: expected_ty,
        };
        let mut infer_expect = InferExpect {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            type_eq,
        };

        match infer_expect.infer_rec(inferred_ty, expected_ty) {
            Ok(ty) => (ty, node_idx),
            Err(type_error) => self
                .map_if_possible(ctx, node_idx, type_eq, &span)
                .unwrap_or_else(|_| (self.type_error(type_error, &span), ERROR_NODE)),
        }
    }

    fn map_if_possible(
        &mut self,
        ctx: &mut CheckExprContext<'m>,
        node_idx: HirIdx,
        type_eq: TypeEquation<'m>,
        span: &SourceSpan,
    ) -> Result<(TypeRef<'m>, HirIdx), ()> {
        match (type_eq.actual, type_eq.expected) {
            (Type::Domain(_), Type::Domain(_)) => {
                let map_node = ctx.nodes.add(HirNode {
                    ty: type_eq.expected,
                    kind: HirKind::MapCall(node_idx, type_eq.actual),
                    span: *span,
                });
                Ok((type_eq.expected, map_node))
            }
            (Type::Array(_), Type::Array(_)) => Ok((type_eq.actual, node_idx)),
            _ => Err(()),
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, HirIdx) {
        self.errors.push(error.spanned(span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

fn write_back_types<'m>(node_idx: HirIdx, ty: TypeRef<'m>, ctx: &mut CheckExprContext<'m>) {
    ctx.nodes[node_idx].ty = ty;
    /*
    match (&ctx.nodes[node_idx].kind, ty) {
        (HirKind::Aggr(_, body_idx), Type::Array(elem_ty)) => {
            let body = &ctx.bodies[body_idx.0 as usize];
            let elem_index = match ctx.arm {
                Arm::First => body.first,
                Arm::Second => body.second,
            };

            write_back_types(elem_index, elem_ty, ctx)
        }
        _ => {}
    }
    */
}

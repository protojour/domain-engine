use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, value::PropertyId, DefId, RelationId, Role};
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr, TypePath},
    hir_node::{
        BindDepth, HirBodyIdx, HirIdx, HirKind, HirNode, HirNodeTable, HirVariable, ERROR_NODE,
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
    pub bodies: Vec<CtrlFlowBody<'m>>,
    pub nodes: HirNodeTable<'m>,
    pub bound_variables: FnvHashMap<ExprId, BoundVariable>,
    pub body_variables: FnvHashMap<HirBodyIdx, HirIdx>,
    pub body_map: FnvHashMap<ExprId, HirBodyIdx>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub partial: bool,

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
            body_variables: Default::default(),
            body_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            partial: false,
            arm: Arm::First,
            bind_depth: BindDepth(0),
            hir_var_allocations: vec![0],
            body_id_counter: 0,
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_ctrl<T>(&mut self, f: impl FnOnce(&mut Self, HirVariable) -> T) -> T {
        // There is a unique bind depth for the control flow variable:
        let ctrl_flow_var = self.alloc_hir_variable();

        self.bind_depth.0 += 1;
        let ret = f(self, ctrl_flow_var);
        self.bind_depth.0 -= 1;

        ret
    }

    pub fn alloc_hir_body_idx(&mut self) -> HirBodyIdx {
        let next = self.body_id_counter;
        self.body_id_counter += 1;
        self.bodies.push(CtrlFlowBody::default());
        HirBodyIdx(next)
    }

    pub fn expr_body_mut(&mut self, id: HirBodyIdx) -> &mut CtrlFlowBody<'m> {
        self.bodies.get_mut(id.0 as usize).unwrap()
    }

    pub fn alloc_hir_variable(&mut self) -> HirVariable {
        let alloc = self.hir_var_allocations.get_mut(0).unwrap();
        let hir_var = HirVariable(*alloc);
        *alloc += 1;
        hir_var
    }
}

#[derive(Default)]
pub struct CtrlFlowBody<'m> {
    pub first: Option<ExprRoot<'m>>,
    pub second: Option<ExprRoot<'m>>,
}

pub struct ExprRoot<'m> {
    pub id: ExprId,
    pub expr: Expr,
    pub expected_ty: Option<TypeRef<'m>>,
}

pub struct BoundVariable {
    pub syntax_var: HirVariable,
    pub node_id: HirIdx,
    pub ctrl_group: Option<CtrlFlowGroup>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CtrlFlowGroup {
    pub body_id: HirBodyIdx,
    pub bind_depth: BindDepth,
}

/// Tracks which control flow "statements" are children of other control flow "statements"
#[derive(Default)]
pub struct CtrlFlowForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<HirBodyIdx, HirBodyIdx>,
}

impl CtrlFlowForest {
    pub fn insert(&mut self, idx: HirBodyIdx, parent: Option<HirBodyIdx>) {
        self.map.insert(idx, parent.unwrap_or(idx));
    }

    pub fn find_parent(&self, idx: HirBodyIdx) -> Option<HirBodyIdx> {
        let parent = self.map.get(&idx).unwrap();
        if parent == &idx {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut idx: HirBodyIdx) -> HirBodyIdx {
        loop {
            let parent = self.map.get(&idx).unwrap();
            if parent == &idx {
                return idx;
            }
            idx = *parent;
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
        let (ty, hir_idx) = self.check_expr(
            root.expr,
            // Don't pass inference types as the expected type:
            match root.expected_ty {
                Some(Type::Infer(_)) => None,
                Some(other) => Some(other),
                None => None,
            },
            ctx,
        );

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
            (ExprKind::Struct(type_path, attributes), expected_ty) => {
                let (ty, hir_idx) = self.check_struct(type_path, attributes, expr.span, ctx);
                match expected_ty {
                    Some(Type::Domain(_)) => (ty, hir_idx),
                    Some(Type::Option(Type::Domain(_))) => {
                        (self.types.intern(Type::Option(ty)), hir_idx)
                    }
                    Some(expected_ty) => (
                        self.type_error(
                            TypeError::Mismatch(TypeEquation {
                                actual: ty,
                                expected: expected_ty,
                            }),
                            &expr.span,
                        ),
                        hir_idx,
                    ),
                    _ => (ty, hir_idx),
                }
            }
            (ExprKind::Seq(aggr_expr_id, inner), expected_ty) => {
                // The variables outside the aggregation refer to the aggregated object (an array).
                let aggr_body_idx = *ctx.body_map.get(&aggr_expr_id).unwrap();

                let expr_id = self.expressions.alloc_expr_id();

                let elem_ty = match expected_ty {
                    Some(Type::Array(elem_ty)) => elem_ty,
                    Some(other_ty) => {
                        self.type_error(TypeError::MustBeSequence(other_ty), &expr.span);
                        self.types.intern(Type::Error)
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
                    .body_variables
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

                match expected_ty {
                    Some(Type::Array(elem_ty)) => (
                        self.type_error(
                            TypeError::VariableMustBeSequenceEnclosed(elem_ty),
                            &expr.span,
                        ),
                        ERROR_NODE,
                    ),
                    Some(expected_ty) => {
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
                            Err(err @ TypeError::Mismatch(type_eq)) => {
                                match (&type_eq.actual, &type_eq.expected) {
                                    (Type::Domain(_), Type::Domain(_)) => (
                                        expected_ty,
                                        ctx.nodes.add(HirNode {
                                            ty: expected_ty,
                                            kind: HirKind::MapCall(variable_ref, type_eq.actual),
                                            span: expr.span,
                                        }),
                                    ),
                                    _ => (self.type_error(err, &expr.span), ERROR_NODE),
                                }
                            }
                            Err(err) => todo!("Report unification error: {err:?}"),
                        }
                    }
                    None => {
                        todo!()
                    }
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
        attributes: Box<[ExprStructAttr]>,
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
                                    let meta = self
                                        .property_meta_by_subject(
                                            *subject_id,
                                            property_id.relation_id,
                                        )
                                        .expect("BUG: problem getting property meta");
                                    let property_name = meta
                                        .relation
                                        .subject_prop(self.defs)
                                        .expect("BUG: Expected named subject property");

                                    Some((
                                        property_name,
                                        MatchProperty {
                                            relation_id: property_id.relation_id,
                                            cardinality: meta.relationship.subject_cardinality,
                                            object_def: meta.relationship.object.0.def_id,
                                            used: false,
                                        },
                                    ))
                                }
                                Role::Object => None,
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut typed_properties = IndexMap::new();

                        for ExprStructAttr {
                            key: (def, prop_span),
                            bind_option,
                            expr,
                        } in attributes.into_vec().into_iter()
                        {
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
                            debug!("object_ty: {object_ty:?}");

                            let object_ty = match match_property.cardinality.1 {
                                ValueCardinality::One => object_ty,
                                ValueCardinality::Many => self.types.intern(Type::Array(object_ty)),
                            };
                            let (_, node_idx) = match match_property.cardinality.0 {
                                PropertyCardinality::Mandatory => {
                                    self.check_expr_expect(expr, object_ty, ctx)
                                }
                                PropertyCardinality::Optional => {
                                    let object_ty = self.types.intern(Type::Option(object_ty));

                                    if bind_option {
                                    } else {
                                        ctx.partial = true;
                                        panic!("partial unification");
                                    }

                                    self.check_expr_expect(expr, object_ty, ctx)
                                }
                            };

                            typed_properties
                                .insert(PropertyId::subject(match_property.relation_id), node_idx);
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
                    Some(ExprStructAttr {
                        key: (def, _),
                        bind_option: _,
                        expr: value,
                    }) if def.def_id == DefId::unit() => {
                        let meta = self
                            .get_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting anonymous property meta");

                        let object_ty = self.check_def(meta.relationship.object.0.def_id);
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
        self.check_expr(expr, Some(expected_ty), ctx)
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, HirIdx) {
        self.errors.push(error.spanned(span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}

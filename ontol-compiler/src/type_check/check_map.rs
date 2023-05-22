use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_runtime::smart_format;
use ontos::visitor::OntosVisitor;
use tracing::debug;

use crate::{
    codegen::task::{CodegenTask, MapCodegenTask, OntosMapCodegenTask},
    def::{Def, Variables},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, Expressions},
    hir_node::{BindDepth, HirBody, HirBodyIdx, HirKind, HirNode, ERROR_NODE},
    mem::Intern,
    type_check::unify_ctx::Arm,
    typed_ontos::lang::{OntosNode, TypedOntos},
    types::{Type, TypeRef, Types},
    CompileErrors, SourceSpan,
};

use super::{
    inference::{Infer, TypeVar},
    unify_ctx::{CheckUnifyExprContext, CtrlFlowGroup, ExplicitVariable},
    TypeCheck, TypeError,
};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_map(
        &mut self,
        def: &Def,
        variables: &Variables,
        first_id: ExprId,
        second_id: ExprId,
    ) -> Result<TypeRef<'m>, AggrGroupError> {
        let mut ctx = CheckUnifyExprContext::new();
        let root_body_idx = ctx.alloc_hir_body_idx();

        {
            let mut map_check = MapCheck {
                types: self.types,
                errors: self.errors,
                expressions: self.expressions,
            };
            debug!("FIRST ARM START");
            ctx.arm = Arm::First;
            let _ = map_check.analyze_arm(
                map_check.expressions.map.get(&first_id).unwrap(),
                variables,
                None,
                &mut ctx,
            )?;
            debug!("SECOND ARM START");
            ctx.arm = Arm::Second;
            let _ = map_check.analyze_arm(
                map_check.expressions.map.get(&second_id).unwrap(),
                variables,
                None,
                &mut ctx,
            )?;
        }

        // experimental:
        self.check_arms_v2(def, first_id, second_id, &mut ctx)?;

        self.check_arms_v1(def, first_id, second_id, root_body_idx, &mut ctx)?;

        Ok(self.types.intern(Type::Tautology))
    }

    fn check_arms_v1(
        &mut self,
        def: &Def,
        first_id: ExprId,
        second_id: ExprId,
        root_body_idx: HirBodyIdx,
        ctx: &mut CheckUnifyExprContext<'m>,
    ) -> Result<(), AggrGroupError> {
        let mut root_body = ctx.expr_body_mut(root_body_idx);
        root_body.first = Some(self.consume_expr(first_id));
        root_body.second = Some(self.consume_expr(second_id));

        let bodies_len = ctx.bodies.len() as u32;
        let mut bodies = vec![];

        // FIXME: This algorithm is no good..
        // There is no guarantee that the bodies will be traversed in the correct order,
        // they get "initialized" inside check_expr.
        // Instead, check_expr has to notify which bodies are "done" and can be processed next.
        for body_index in 0..bodies_len {
            debug!("Check body {body_index}");

            let expr_body = ctx.expr_body_mut(HirBodyIdx(body_index));
            let first_root = expr_body.first.take();
            let second_root = expr_body.second.take();

            // If these are not Some, an error should have been raised
            if let (Some(first_root), Some(second_root)) = (first_root, second_root) {
                ctx.arm = Arm::First;
                let (_first_ty, first) = self.check_expr_root(first_root, ctx);
                ctx.arm = Arm::Second;
                let (_second_ty, second) = self.check_expr_root(second_root, ctx);

                // pad the vector
                bodies.resize_with(body_index as usize + 1, || HirBody {
                    first: ERROR_NODE,
                    second: ERROR_NODE,
                });

                bodies[body_index as usize] = HirBody { first, second };
            }
        }

        // Type inference:
        for node in &mut ctx.nodes.0 {
            let mut infer = Infer {
                types: self.types,
                eq_relations: &mut ctx.inference.eq_relations,
            };

            match infer.infer_recursive(node.ty) {
                Ok(ty) => node.ty = ty,
                Err(TypeError::Propagated) => {}
                Err(TypeError::NotEnoughInformation) => {
                    self.error(
                        CompileError::TODO(smart_format!("Not enough type information")),
                        &node.span,
                    );
                }
                _ => panic!("Unexpected inference error"),
            }
        }

        self.codegen_tasks.push(CodegenTask::Map(MapCodegenTask {
            nodes: std::mem::take(&mut ctx.nodes),
            bodies,
            span: def.span,
        }));

        Ok(())
    }

    fn check_arms_v2(
        &mut self,
        def: &Def,
        first_id: ExprId,
        second_id: ExprId,
        ctx: &mut CheckUnifyExprContext<'m>,
    ) -> Result<(), AggrGroupError> {
        let mut first = self.check_root_expr2(first_id, ctx);
        self.infer_ontos_types(&mut first, ctx);

        let mut second = self.check_root_expr2(second_id, ctx);
        self.infer_ontos_types(&mut second, ctx);

        self.codegen_tasks
            .push(CodegenTask::OntosMap(OntosMapCodegenTask {
                first,
                second,
                span: def.span,
            }));

        Ok(())
    }

    fn infer_ontos_types(&mut self, node: &mut OntosNode<'m>, ctx: &mut CheckUnifyExprContext<'m>) {
        let mut inference = OntosTypeInference {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        inference.visit_node(0, node);
    }
}

struct OntosTypeInference<'c, 'm> {
    types: &'c mut Types<'m>,
    eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
    errors: &'c mut CompileErrors,
}

impl<'c, 'm> OntosVisitor<'m, TypedOntos> for OntosTypeInference<'c, 'm> {
    fn visit_node(&mut self, index: usize, node: &mut <TypedOntos as ontos::Lang>::Node<'m>) {
        let mut infer = Infer {
            types: self.types,
            eq_relations: self.eq_relations,
        };
        match infer.infer_recursive(node.meta.ty) {
            Ok(ty) => node.meta.ty = ty,
            Err(TypeError::Propagated) => {}
            Err(TypeError::NotEnoughInformation) => {
                self.errors.push(
                    CompileError::TODO(smart_format!("Not enough type information"))
                        .spanned(&node.meta.span),
                );
            }
            _ => panic!("Unexpected inference error"),
        }

        self.visit_kind(index, &mut node.kind);
    }
}

pub struct MapCheck<'c, 'm> {
    types: &'c mut Types<'m>,
    errors: &'c mut CompileErrors,
    expressions: &'c Expressions,
}

impl<'c, 'm> MapCheck<'c, 'm> {
    fn analyze_arm(
        &mut self,
        expr: &Expr,
        variables: &Variables,
        parent_aggr_group: Option<CtrlFlowGroup>,
        ctx: &mut CheckUnifyExprContext<'m>,
    ) -> Result<AggrGroupSet, AggrGroupError> {
        let mut group_set = AggrGroupSet::new();

        match &expr.kind {
            ExprKind::Call(_, args) => {
                for arg in args.iter() {
                    group_set.join(self.analyze_arm(arg, variables, parent_aggr_group, ctx)?);
                }
            }
            ExprKind::Struct(_, attributes) => {
                for attr in attributes.iter() {
                    group_set.join(self.analyze_arm(
                        &attr.expr,
                        variables,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            ExprKind::Seq(expr_id, inner) => {
                if ctx.arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register aggregation body
                    let aggr_body_idx = ctx.alloc_hir_body_idx();
                    debug!("first arm seq: expr_id={expr_id:?}");
                    ctx.body_map.insert(*expr_id, aggr_body_idx);
                    ctx.ctrl_flow_forest.insert(
                        aggr_body_idx,
                        parent_aggr_group.map(|parent| parent.body_id),
                    );

                    // Register aggregation variable
                    let aggr_var = ctx.alloc_ontos_variable();
                    let aggr_var_idx = ctx.nodes.add(HirNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: HirKind::Variable(aggr_var),
                        span: expr.span,
                    });
                    ctx.body_variables.insert(aggr_body_idx, aggr_var_idx);

                    let result =
                        ctx.enter_ctrl::<Result<AggrGroupSet, AggrGroupError>>(|ctx, _| {
                            self.analyze_arm(
                                inner,
                                variables,
                                Some(CtrlFlowGroup {
                                    body_id: aggr_body_idx,
                                    bind_depth: ctx.current_bind_depth(),
                                }),
                                ctx,
                            )
                        });

                    assert!(result.is_ok());
                } else {
                    let outer_bind_depth = ctx.current_bind_depth();

                    ctx.enter_ctrl::<Result<(), AggrGroupError>>(|ctx, _| {
                        let inner_aggr_group =
                            self.analyze_arm(inner, variables, None, ctx).unwrap();

                        match inner_aggr_group.disambiguate(ctx, ctx.current_bind_depth()) {
                            Ok(aggr_body_id) => {
                                ctx.body_map.insert(*expr_id, aggr_body_id);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(aggr_body_id).map(
                                    |body_id| CtrlFlowGroup {
                                        body_id,
                                        bind_depth: outer_bind_depth,
                                    },
                                ));
                                Ok(())
                            }
                            Err(error) => {
                                debug!("Failure: {error:?}");

                                self.error(
                                    CompileError::TODO(smart_format!(
                                        "Incompatible aggregation group"
                                    )),
                                    &expr.span,
                                );
                                Err(error)
                            }
                        }
                    })?;
                }
            }
            ExprKind::Variable(expr_id) => {
                if let Some(explicit_variable) = ctx.explicit_variables.get(expr_id) {
                    // Variable is used more than once
                    if ctx.arm.is_first() && explicit_variable.ctrl_group != parent_aggr_group {
                        self.error(
                            CompileError::TODO(smart_format!("Incompatible aggregation group")),
                            &expr.span,
                        );
                    }

                    debug!("Join existing bound variable");

                    group_set.add(explicit_variable.ctrl_group);
                } else {
                    let (variable_expr_id, variable_span) = variables
                        .0
                        .iter()
                        .find(|(var_expr_id, _)| var_expr_id == expr_id)
                        .unwrap();

                    // Register variable
                    let syntax_var = ctx.alloc_ontos_variable();
                    let var_id = ctx.nodes.add(HirNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: HirKind::Variable(syntax_var),
                        span: *variable_span,
                    });

                    if ctx.arm.is_first() {
                        ctx.explicit_variables.insert(
                            *variable_expr_id,
                            ExplicitVariable {
                                variable: syntax_var,
                                node_id: var_id,
                                ctrl_group: parent_aggr_group,
                            },
                        );

                        group_set.add(parent_aggr_group);
                    } else {
                        match ctx.explicit_variables.entry(*variable_expr_id) {
                            Entry::Occupied(_occ) => {
                                todo!();
                            }
                            Entry::Vacant(vac) => {
                                vac.insert(ExplicitVariable {
                                    variable: syntax_var,
                                    node_id: var_id,
                                    ctrl_group: parent_aggr_group,
                                });
                                group_set.add(parent_aggr_group);
                            }
                        }
                    }
                }
            }
            ExprKind::Constant(_) => {}
        };

        Ok(group_set)
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(span));
        self.types.intern(Type::Error)
    }
}

#[derive(Clone, Copy)]
struct FirstArm(bool);

struct AggrGroupSet {
    set: FnvHashSet<Option<HirBodyIdx>>,
    tallest_depth: u16,
}

#[derive(Debug)]
pub enum AggrGroupError {
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<HirBodyIdx>),
}

impl AggrGroupSet {
    fn new() -> Self {
        Self {
            set: Default::default(),
            tallest_depth: 0,
        }
    }

    fn join(&mut self, other: AggrGroupSet) {
        self.set.extend(other.set.into_iter());
        self.tallest_depth = core::cmp::max(self.tallest_depth, other.tallest_depth)
    }

    fn add(&mut self, aggr_group: Option<CtrlFlowGroup>) {
        self.set.insert(aggr_group.map(|group| group.body_id));
        if let Some(group) = aggr_group {
            self.tallest_depth = core::cmp::max(self.tallest_depth, group.bind_depth.0)
        }
    }

    // Find a unique leaf in the aggregation forest
    fn disambiguate(
        self,
        ctx: &CheckUnifyExprContext,
        max_depth: BindDepth,
    ) -> Result<HirBodyIdx, AggrGroupError> {
        if self.tallest_depth > max_depth.0 {
            return Err(AggrGroupError::DepthExceeded);
        }

        let mut roots: FnvHashSet<HirBodyIdx> = Default::default();
        let mut parents: FnvHashSet<HirBodyIdx> = Default::default();

        for group in self.set.iter().flatten() {
            roots.insert(ctx.ctrl_flow_forest.find_root(*group));
            if let Some(parent) = ctx.ctrl_flow_forest.find_parent(*group) {
                parents.insert(parent);
            }
        }

        debug!("self.set {:?}", self.set);
        debug!("parents: {parents:?}");

        match roots.len() {
            1 => {
                let leaves = self
                    .set
                    .iter()
                    .filter_map(|opt| *opt)
                    .filter(|node_id| !parents.contains(node_id))
                    .collect::<Vec<_>>();

                match leaves.len() {
                    0 => Err(AggrGroupError::NoLeaves),
                    1 => Ok(leaves.into_iter().next().unwrap()),
                    _ => Err(AggrGroupError::TooManyLeaves(leaves)),
                }
            }
            other => Err(AggrGroupError::RootCount(other)),
        }
    }
}

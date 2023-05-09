use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_runtime::smart_format;
use tracing::debug;

use crate::{
    codegen::{CodegenTask, MapCodegenTask},
    def::{Def, Variables},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, Expressions},
    hir_node::{BindDepth, HirBody, HirBodyIdx, HirKind, HirNode, ERROR_NODE},
    mem::Intern,
    type_check::check_expr::Arm,
    types::{Type, TypeRef, Types},
    CompileErrors, SourceSpan,
};

use super::{
    check_expr::{BoundVariable, CheckExprContext, CtrlFlowGroup},
    inference::Infer,
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
        let mut ctx = CheckExprContext::new();
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
                let (_first_ty, first) = self.check_expr_root(first_root, &mut ctx);
                ctx.arm = Arm::Second;
                let (_second_ty, second) = self.check_expr_root(second_root, &mut ctx);

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
            nodes: ctx.nodes,
            bodies,
            span: def.span,
        }));

        Ok(self.types.intern(Type::Tautology))
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
        ctx: &mut CheckExprContext<'m>,
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
                    let aggr_var = ctx.alloc_hir_variable();
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
                if let Some(bound_variable) = ctx.bound_variables.get(expr_id) {
                    // Variable is used more than once
                    if ctx.arm.is_first() && bound_variable.ctrl_group != parent_aggr_group {
                        self.error(
                            CompileError::TODO(smart_format!("Incompatible aggregation group")),
                            &expr.span,
                        );
                    }

                    debug!("Join existing bound variable");

                    group_set.add(bound_variable.ctrl_group);
                } else {
                    let (variable_expr_id, variable_span) = variables
                        .0
                        .iter()
                        .find(|(var_expr_id, _)| var_expr_id == expr_id)
                        .unwrap();

                    // Register variable
                    let syntax_var = ctx.alloc_hir_variable();
                    let var_id = ctx.nodes.add(HirNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: HirKind::Variable(syntax_var),
                        span: *variable_span,
                    });

                    if ctx.arm.is_first() {
                        ctx.bound_variables.insert(
                            *variable_expr_id,
                            BoundVariable {
                                syntax_var,
                                node_id: var_id,
                                ctrl_group: parent_aggr_group,
                            },
                        );

                        group_set.add(parent_aggr_group);
                    } else {
                        match ctx.bound_variables.entry(*variable_expr_id) {
                            Entry::Occupied(_occ) => {
                                todo!();
                            }
                            Entry::Vacant(vac) => {
                                vac.insert(BoundVariable {
                                    syntax_var,
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
        ctx: &CheckExprContext,
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

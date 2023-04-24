use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_runtime::smart_format;
use tracing::debug;

use crate::{
    codegen::{CodegenTask, MapCodegenTask},
    def::{Def, Variables},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind},
    ir_node::{BindDepth, IrKind, IrNode, IrNodeId},
    mem::Intern,
    type_check::check_expr::Arm,
    types::{Type, TypeRef},
};

use super::{
    check_expr::{AggregationGroup, BoundVariable, CheckExprContext},
    TypeCheck,
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

        debug!("FIRST ARM START");
        ctx.arm = Arm::First;
        let _ = self.aggr_group_map_variables(
            self.expressions.get(&first_id).unwrap(),
            variables,
            None,
            &mut ctx,
        )?;
        debug!("SECOND ARM START");
        ctx.arm = Arm::Second;
        let _ = self.aggr_group_map_variables(
            self.expressions.get(&second_id).unwrap(),
            variables,
            None,
            &mut ctx,
        )?;

        debug!("ctx aggregation variables {:?}", ctx.aggr_map);

        ctx.arm = Arm::First;
        let (_, node_a) = self.check_expr_id(first_id, &mut ctx);
        ctx.arm = Arm::Second;
        let (_, node_b) = self.check_expr_id(second_id, &mut ctx);

        self.codegen_tasks.push(CodegenTask::Map(MapCodegenTask {
            expressions: ctx.nodes,
            node_a,
            node_b,
            span: def.span,
        }));

        Ok(self.types.intern(Type::Tautology))
    }

    fn aggr_group_map_variables(
        &mut self,
        expr: &Expr,
        variables: &Variables,
        parent_aggr_group: Option<AggregationGroup>,
        ctx: &mut CheckExprContext<'m>,
    ) -> Result<AggrGroupSet, AggrGroupError> {
        let mut group_set = AggrGroupSet::new();

        match &expr.kind {
            ExprKind::Call(_, args) => {
                for arg in args.iter() {
                    group_set.join(self.aggr_group_map_variables(
                        arg,
                        variables,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            ExprKind::Struct(_, attributes) => {
                for (_, attr) in attributes.iter() {
                    group_set.join(self.aggr_group_map_variables(
                        attr,
                        variables,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            ExprKind::Seq(expr_id, inner) => {
                if ctx.arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register variable
                    let aggr_syntax_var = ctx.alloc_syntax_var();
                    let aggr_var_id = ctx.nodes.add(IrNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: IrKind::Variable(aggr_syntax_var),
                        span: expr.span,
                    });
                    debug!("first arm seq: expr_id={expr_id:?}");
                    ctx.aggr_map.insert(*expr_id, aggr_var_id);
                    ctx.aggr_forest
                        .insert(aggr_var_id, parent_aggr_group.map(|parent| parent.node_id));

                    let result =
                        ctx.enter_aggregation::<Result<AggrGroupSet, AggrGroupError>>(|ctx, _| {
                            self.aggr_group_map_variables(
                                inner,
                                variables,
                                Some(AggregationGroup {
                                    node_id: aggr_var_id,
                                    bind_depth: ctx.current_bind_depth(),
                                }),
                                ctx,
                            )
                        });

                    assert!(result.is_ok());
                } else {
                    let outer_bind_depth = ctx.current_bind_depth();

                    ctx.enter_aggregation::<Result<(), AggrGroupError>>(|ctx, _| {
                        let inner_aggr_group = self
                            .aggr_group_map_variables(inner, variables, None, ctx)
                            .unwrap();

                        match inner_aggr_group.disambiguate(ctx, ctx.current_bind_depth()) {
                            Ok(aggr_var_id) => {
                                ctx.aggr_map.insert(*expr_id, aggr_var_id);

                                group_set.add(ctx.aggr_forest.find_parent(aggr_var_id).map(
                                    |node_id| AggregationGroup {
                                        node_id,
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
                if let Some(bound_variable) = ctx.bound_variables.get(&expr_id) {
                    // Variable is used more than once
                    if ctx.arm.is_first() {
                        if bound_variable.aggr_group != parent_aggr_group {
                            self.error(
                                CompileError::TODO(smart_format!("Incompatible aggregation group")),
                                &expr.span,
                            );
                        }
                    }

                    debug!("Join existing bound variable");

                    group_set.add(bound_variable.aggr_group);
                } else {
                    let (variable_expr_id, variable_span) = variables
                        .0
                        .iter()
                        .find(|(var_expr_id, _)| var_expr_id == expr_id)
                        .unwrap();

                    // Register variable
                    let syntax_var = ctx.alloc_syntax_var();
                    let var_id = ctx.nodes.add(IrNode {
                        ty: self.types.intern(Type::Tautology),
                        kind: IrKind::Variable(syntax_var),
                        span: *variable_span,
                    });

                    if ctx.arm.is_first() {
                        ctx.bound_variables.insert(
                            *variable_expr_id,
                            BoundVariable {
                                syntax_var,
                                node_id: var_id,
                                aggr_group: parent_aggr_group,
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
                                    aggr_group: parent_aggr_group,
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
}

#[derive(Clone, Copy)]
struct FirstArm(bool);

struct AggrGroupSet {
    set: FnvHashSet<Option<IrNodeId>>,
    tallest_depth: u16,
}

#[derive(Debug)]
pub enum AggrGroupError {
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<IrNodeId>),
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

    fn add(&mut self, aggr_group: Option<AggregationGroup>) {
        self.set.insert(aggr_group.map(|group| group.node_id));
        if let Some(group) = aggr_group {
            self.tallest_depth = core::cmp::max(self.tallest_depth, group.bind_depth.0)
        }
    }

    // Find a unique leaf in the aggregation forest
    fn disambiguate(
        self,
        ctx: &CheckExprContext,
        max_depth: BindDepth,
    ) -> Result<IrNodeId, AggrGroupError> {
        if self.tallest_depth > max_depth.0 {
            return Err(AggrGroupError::DepthExceeded);
        }

        let mut roots: FnvHashSet<IrNodeId> = Default::default();
        let mut parents: FnvHashSet<IrNodeId> = Default::default();

        for group in &self.set {
            if let Some(aggr_group) = group {
                roots.insert(ctx.aggr_forest.find_root(*aggr_group));
                if let Some(parent) = ctx.aggr_forest.find_parent(*aggr_group) {
                    parents.insert(parent);
                }
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

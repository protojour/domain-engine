use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_hir::{visitor::HirMutVisitor, Label};
use ontol_runtime::smart_format;
use tracing::debug;

use crate::{
    codegen::{
        task::{ExplicitMapCodegenTask, MapCodegenTask, MapKeyPair},
        type_mapper::TypeMapper,
    },
    def::{Def, MapDirection, Variables},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, Expressions},
    mem::Intern,
    type_check::hir_build_ctx::{Arm, VariableMapping},
    typed_hir::TypedHirNode,
    types::{Type, TypeRef, Types},
    CompileErrors, Note, SourceSpan, SpannedNote,
};

use super::{
    hir_build_ctx::{CtrlFlowDepth, CtrlFlowGroup, ExpressionVariable, HirBuildCtx},
    hir_type_inference::{HirArmTypeInference, HirVariableMapper},
    TypeCheck, TypeEquation, TypeError,
};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_map(
        &mut self,
        def: &Def,
        direction: MapDirection,
        variables: &Variables,
        first_id: ExprId,
        second_id: ExprId,
    ) -> Result<TypeRef<'m>, AggrGroupError> {
        let mut ctx = HirBuildCtx::new(def.span, direction);

        {
            let mut map_check = MapCheck {
                types: self.types,
                errors: self.errors,
                expressions: self.expressions,
            };
            debug!("FIRST ARM START");
            ctx.arm = Arm::First;
            let _ = map_check.analyze_arm(
                map_check.expressions.table.get(&first_id).unwrap(),
                variables,
                None,
                &mut ctx,
            )?;
            debug!("SECOND ARM START");
            ctx.arm = Arm::Second;
            let _ = map_check.analyze_arm(
                map_check.expressions.table.get(&second_id).unwrap(),
                variables,
                None,
                &mut ctx,
            )?;
        }

        self.build_arms(def, first_id, second_id, &mut ctx)?;

        self.report_missing_prop_errors(&mut ctx);

        Ok(self.types.intern(Type::Tautology))
    }

    fn build_arms(
        &mut self,
        def: &Def,
        first_id: ExprId,
        second_id: ExprId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<(), AggrGroupError> {
        ctx.arm = Arm::First;
        let mut first = self.build_root_expr(first_id, ctx);
        self.infer_hir_arm_types(&mut first, ctx);

        ctx.arm = Arm::Second;
        let mut second = self.build_root_expr(second_id, ctx);
        self.infer_hir_arm_types(&mut second, ctx);

        // unify the type of variables on either side:
        self.infer_hir_unify_arms(&mut first, &mut second, ctx);

        if let Some(key_pair) = TypeMapper::new(self.relations, self.defs, self.seal_ctx)
            .find_map_key_pair(first.1.ty, second.1.ty)
        {
            self.codegen_tasks.add_map_task(
                key_pair,
                crate::codegen::task::MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                    direction: ctx.direction,
                    first,
                    second,
                    span: def.span,
                }),
            );
        }

        Ok(())
    }

    fn infer_hir_arm_types(&mut self, node: &mut TypedHirNode<'m>, ctx: &mut HirBuildCtx<'m>) {
        let mut inference = HirArmTypeInference {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        inference.visit_node(0, node);
    }

    fn infer_hir_unify_arms(
        &mut self,
        first: &mut TypedHirNode<'m>,
        second: &mut TypedHirNode<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for explicit_var in &mut ctx.expr_variables.values_mut() {
            let first_arm = explicit_var.hir_arms.get(&Arm::First);
            let second_arm = explicit_var.hir_arms.get(&Arm::Second);

            if let (Some(first_arm), Some(second_arm)) = (first_arm, second_arm) {
                let first_type_var = ctx.inference.new_type_variable(first_arm.expr_id);
                let second_type_var = ctx.inference.new_type_variable(second_arm.expr_id);

                match ctx
                    .inference
                    .eq_relations
                    .unify_var_var(first_type_var, second_type_var)
                {
                    Ok(_) => {}
                    Err(TypeError::Mismatch(TypeEquation { actual, expected })) => {
                        match (actual.get_single_def_id(), expected.get_single_def_id()) {
                            (Some(first_def_id), Some(second_def_id))
                                if actual.is_domain_specific() || expected.is_domain_specific() =>
                            {
                                ctx.variable_mapping.insert(
                                    explicit_var.variable,
                                    VariableMapping {
                                        first_arm_type: actual,
                                        second_arm_type: expected,
                                    },
                                );

                                self.codegen_tasks.add_map_task(
                                    MapKeyPair::new(first_def_id.into(), second_def_id.into()),
                                    MapCodegenTask::Auto,
                                );
                            }
                            _ => {
                                self.type_error(
                                    TypeError::Mismatch(TypeEquation { actual, expected }),
                                    &second_arm.span,
                                );
                            }
                        }
                    }
                    Err(_) => todo!(),
                }
            }
        }

        HirVariableMapper {
            variable_mapping: &ctx.variable_mapping,
            arm: Arm::First,
        }
        .visit_node(0, first);
        HirVariableMapper {
            variable_mapping: &ctx.variable_mapping,
            arm: Arm::Second,
        }
        .visit_node(0, second);
    }

    fn report_missing_prop_errors(&mut self, ctx: &mut HirBuildCtx<'m>) {
        let mut did_report_one_way_note = false;
        let mut errors_in_second = false;

        for arm in ctx.missing_properties.keys() {
            if matches!(arm, Arm::Second) {
                errors_in_second = true;
            }
        }

        for (arm, missing) in std::mem::take(&mut ctx.missing_properties) {
            for (span, properties) in missing {
                let error = CompileError::MissingProperties(properties);

                if matches!(arm, Arm::First)
                    && matches!(ctx.direction, MapDirection::Omni)
                    && !errors_in_second
                    && !did_report_one_way_note
                {
                    self.error_with_notes(
                        error,
                        &span,
                        vec![SpannedNote {
                            note: Note::ConsiderUsingOneWayMap,
                            span: ctx.map_kw_span,
                        }],
                    );
                    did_report_one_way_note = true;
                } else {
                    self.error(error, &span);
                }
            }
        }
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
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<AggrGroupSet, AggrGroupError> {
        let mut group_set = AggrGroupSet::new();

        match &expr.kind {
            ExprKind::Call(_, args) => {
                for arg in args.iter() {
                    group_set.join(self.analyze_arm(arg, variables, parent_aggr_group, ctx)?);
                }
            }
            ExprKind::Struct {
                attributes: attrs, ..
            } => {
                for attr in attrs.iter() {
                    group_set.join(self.analyze_arm(
                        &attr.value,
                        variables,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            ExprKind::Seq(expr_id, object) => {
                if ctx.arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm seq: expr_id={expr_id:?}");
                    ctx.ctrl_flow_forest
                        .insert(label, parent_aggr_group.map(|parent| parent.label));
                    ctx.label_map.insert(*expr_id, label);

                    let result = ctx.enter_ctrl::<Result<AggrGroupSet, AggrGroupError>>(|ctx| {
                        self.analyze_arm(
                            object,
                            variables,
                            Some(CtrlFlowGroup {
                                label,
                                bind_depth: ctx.current_ctrl_flow_depth(),
                            }),
                            ctx,
                        )
                    });

                    assert!(result.is_ok());
                } else {
                    let outer_bind_depth = ctx.current_ctrl_flow_depth();

                    ctx.enter_ctrl::<Result<(), AggrGroupError>>(|ctx| {
                        let inner_aggr_group =
                            self.analyze_arm(object, variables, None, ctx).unwrap();

                        match inner_aggr_group.disambiguate(ctx, ctx.current_ctrl_flow_depth()) {
                            Ok(label) => {
                                ctx.label_map.insert(*expr_id, label);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(label).map(
                                    |label| CtrlFlowGroup {
                                        label,
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
                if let Some(explicit_variable) = ctx.expr_variables.get(expr_id) {
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
                    let variable_expr_id = variables
                        .0
                        .iter()
                        .find(|var_expr_id| *var_expr_id == expr_id)
                        .unwrap();

                    // Register variable
                    let variable = ctx.var_allocator.alloc();

                    if ctx.arm.is_first() {
                        ctx.expr_variables.insert(
                            *variable_expr_id,
                            ExpressionVariable {
                                variable,
                                ctrl_group: parent_aggr_group,
                                hir_arms: Default::default(),
                            },
                        );

                        group_set.add(parent_aggr_group);
                    } else {
                        match ctx.expr_variables.entry(*variable_expr_id) {
                            Entry::Occupied(_occ) => {
                                todo!();
                            }
                            Entry::Vacant(vac) => {
                                vac.insert(ExpressionVariable {
                                    variable,
                                    ctrl_group: parent_aggr_group,
                                    hir_arms: Default::default(),
                                });
                                group_set.add(parent_aggr_group);
                            }
                        }
                    }
                }
            }
            ExprKind::ConstI64(_) | ExprKind::ConstString(_) => {}
        };

        Ok(group_set)
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(span));
        self.types.intern(Type::Error)
    }
}

#[derive(Debug)]
pub enum AggrGroupError {
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<Label>),
}

struct AggrGroupSet {
    set: FnvHashSet<Option<Label>>,
    tallest_depth: u16,
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
        self.set.insert(aggr_group.map(|group| group.label));
        if let Some(group) = aggr_group {
            self.tallest_depth = core::cmp::max(self.tallest_depth, group.bind_depth.0)
        }
    }

    // Find a unique leaf in the aggregation forest
    fn disambiguate(
        self,
        ctx: &HirBuildCtx,
        max_depth: CtrlFlowDepth,
    ) -> Result<Label, AggrGroupError> {
        if self.tallest_depth > max_depth.0 {
            return Err(AggrGroupError::DepthExceeded);
        }

        let mut roots: FnvHashSet<Label> = Default::default();
        let mut parents: FnvHashSet<Label> = Default::default();

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

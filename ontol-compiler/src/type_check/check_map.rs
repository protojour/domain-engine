use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_hir::{Label, VarAllocator};
use ontol_runtime::smart_format;
use tracing::debug;

use crate::{
    codegen::{
        task::{ExplicitMapCodegenTask, MapArm, MapCodegenTask, MapKeyPair},
        type_mapper::TypeMapper,
    },
    def::Def,
    error::CompileError,
    mem::Intern,
    pattern::{PatId, Pattern, PatternKind, Patterns, RegexPatternCaptureNode},
    type_check::hir_build_ctx::{Arm, VariableMapping},
    typed_hir::TypedHir,
    types::{Type, TypeRef, Types},
    CompileErrors, Note, SourceSpan, SpannedNote,
};

use super::{
    hir_build_ctx::{CtrlFlowDepth, CtrlFlowGroup, HirBuildCtx, PatternVariable},
    hir_type_inference::{HirArmTypeInference, HirVariableMapper},
    TypeCheck, TypeEquation, TypeError,
};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_map(
        &mut self,
        def: &Def,
        var_allocator: &VarAllocator,
        first_id: PatId,
        second_id: PatId,
    ) -> Result<TypeRef<'m>, AggrGroupError> {
        let mut ctx = HirBuildCtx::new(def.span, VarAllocator::from(*var_allocator.peek_next()));

        {
            let mut map_check = MapCheck {
                types: self.types,
                errors: self.errors,
                patterns: self.patterns,
            };
            debug!("FIRST ARM START");
            ctx.arm = Arm::First;
            let _ = map_check.analyze_arm(
                map_check.patterns.table.get(&first_id).unwrap(),
                None,
                &mut ctx,
            )?;
            debug!("SECOND ARM START");
            ctx.arm = Arm::Second;
            let _ = map_check.analyze_arm(
                map_check.patterns.table.get(&second_id).unwrap(),
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
        first_id: PatId,
        second_id: PatId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<(), AggrGroupError> {
        ctx.arm = Arm::First;
        let mut first = self.build_root_pattern(first_id, ctx);
        self.infer_hir_arm_types(&mut first, ctx);

        ctx.arm = Arm::Second;
        let mut second = self.build_root_pattern(second_id, ctx);
        self.infer_hir_arm_types(&mut second, ctx);

        // unify the type of variables on either side:
        self.infer_hir_unify_arms(&mut first, &mut second, ctx);

        fn mk_map_arm(node: ontol_hir::RootNode<TypedHir>) -> MapArm {
            let is_match = match node.as_ref().value() {
                ontol_hir::Kind::Struct(_, flags, _) => {
                    flags.contains(ontol_hir::StructFlags::MATCH)
                }
                _ => false,
            };
            MapArm { node, is_match }
        }

        if let Some(key_pair) = TypeMapper::new(self.relations, self.defs, self.seal_ctx)
            .find_map_key_pair(first.as_ref().meta().ty, second.as_ref().meta().ty)
        {
            self.codegen_tasks.add_map_task(
                key_pair,
                crate::codegen::task::MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                    first: mk_map_arm(first),
                    second: mk_map_arm(second),
                    span: def.span,
                }),
            );
        }

        Ok(())
    }

    fn infer_hir_arm_types(
        &mut self,
        hir_root_node: &mut ontol_hir::RootNode<'m, TypedHir>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        let mut inference = HirArmTypeInference {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        for node in hir_root_node.arena_mut().iter_mut() {
            inference.infer_node(node);
        }
    }

    fn infer_hir_unify_arms(
        &mut self,
        first: &mut ontol_hir::RootNode<'m, TypedHir>,
        second: &mut ontol_hir::RootNode<'m, TypedHir>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (var, explicit_var) in &mut ctx.pattern_variables {
            let first_arm = explicit_var.hir_arms.get(&Arm::First);
            let second_arm = explicit_var.hir_arms.get(&Arm::Second);

            if let (Some(first_arm), Some(second_arm)) = (first_arm, second_arm) {
                let first_type_var = ctx.inference.new_type_variable(first_arm.pat_id);
                let second_type_var = ctx.inference.new_type_variable(second_arm.pat_id);

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
                                    *var,
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
        .map_vars(first.arena_mut());
        HirVariableMapper {
            variable_mapping: &ctx.variable_mapping,
            arm: Arm::Second,
        }
        .map_vars(second.arena_mut());
    }

    fn report_missing_prop_errors(&mut self, ctx: &mut HirBuildCtx<'m>) {
        for (_arm, missing) in std::mem::take(&mut ctx.missing_properties) {
            for (span, properties) in missing {
                let error = CompileError::MissingProperties(properties);
                self.error_with_notes(
                    error,
                    &span,
                    vec![SpannedNote {
                        note: Note::ConsiderUsingMatch,
                        span,
                    }],
                );
            }
        }
    }
}

pub struct MapCheck<'c, 'm> {
    types: &'c mut Types<'m>,
    errors: &'c mut CompileErrors,
    patterns: &'c Patterns,
}

impl<'c, 'm> MapCheck<'c, 'm> {
    fn analyze_arm(
        &mut self,
        expr: &Pattern,
        parent_aggr_group: Option<CtrlFlowGroup>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<AggrGroupSet, AggrGroupError> {
        let mut group_set = AggrGroupSet::new();

        match &expr.kind {
            PatternKind::Call(_, args) => {
                for arg in args.iter() {
                    group_set.join(self.analyze_arm(arg, parent_aggr_group, ctx)?);
                }
            }
            PatternKind::Struct {
                attributes: attrs, ..
            } => {
                for attr in attrs.iter() {
                    group_set.join(self.analyze_arm(&attr.value, parent_aggr_group, ctx)?);
                }
            }
            PatternKind::Seq(pat_id, elements) => {
                if ctx.arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm seq: pat_id={pat_id:?}");
                    ctx.ctrl_flow_forest
                        .insert(label, parent_aggr_group.map(|parent| parent.label));
                    ctx.label_map.insert(*pat_id, label);

                    ctx.enter_ctrl(|ctx| {
                        for element in elements {
                            // TODO: Skip non-iter?
                            let result = self.analyze_arm(
                                &element.pattern,
                                Some(CtrlFlowGroup {
                                    label,
                                    bind_depth: ctx.current_ctrl_flow_depth(),
                                }),
                                ctx,
                            );

                            assert!(result.is_ok());
                        }
                    });
                } else {
                    let outer_bind_depth = ctx.current_ctrl_flow_depth();

                    let mut inner_aggr_group = AggrGroupSet::new();
                    let mut iter_element_count = 0;

                    for element in elements {
                        if element.iter {
                            iter_element_count += 1;
                            ctx.enter_ctrl(|ctx| {
                                let group = self.analyze_arm(&element.pattern, None, ctx).unwrap();
                                inner_aggr_group.join(group);
                            });
                        }
                    }

                    ctx.enter_ctrl::<Result<(), AggrGroupError>>(|ctx| {
                        match inner_aggr_group.disambiguate(ctx, ctx.current_ctrl_flow_depth()) {
                            Ok(label) => {
                                ctx.label_map.insert(*pat_id, label);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(label).map(
                                    |label| CtrlFlowGroup {
                                        label,
                                        bind_depth: outer_bind_depth,
                                    },
                                ));
                                Ok(())
                            }
                            Err(error) => {
                                if iter_element_count > 0 {
                                    debug!("Failure: {error:?}");

                                    self.error(
                                        CompileError::TODO(smart_format!(
                                            "Incompatible aggregation group"
                                        )),
                                        &expr.span,
                                    );
                                    Err(error)
                                } else {
                                    // Since there's no iteration this is considered OK

                                    let label = Label(ctx.var_allocator.alloc().0);
                                    debug!("first arm seq: pat_id={pat_id:?}");
                                    ctx.ctrl_flow_forest.insert(
                                        label,
                                        parent_aggr_group.map(|parent| parent.label),
                                    );
                                    ctx.label_map.insert(*pat_id, label);

                                    Ok(())
                                }
                            }
                        }
                    })?
                }
            }
            PatternKind::Variable(var) => {
                self.register_variable(*var, &expr.span, parent_aggr_group, &mut group_set, ctx);
            }
            PatternKind::Regex(expr_regex) => {
                group_set.join(self.analyze_regex_capture_node(
                    &expr_regex.capture_node,
                    &expr.span,
                    parent_aggr_group,
                    ctx,
                )?);
            }
            PatternKind::ConstI64(_) | PatternKind::ConstText(_) => {}
        };

        Ok(group_set)
    }

    fn analyze_regex_capture_node(
        &mut self,
        node: &RegexPatternCaptureNode,
        full_span: &SourceSpan,
        parent_aggr_group: Option<CtrlFlowGroup>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<AggrGroupSet, AggrGroupError> {
        let mut group_set = AggrGroupSet::new();
        match node {
            RegexPatternCaptureNode::Capture { var, name_span, .. } => {
                self.register_variable(*var, name_span, parent_aggr_group, &mut group_set, ctx);
            }
            RegexPatternCaptureNode::Concat { nodes } => {
                for node in nodes {
                    group_set.join(self.analyze_regex_capture_node(
                        node,
                        full_span,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            RegexPatternCaptureNode::Alternation { variants } => {
                for variant in variants {
                    group_set.join(self.analyze_regex_capture_node(
                        variant,
                        full_span,
                        parent_aggr_group,
                        ctx,
                    )?);
                }
            }
            RegexPatternCaptureNode::Repetition {
                pat_id,
                node: inner_node,
                ..
            } => {
                if ctx.arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm regex repetition: pat_id={pat_id:?}");
                    ctx.ctrl_flow_forest
                        .insert(label, parent_aggr_group.map(|parent| parent.label));
                    ctx.label_map.insert(*pat_id, label);

                    ctx.enter_ctrl(|ctx| {
                        self.analyze_regex_capture_node(
                            inner_node,
                            full_span,
                            Some(CtrlFlowGroup {
                                label,
                                bind_depth: ctx.current_ctrl_flow_depth(),
                            }),
                            ctx,
                        )
                        .expect("repetition child failed");
                    });
                } else {
                    let outer_bind_depth = ctx.current_ctrl_flow_depth();

                    let mut inner_aggr_group = AggrGroupSet::new();
                    ctx.enter_ctrl(|ctx| {
                        let group = self
                            .analyze_regex_capture_node(
                                inner_node,
                                full_span,
                                parent_aggr_group,
                                ctx,
                            )
                            .unwrap();
                        inner_aggr_group.join(group);
                    });

                    ctx.enter_ctrl::<Result<(), AggrGroupError>>(|ctx| {
                        match inner_aggr_group.disambiguate(ctx, ctx.current_ctrl_flow_depth()) {
                            Ok(label) => {
                                ctx.label_map.insert(*pat_id, label);

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
                                    &node.constrain_span(*full_span),
                                );
                                Err(error)
                            }
                        }
                    })?
                }
            }
        }

        Ok(group_set)
    }

    fn register_variable(
        &mut self,
        var: ontol_hir::Var,
        span: &SourceSpan,
        parent_aggr_group: Option<CtrlFlowGroup>,
        group_set: &mut AggrGroupSet,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        if let Some(explicit_variable) = ctx.pattern_variables.get(&var) {
            // Variable is used more than once
            if ctx.arm.is_first() && explicit_variable.ctrl_group != parent_aggr_group {
                self.error(
                    CompileError::TODO(smart_format!("Incompatible aggregation group")),
                    span,
                );
            }

            debug!("Join existing bound variable");

            group_set.add(explicit_variable.ctrl_group);
        } else if ctx.arm.is_first() {
            ctx.pattern_variables.insert(
                var,
                PatternVariable {
                    ctrl_group: parent_aggr_group,
                    hir_arms: Default::default(),
                },
            );

            group_set.add(parent_aggr_group);
        } else {
            match ctx.pattern_variables.entry(var) {
                Entry::Occupied(_occ) => {
                    todo!();
                }
                Entry::Vacant(vac) => {
                    vac.insert(PatternVariable {
                        ctrl_group: parent_aggr_group,
                        hir_arms: Default::default(),
                    });
                    group_set.add(parent_aggr_group);
                }
            }
        }
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
        self.set.extend(other.set);
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

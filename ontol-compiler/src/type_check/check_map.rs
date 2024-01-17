use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_hir::{Label, VarAllocator};
use ontol_runtime::{smart_format, var::Var};
use tracing::debug;

use crate::{
    codegen::{
        task::{ExplicitMapCodegenTask, MapCodegenTask},
        type_mapper::TypeMapper,
    },
    def::Def,
    error::CompileError,
    map::UndirectedMapKey,
    mem::Intern,
    pattern::{CompoundPatternModifier, PatId, Pattern, PatternKind, RegexPatternCaptureNode},
    repr::repr_model::ReprKind,
    type_check::hir_build_ctx::{Arm, VariableMapping},
    typed_hir::TypedRootNode,
    types::{Type, TypeRef, ERROR_TYPE},
    CompileErrors, Note, SourceSpan, SpannedNote,
};

use super::{
    ena_inference::{KnownType, Strength},
    hir_build_ctx::{CtrlFlowDepth, HirBuildCtx, PatternVariable, SetElementGroup, ARMS},
    hir_type_inference::{HirArmTypeInference, HirVariableMapper},
    TypeCheck, TypeEquation, TypeError,
};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_map(
        &mut self,
        def: &Def,
        var_allocator: &VarAllocator,
        pat_ids: [PatId; 2],
    ) -> Result<TypeRef<'m>, CheckMapError> {
        let mut ctx = HirBuildCtx::new(def.span, VarAllocator::from(*var_allocator.peek_next()));

        let mut analyzer = PreAnalyzer {
            errors: self.errors,
        };
        for (pat_id, arm) in pat_ids.iter().zip(ARMS) {
            let _entered = arm.tracing_debug_span().entered();

            ctx.current_arm = arm;
            analyzer.analyze_arm(self.patterns.table.get(pat_id).unwrap(), None, &mut ctx)?;
        }

        self.build_typed_ontol_hir_arms(
            def,
            [(pat_ids[0], Arm::First), (pat_ids[1], Arm::Second)],
            &mut ctx,
        )?;

        self.report_missing_prop_errors(&mut ctx);

        Ok(self.types.intern(Type::Tautology))
    }

    fn build_typed_ontol_hir_arms(
        &mut self,
        def: &Def,
        input: [(PatId, Arm); 2],
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<(), CheckMapError> {
        let mut arm_nodes = input.map(|(pat_id, arm)| {
            let _entered = arm.tracing_debug_span().entered();

            ctx.current_arm = arm;
            let mut root_node = self.build_root_pattern(pat_id, ctx);
            self.infer_hir_arm_types(&mut root_node, ctx);
            root_node
        });

        // unify the type of variables on either side:
        self.infer_hir_unify_arms(def, &mut arm_nodes, ctx);

        if let Some(key_pair) = TypeMapper::new(self.relations, self.defs, self.seal_ctx)
            .find_map_key_pair([arm_nodes[0].data().ty(), arm_nodes[1].data().ty()])
        {
            self.codegen_tasks.add_map_task(
                key_pair,
                crate::codegen::task::MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                    def_id: def.id,
                    arms: arm_nodes,
                    span: def.span,
                }),
            );
        }

        Ok(())
    }

    fn infer_hir_arm_types(
        &mut self,
        hir_root_node: &mut TypedRootNode<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        let mut inference = HirArmTypeInference {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        for data in hir_root_node.arena_mut().iter_data_mut() {
            inference.infer(data);
        }
    }

    fn infer_hir_unify_arms(
        &mut self,
        def: &Def,
        arm_nodes: &mut [TypedRootNode<'m>; 2],
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (var, explicit_var) in &mut ctx.pattern_variables {
            let var_arms = match ARMS.map(|arm| explicit_var.hir_arms.get(&arm)) {
                [Some(first), Some(second)] => [first, second],
                _ => continue,
            };
            let type_vars = var_arms.map(|var_arm| ctx.inference.new_type_variable(var_arm.pat_id));

            let Err(type_error) = ctx
                .inference
                .eq_relations
                .unify_var_var(type_vars[0], type_vars[1])
            else {
                continue;
            };
            let TypeError::Mismatch(TypeEquation { actual, expected }) = type_error else {
                unreachable!();
            };

            match (actual.0.get_single_def_id(), expected.0.get_single_def_id()) {
                (Some(first_def_id), Some(second_def_id))
                    if actual.0.is_domain_specific() || expected.0.is_domain_specific() =>
                {
                    if let Some(ty) = self
                        .get_strong_type_for_distinct_weakly_repr_compatible_types(actual, expected)
                    {
                        ctx.variable_mapping
                            .insert(*var, VariableMapping::Overwrite(ty));
                    } else {
                        ctx.variable_mapping
                            .insert(*var, VariableMapping::Mapping([actual.0, expected.0]));

                        self.codegen_tasks.add_map_task(
                            UndirectedMapKey::new([first_def_id.into(), second_def_id.into()]),
                            MapCodegenTask::Auto(def.id.package_id()),
                        );
                    }
                }
                _ => {
                    self.type_error(
                        TypeError::Mismatch(TypeEquation { actual, expected }),
                        &var_arms[1].span,
                    );
                }
            }
        }

        for (node, arm) in arm_nodes.iter_mut().zip(ARMS) {
            HirVariableMapper {
                variable_mapping: &ctx.variable_mapping,
                arm,
            }
            .map_vars(node.arena_mut());
        }
    }

    /// Computes whether two scalar types are compatible when one has
    /// a weak type constraint and the other has a strong strong type constraint.
    /// In that case, the variables do not need to be mapped.
    /// If successfull, returns the strong type.
    fn get_strong_type_for_distinct_weakly_repr_compatible_types(
        &self,
        (first_ty, first_strength): KnownType<'m>,
        (second_ty, second_strength): KnownType<'m>,
    ) -> Option<TypeRef<'m>> {
        if first_strength == second_strength {
            return None;
        }

        let first_def_id = first_ty.get_single_def_id().unwrap();
        let second_def_id = second_ty.get_single_def_id().unwrap();
        let first_repr = self.seal_ctx.get_repr_kind(&first_def_id).unwrap();
        let second_repr = self.seal_ctx.get_repr_kind(&second_def_id).unwrap();

        match (first_repr, second_repr) {
            (
                ReprKind::Scalar(first_sc_def_id, first_kind, _),
                ReprKind::Scalar(second_sc_def_id, second_kind, _),
            ) => {
                if first_kind != second_kind {
                    return None;
                }

                if first_sc_def_id == second_sc_def_id {
                    if first_strength == Strength::Strong {
                        Some(first_ty)
                    } else {
                        Some(second_ty)
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
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

enum MapOutputClass {
    /// The output is interpreted as a function of the opposing arm.
    /// All output data can be derived and computed from from input data.
    Data,
    /// The output is interpreted as match on some entity storage, meant to produce one value.
    /// Requires some form of runtime datastore to be computed.
    FindMatch,
    /// The output is interpreted as a match on some entity store meant to produce several values.
    /// Requires some form of runtime datastore to be computed.
    FilterMatch,
}

struct PreAnalyzer<'c> {
    errors: &'c mut CompileErrors,
}

struct ArmAnalysis {
    group_set: AggrGroupSet,
    class: MapOutputClass,
}

impl<'c> PreAnalyzer<'c> {
    fn analyze_arm(
        &mut self,
        pat: &Pattern,
        parent_aggr_group: Option<SetElementGroup>,
        ctx: &mut HirBuildCtx<'_>,
    ) -> Result<ArmAnalysis, CheckMapError> {
        let pat_id = pat.id;
        let mut group_set = AggrGroupSet::new();
        let mut arm_class = MapOutputClass::Data;

        match &pat.kind {
            PatternKind::Call(_, args) => {
                for arg in args.iter() {
                    group_set.join(self.analyze_arm(arg, parent_aggr_group, ctx)?.group_set);
                }
            }
            PatternKind::Compound {
                attributes: attrs,
                modifier,
                ..
            } => {
                for attr in attrs.iter() {
                    group_set.join(
                        self.analyze_arm(&attr.value, parent_aggr_group, ctx)?
                            .group_set,
                    );
                }

                if matches!(modifier, Some(CompoundPatternModifier::Match)) {
                    arm_class = MapOutputClass::FindMatch;
                }
            }
            PatternKind::Set { elements, .. } => {
                let contains_iter_element = elements.iter().any(|element| element.iter);

                if ctx.current_arm.is_first() {
                    group_set.add(parent_aggr_group);

                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm set: pat_id={pat_id:?}");
                    ctx.ctrl_flow_forest
                        .insert(label, parent_aggr_group.map(|parent| parent.label));
                    ctx.label_map.insert(pat_id, label);

                    ctx.enter_ctrl(|ctx| {
                        for element in elements {
                            // TODO: Skip non-iter?
                            let result = self.analyze_arm(
                                &element.pattern,
                                Some(SetElementGroup {
                                    label,
                                    iterated: element.iter,
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
                    let mut iter_match_element_count = 0;

                    for element in elements {
                        if element.iter {
                            iter_element_count += 1;
                        } else if contains_iter_element {
                            continue;
                        }

                        ctx.enter_ctrl(|ctx| {
                            let analysis = self.analyze_arm(&element.pattern, None, ctx).unwrap();
                            inner_aggr_group.join(analysis.group_set);

                            if !matches!(analysis.class, MapOutputClass::Data) {
                                // A match directly in an iterated element does not require
                                // finding an exact aggregation group
                                iter_match_element_count += 1;

                                arm_class = MapOutputClass::FilterMatch;
                            }
                        });
                    }

                    ctx.enter_ctrl::<Result<(), CheckMapError>>(|ctx| {
                        match inner_aggr_group.disambiguate(ctx.current_ctrl_flow_depth(), ctx) {
                            Ok(label) => {
                                ctx.label_map.insert(pat_id, label);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(label).map(
                                    |label| SetElementGroup {
                                        label,
                                        iterated: true,
                                        bind_depth: outer_bind_depth,
                                    },
                                ));
                                Ok(())
                            }
                            Err(error) => {
                                if iter_element_count > 0
                                    && iter_element_count > iter_match_element_count
                                {
                                    debug!("Failure: {error:?}");

                                    self.error(
                                        CompileError::TODO(smart_format!(
                                            "Incompatible aggregation group"
                                        )),
                                        &pat.span,
                                    );
                                    Err(error)
                                } else {
                                    // Since there's no iteration (or all the iterations are `match`) this is considered OK.
                                    // FIXME: But a `match` can still iterate on variables.
                                    // So the logic here has to be improved.

                                    let label = Label(ctx.var_allocator.alloc().0);
                                    debug!("first arm set: pat_id={pat_id:?}");
                                    ctx.ctrl_flow_forest.insert(
                                        label,
                                        parent_aggr_group.map(|parent| parent.label),
                                    );
                                    ctx.label_map.insert(pat_id, label);

                                    Ok(())
                                }
                            }
                        }
                    })?
                }
            }
            PatternKind::Variable(var) => {
                self.register_variable(*var, &pat.span, parent_aggr_group, &mut group_set, ctx);
            }
            PatternKind::Regex(expr_regex) => {
                group_set.join(self.analyze_regex_capture_node(
                    &expr_regex.capture_node,
                    &pat.span,
                    parent_aggr_group,
                    ctx,
                )?);
            }
            PatternKind::ConstI64(_) | PatternKind::ConstText(_) => {}
            PatternKind::ContainsElement(_) => {}
            PatternKind::SetOperator { .. } => {}
        };

        Ok(ArmAnalysis {
            group_set,
            class: arm_class,
        })
    }

    fn analyze_regex_capture_node(
        &mut self,
        node: &RegexPatternCaptureNode,
        full_span: &SourceSpan,
        parent_set_element_group: Option<SetElementGroup>,
        ctx: &mut HirBuildCtx<'_>,
    ) -> Result<AggrGroupSet, CheckMapError> {
        let mut group_set = AggrGroupSet::new();
        match node {
            RegexPatternCaptureNode::Capture { var, name_span, .. } => {
                self.register_variable(
                    *var,
                    name_span,
                    parent_set_element_group,
                    &mut group_set,
                    ctx,
                );
            }
            RegexPatternCaptureNode::Concat { nodes } => {
                for node in nodes {
                    group_set.join(self.analyze_regex_capture_node(
                        node,
                        full_span,
                        parent_set_element_group,
                        ctx,
                    )?);
                }
            }
            RegexPatternCaptureNode::Alternation { variants } => {
                for variant in variants {
                    group_set.join(self.analyze_regex_capture_node(
                        variant,
                        full_span,
                        parent_set_element_group,
                        ctx,
                    )?);
                }
            }
            RegexPatternCaptureNode::Repetition {
                pat_id,
                node: inner_node,
                ..
            } => {
                if ctx.current_arm.is_first() {
                    group_set.add(parent_set_element_group);

                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm regex repetition: pat_id={pat_id:?}");
                    ctx.ctrl_flow_forest
                        .insert(label, parent_set_element_group.map(|parent| parent.label));
                    ctx.label_map.insert(*pat_id, label);

                    ctx.enter_ctrl(|ctx| {
                        self.analyze_regex_capture_node(
                            inner_node,
                            full_span,
                            Some(SetElementGroup {
                                label,
                                iterated: true,
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
                                parent_set_element_group,
                                ctx,
                            )
                            .unwrap();
                        inner_aggr_group.join(group);
                    });

                    ctx.enter_ctrl::<Result<(), CheckMapError>>(|ctx| {
                        match inner_aggr_group.disambiguate(ctx.current_ctrl_flow_depth(), ctx) {
                            Ok(label) => {
                                ctx.label_map.insert(*pat_id, label);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(label).map(
                                    |label| SetElementGroup {
                                        label,
                                        iterated: true,
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
        var: Var,
        span: &SourceSpan,
        parent_set_element_group: Option<SetElementGroup>,
        group_set: &mut AggrGroupSet,
        ctx: &mut HirBuildCtx<'_>,
    ) {
        if let Some(explicit_variable) = ctx.pattern_variables.get(&var) {
            // Variable is used more than once
            if ctx.current_arm.is_first()
                && explicit_variable.set_element_group != parent_set_element_group
            {
                self.error(
                    CompileError::TODO(smart_format!("Incompatible aggregation group")),
                    span,
                );
            }

            debug!("Join existing bound variable");

            group_set.add(explicit_variable.set_element_group);
        } else if ctx.current_arm.is_first() {
            ctx.pattern_variables.insert(
                var,
                PatternVariable {
                    set_element_group: parent_set_element_group,
                    hir_arms: Default::default(),
                },
            );

            group_set.add(parent_set_element_group);
        } else {
            match ctx.pattern_variables.entry(var) {
                Entry::Occupied(_occ) => {
                    todo!();
                }
                Entry::Vacant(vac) => {
                    vac.insert(PatternVariable {
                        set_element_group: parent_set_element_group,
                        hir_arms: Default::default(),
                    });
                    group_set.add(parent_set_element_group);
                }
            }
        }
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef {
        self.errors.push(error.spanned(span));
        &ERROR_TYPE
    }
}

#[derive(Debug)]
pub enum CheckMapError {
    MutualArmInference,
    ArmNotInferrable,
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<Label>),
}

struct AggrGroupSet {
    set: FnvHashSet<Option<Label>>,
    iterated: bool,
    tallest_depth: u16,
}

impl AggrGroupSet {
    fn new() -> Self {
        Self {
            set: Default::default(),
            iterated: false,
            tallest_depth: 0,
        }
    }

    fn join(&mut self, other: AggrGroupSet) {
        self.set.extend(other.set);
        self.iterated |= other.iterated;
        self.tallest_depth = core::cmp::max(self.tallest_depth, other.tallest_depth)
    }

    fn add(&mut self, set_element_group: Option<SetElementGroup>) {
        self.set.insert(set_element_group.map(|group| group.label));
        if let Some(group) = set_element_group {
            if group.iterated {
                self.iterated = true;
            }
            self.tallest_depth = core::cmp::max(self.tallest_depth, group.bind_depth.0)
        }
    }

    // Find a unique leaf in the aggregation forest
    fn disambiguate(
        self,
        max_depth: CtrlFlowDepth,
        ctx: &HirBuildCtx,
    ) -> Result<Label, CheckMapError> {
        if self.tallest_depth > max_depth.0 {
            return Err(CheckMapError::DepthExceeded);
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
                    0 => Err(CheckMapError::NoLeaves),
                    1 => Ok(leaves.into_iter().next().unwrap()),
                    _ => Err(CheckMapError::TooManyLeaves(leaves)),
                }
            }
            other => Err(CheckMapError::RootCount(other)),
        }
    }
}

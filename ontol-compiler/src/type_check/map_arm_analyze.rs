use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use ontol_hir::Label;
use ontol_runtime::{smart_format, var::Var};
use tracing::debug;

use crate::{
    pattern::{
        CompoundPatternAttrKind, CompoundPatternModifier, Pattern, PatternKind,
        RegexPatternCaptureNode, SetPatternElement,
    },
    types::{TypeRef, ERROR_TYPE},
    CompileError, CompileErrors, SourceSpan,
};

use super::{
    check_map::{CheckMapError, MapOutputClass},
    hir_build_ctx::{CtrlFlowDepth, HirBuildCtx, PatternVariable, SetElementGroup},
};

pub struct ArmAnalysis {
    group_set: AggrGroupSet,
    class: MapOutputClass,
}

pub struct PreAnalyzer<'c> {
    pub errors: &'c mut CompileErrors,
}

impl<'c> PreAnalyzer<'c> {
    pub fn analyze_arm(
        &mut self,
        pat: &Pattern,
        parent_aggr_group: Option<SetElementGroup>,
        ctx: &mut HirBuildCtx<'_>,
    ) -> Result<ArmAnalysis, CheckMapError> {
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
                    match &attr.kind {
                        CompoundPatternAttrKind::Value { val, .. }
                        | CompoundPatternAttrKind::ContainsElement { val, .. } => {
                            group_set
                                .join(self.analyze_arm(val, parent_aggr_group, ctx)?.group_set);
                        }
                        CompoundPatternAttrKind::SetOperator { elements, .. } => {
                            self.analyze_set_pattern_elements(
                                elements,
                                &pat.span,
                                parent_aggr_group,
                                (&mut group_set, &mut arm_class),
                                ctx,
                            )?;
                        }
                    }
                }

                if matches!(modifier, Some(CompoundPatternModifier::Match)) {
                    arm_class = MapOutputClass::FindMatch;
                }
            }
            PatternKind::Set { elements, .. } => {
                self.analyze_set_pattern_elements(
                    elements,
                    &pat.span,
                    parent_aggr_group,
                    (&mut group_set, &mut arm_class),
                    ctx,
                )?;
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
        };

        Ok(ArmAnalysis {
            group_set,
            class: arm_class,
        })
    }

    fn analyze_set_pattern_elements(
        &mut self,
        elements: &[SetPatternElement],
        pat_span: &SourceSpan,
        parent_aggr_group: Option<SetElementGroup>,
        (group_set, arm_class): (&mut AggrGroupSet, &mut MapOutputClass),
        ctx: &mut HirBuildCtx<'_>,
    ) -> Result<(), CheckMapError> {
        if ctx.current_arm.is_first() {
            group_set.add(parent_aggr_group);

            for element in elements.iter() {
                let result = if element.is_iter {
                    // Register aggregation body
                    let label = Label(ctx.var_allocator.alloc().0);
                    debug!("first arm set: pat_id={:?}", element.id);
                    ctx.ctrl_flow_forest
                        .insert(label, parent_aggr_group.map(|parent| parent.label));
                    ctx.label_map.insert(element.id, label);

                    ctx.enter_ctrl(|ctx| {
                        self.analyze_arm(
                            &element.val,
                            Some(SetElementGroup {
                                label,
                                iterated: element.is_iter,
                                bind_depth: ctx.current_ctrl_flow_depth(),
                            }),
                            ctx,
                        )
                    })
                } else {
                    self.analyze_arm(&element.val, parent_aggr_group, ctx)
                };

                // TODO: Skip non-iter?

                assert!(result.is_ok());
            }
        } else {
            let outer_bind_depth = ctx.current_ctrl_flow_depth();

            for element in elements.iter() {
                let mut inner_aggr_group = AggrGroupSet::new();

                if element.is_iter {
                    ctx.enter_ctrl(|ctx| {
                        let analysis = self.analyze_arm(&element.val, None, ctx).unwrap();
                        inner_aggr_group.join(analysis.group_set);

                        if !matches!(analysis.class, MapOutputClass::Data) {
                            *arm_class = MapOutputClass::FilterMatch;
                        }

                        match inner_aggr_group.disambiguate(ctx.current_ctrl_flow_depth(), ctx) {
                            Ok(label) => {
                                ctx.label_map.insert(element.id, label);

                                group_set.add(ctx.ctrl_flow_forest.find_parent(label).map(
                                    |label| SetElementGroup {
                                        label,
                                        iterated: true,
                                        bind_depth: outer_bind_depth,
                                    },
                                ));
                            }
                            Err(_) => {
                                if matches!(*arm_class, MapOutputClass::FilterMatch) {
                                    let label = Label(ctx.var_allocator.alloc().0);
                                    debug!("FALLBACK first arm set: pat_id={:?}", element.id);
                                    ctx.ctrl_flow_forest.insert(
                                        label,
                                        parent_aggr_group.map(|parent| parent.label),
                                    );
                                    ctx.label_map.insert(element.id, label);
                                } else {
                                    self.error(
                                        CompileError::TODO(smart_format!(
                                            "Incompatible aggregation group"
                                        )),
                                        pat_span,
                                    );
                                }
                            }
                        }
                    });
                } else {
                    self.analyze_arm(&element.val, None, ctx).unwrap();
                }
            }
        }

        Ok(())
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

pub struct AggrGroupSet {
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

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    smart_format,
    var::{Var, VarSet},
};
use tracing::debug;

use crate::{
    hir_unify::{
        expr,
        flat_level_builder::LevelBuilder,
        flat_scope::{self, OutputVar},
        flat_unifier::unifier_todo,
        flat_unifier_table::IsInScope,
        seq_type_infer::SeqTypeInfer,
    },
    typed_hir::{self, IntoTypedHirData, TypedHir, TypedHirData, UNIT_META},
    NO_SPAN,
};

use super::{
    dep_tree::Expression,
    flat_scope::ScopeVar,
    flat_unifier::{FlatUnifier, Level, MainScope, StructuralOrigin},
    flat_unifier_expr_to_node::ScopedExprToNode,
    flat_unifier_table::{Assignment, ExprSelector, ScopedAssignment, Table},
    unifier::UnifiedNode,
    UnifierError, UnifierResult,
};

#[derive(Default)]
struct ScopeGroup<'m> {
    assignments: Vec<ScopedAssignment<'m>>,
    introducing: VarSet,
    subgroups: FnvHashMap<ScopeVar, ScopeGroup<'m>>,
}

pub(super) fn unify_single<'m>(
    parent_scope_var: Option<ScopeVar>,
    in_scope: VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<UnifiedNode<'m>> {
    if let Some(const_expr) = table.const_expr.take() {
        return Ok(UnifiedNode {
            typed_binder: None,
            node: ScopedExprToNode {
                table,
                unifier,
                scope_var: None,
                level: Level(0),
            }
            .scoped_expr_to_node(const_expr, &in_scope, MainScope::Const)?,
        });
    }

    if let Some(var) = parent_scope_var {
        if in_scope.contains(var.0) {
            panic!("Variable is already in scope");
        }
    }

    let indexes = table.dependees(parent_scope_var);
    let Some(index) = indexes.into_iter().next() else {
        panic!("multiple indexes");
    };

    let scope_map = &mut table.scope_map_mut(index);
    let scope_meta = scope_map.scope.meta().clone();
    let scope_var = scope_meta.scope_var;

    match (scope_map.take_single_assignment(), scope_map.scope.kind()) {
        (
            Some(Assignment {
                expr: expr::Expr(kind, meta),
                ..
            }),
            flat_scope::Kind::Var,
        ) => {
            let typed_binder = Some(
                ontol_hir::Binder {
                    var: scope_map.scope.meta().scope_var.0,
                }
                .with_meta(scope_map.scope.meta().hir_meta),
            );

            let inner_node = ScopedExprToNode {
                table,
                unifier,
                scope_var: Some(scope_var),
                level: Level(0),
            }
            .scoped_expr_to_node(
                expr::Expr(kind, meta),
                &in_scope,
                MainScope::Value(scope_var),
            )?;

            Ok(UnifiedNode {
                typed_binder,
                node: inner_node,
            })
        }
        (
            Some(Assignment {
                expr:
                    expr::Expr(
                        expr::Kind::Struct {
                            binder,
                            flags,
                            props,
                        },
                        meta,
                    ),
                ..
            }),
            flat_scope::Kind::Struct,
        ) => {
            // FIXME/DRY: This looks a lot like the expr code
            let next_in_scope = in_scope.union_one(scope_meta.scope_var.0);

            unifier.push_struct_expr_flags(flags, meta.hir_meta.ty)?;

            let mut body = unify_scope_structural(
                MainScope::Value(scope_meta.scope_var),
                ExprSelector::Struct(binder.hir().var, scope_var),
                StructuralOrigin::DependeesOf(scope_var),
                next_in_scope.clone(),
                table,
                unifier,
                Level(0),
            )?;

            for prop in props {
                let free_vars = prop.free_vars.clone();
                let prop_node = ScopedExprToNode {
                    table,
                    unifier,
                    scope_var: Some(scope_var),
                    level: Level(0),
                }
                .scoped_expr_to_node(
                    expr::Expr(
                        expr::Kind::Prop(Box::new(prop)),
                        expr::Meta {
                            free_vars,
                            hir_meta: UNIT_META,
                        },
                    ),
                    &next_in_scope,
                    MainScope::Value(scope_var),
                )?;

                body.push(prop_node);
            }

            let node = UnifiedNode {
                typed_binder: Some(
                    ontol_hir::Binder {
                        var: scope_meta.scope_var.0,
                    }
                    .with_meta(scope_meta.hir_meta),
                ),
                node: unifier.mk_node(
                    ontol_hir::Kind::Struct(binder, flags, body.into()),
                    meta.hir_meta,
                ),
            };

            unifier.pop_struct_expr_flags(flags);

            Ok(node)
        }
        other => Err(unifier_todo(smart_format!("Handle pair {other:?}"))),
    }
}

pub(super) fn unify_scope_structural<'m>(
    main_scope: MainScope,
    selector: ExprSelector,
    origin: StructuralOrigin,
    in_scope: VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<Vec<ontol_hir::Node>> {
    let mut indexes = match origin {
        StructuralOrigin::DependeesOf(parent_scope_var) => table.dependees(Some(parent_scope_var)),
        StructuralOrigin::Start => vec![0],
    };

    let mut builder = LevelBuilder::default();

    debug!("{level}USS main_scope={main_scope:?} {selector:?} origin={origin:?}");

    while !indexes.is_empty() {
        let mut next_indexes = vec![];

        debug!("{level}indexes={indexes:?}");

        for index in indexes {
            let scope_map = &mut table.scope_map_mut(index);
            let scope_var = scope_map.scope.meta().scope_var;

            match scope_map.scope.kind() {
                flat_scope::Kind::PropRelParam
                | flat_scope::Kind::PropValue
                | flat_scope::Kind::Struct
                | flat_scope::Kind::Var
                | flat_scope::Kind::RegexAlternation
                | flat_scope::Kind::RegexCapture(_) => {
                    builder.output.extend(apply_lateral_scope(
                        main_scope,
                        scope_map.select_assignments(selector),
                        &|| in_scope.clone(),
                        table,
                        unifier,
                        level,
                    )?);

                    next_indexes.extend(table.dependees(Some(scope_var)));
                }
                flat_scope::Kind::PropVariant(_, optional, prop_struct_var, property_id) => {
                    let optional = *optional;
                    let property_id = *property_id;
                    let prop_key = (optional, *prop_struct_var, property_id);
                    let mut body = vec![];
                    let inner_scope = in_scope.union(&scope_map.scope.meta().defs);

                    let mut assignments = scope_map.select_assignments(selector);
                    let assignments_len = assignments.len();

                    if optional.0 {
                        for assignment in &mut assignments {
                            // FIXME: Did not remodel "expr" yet,
                            // so re-traverse for free vars here.
                            //
                            // The point is that _structs_ do not contain the properties "inline" anymore,
                            // the props are already scattered around the scope table.
                            // This will move successive scope introductions to _inside_ the struct
                            // rather than outside.
                            let new_free_vars = expr::collect_free_vars(&assignment.expr);

                            assignment.expr.1.free_vars = new_free_vars;
                        }
                    }

                    body.extend(apply_lateral_scope(
                        main_scope,
                        assignments,
                        &|| inner_scope.clone(),
                        table,
                        unifier,
                        level.next(),
                    )?);

                    let bindings = table.rel_val_bindings(scope_var);
                    if !bindings.is_in_scope(&in_scope) {
                        debug!("{level}{property_id:?}: bindings {bindings:?} were not in scope");
                        body.extend(unify_scope_structural(
                            main_scope,
                            selector,
                            StructuralOrigin::DependeesOf(scope_var),
                            inner_scope.union(&bindings.var_set()),
                            table,
                            unifier,
                            level.next(),
                        )?);

                        builder.add_prop_variant_scope(prop_key, bindings, body);
                    } else {
                        debug!("{level}{property_id:?}: bindings {bindings:?} were in scope. Assignments len = {assignments_len}");
                        builder.output.extend(body);
                        next_indexes.extend(table.dependees(Some(scope_var)));
                    }
                }
                flat_scope::Kind::SeqPropVariant(
                    label,
                    _output_var,
                    optional,
                    has_default,
                    prop_struct_var,
                    property_id,
                ) => {
                    let label = *label;
                    let prop_key = (*optional, *has_default, *prop_struct_var, *property_id);
                    let mut body = vec![];
                    let inner_scope = in_scope.union(&scope_map.scope.meta().defs);

                    body.extend(apply_lateral_scope(
                        main_scope,
                        scope_map.select_assignments(selector),
                        &|| inner_scope.clone(),
                        table,
                        unifier,
                        level.next(),
                    )?);

                    if !in_scope.contains(Var(label.hir().0)) {
                        body.extend(unify_scope_structural(
                            main_scope,
                            selector,
                            StructuralOrigin::DependeesOf(scope_var),
                            inner_scope,
                            table,
                            unifier,
                            level.next(),
                        )?);

                        builder.add_seq_prop_variant_scope(scope_var, prop_key, body, table);
                    } else {
                        builder.output.extend(body);
                        next_indexes.extend(table.dependees(Some(scope_var)));
                    }
                }
                flat_scope::Kind::IterElement(label, output_var) => {
                    if scope_map.assignments.is_empty() {
                        debug!("IterElement empty");
                        next_indexes.extend(table.dependees(Some(scope_var)));
                    } else if in_scope.contains(output_var.0) {
                        let label = *label;
                        let output_var = *output_var;
                        let assignments = scope_map.take_assignments();
                        let bindings = table.rel_val_bindings(scope_var);

                        let push_nodes = apply_lateral_scope(
                            MainScope::Sequence(scope_var, output_var),
                            assignments,
                            &|| {
                                let mut next_in_scope = in_scope.clone();
                                if let ontol_hir::Binding::Binder(binder) = &bindings.rel {
                                    next_in_scope.insert(binder.hir().var);
                                }
                                if let ontol_hir::Binding::Binder(binder) = &bindings.val {
                                    next_in_scope.insert(binder.hir().var);
                                }
                                next_in_scope
                            },
                            table,
                            unifier,
                            level,
                        )?;

                        if !push_nodes.is_empty() {
                            builder.output.push(unifier.mk_node(
                                ontol_hir::Kind::ForEach(
                                    Var(label.0),
                                    (bindings.rel, bindings.val),
                                    push_nodes.into(),
                                ),
                                UNIT_META,
                            ));
                        }
                    }
                }
                flat_scope::Kind::Regex(opt_seq_label, regex_def_id) => {
                    let opt_seq_label = *opt_seq_label;
                    let regex_def_id = *regex_def_id;
                    let regex_hir_meta = scope_map.scope.meta().hir_meta;

                    let next_in_scope = in_scope.union_one(scope_var.0);

                    let capture_scope_union = {
                        let mut union = VarSet::default();
                        for alt_idx in table.dependees(Some(scope_var)) {
                            let alt_scope_var = table.scope_map_mut(alt_idx).scope.meta().scope_var;

                            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                                let cap_scope_map = &mut table.scope_map_mut(cap_scope_idx);
                                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                                union.insert(cap_scope_var.0);
                            }
                        }
                        union
                    };

                    let scope_map = &mut table.scope_map_mut(index);

                    if !in_scope.0.is_disjoint(&capture_scope_union.0) {
                        builder.output.extend(apply_lateral_scope(
                            main_scope,
                            scope_map.select_assignments(selector),
                            &|| in_scope.clone(),
                            table,
                            unifier,
                            level.next(),
                        )?);
                        next_indexes.extend(table.dependees(Some(scope_var)));
                    } else if let Some(_seq_label) = opt_seq_label {
                        debug!("{level}looping regex");
                        // looping regex
                        let mut match_arms: Vec<ontol_hir::CaptureMatchArm<'m, TypedHir>> = vec![];

                        let mut seq_type_inferers: IndexMap<ontol_hir::Label, SeqTypeInfer<'m>> =
                            Default::default();

                        let mut hir_props: Vec<ontol_hir::Node> = vec![];

                        // Analyze which sequences are under scrutiny and allocate output variables
                        for assignment in std::mem::take(&mut scope_map.assignments) {
                            if let expr::Expr(expr::Kind::Prop(prop), meta) = assignment.expr {
                                if let expr::PropVariant::Seq { label, .. } = &prop.variant {
                                    let inferer =
                                        seq_type_inferers.entry(*label).or_insert_with(|| {
                                            SeqTypeInfer::new(OutputVar(
                                                unifier.var_allocator.alloc(),
                                            ))
                                        });

                                    let rel = unifier.mk_node(ontol_hir::Kind::Unit, UNIT_META);
                                    let val = unifier.mk_node(
                                        ontol_hir::Kind::Var(inferer.output_seq_var.0),
                                        meta.hir_meta,
                                    );

                                    hir_props.push(
                                        unifier.mk_node(
                                            ontol_hir::Kind::Prop(
                                                prop.optional,
                                                prop.struct_var,
                                                prop.prop_id,
                                                [ontol_hir::PropVariant::Singleton(
                                                    ontol_hir::Attribute { rel, val },
                                                )]
                                                .into(),
                                            ),
                                            meta.hir_meta,
                                        ),
                                    );
                                }
                            }
                        }

                        // Input for sequence type "inference"
                        for scope_map in table.table_mut() {
                            for assignment in &scope_map.assignments {
                                if let expr::Kind::SeqItem(label, _, _, attr) =
                                    assignment.expr.kind()
                                {
                                    if let Some(infer) = seq_type_inferers.get_mut(label) {
                                        infer.types.push((
                                            attr.rel.meta().hir_meta.ty,
                                            attr.val.meta().hir_meta.ty,
                                        ));
                                    }
                                }
                            }
                        }

                        // alternations:
                        for alt_idx in table.dependees(Some(scope_var)) {
                            let alt_scope_var = table.scope_map_mut(alt_idx).scope.meta().scope_var;

                            let mut match_arm = ontol_hir::CaptureMatchArm {
                                capture_groups: vec![],
                                nodes: Default::default(),
                            };
                            let mut captured_scope = VarSet::default();

                            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                                let cap_scope_map = &mut table.scope_map_mut(cap_scope_idx);
                                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                                let flat_scope::Kind::RegexCapture(cap_index) =
                                    cap_scope_map.scope.kind()
                                else {
                                    panic!("Expected regex capture");
                                };

                                match_arm.capture_groups.push(ontol_hir::CaptureGroup {
                                    index: *cap_index,
                                    binder: ontol_hir::Binder {
                                        var: cap_scope_var.0,
                                    }
                                    .with_meta(cap_scope_map.scope.meta().hir_meta),
                                });
                                captured_scope.insert(cap_scope_var.0);
                            }

                            match_arm.nodes.extend(apply_lateral_scope(
                                MainScope::MultiSequence(&seq_type_inferers),
                                table.scope_map_mut(alt_idx).take_assignments(),
                                &|| next_in_scope.union(&captured_scope),
                                table,
                                unifier,
                                level.next(),
                            )?);

                            match_arm.nodes.extend(unify_scope_structural(
                                main_scope,
                                ExprSelector::SeqItem,
                                StructuralOrigin::DependeesOf(alt_scope_var),
                                captured_scope,
                                table,
                                unifier,
                                level.next(),
                            )?);

                            match_arms.push(match_arm);
                        }

                        let mut nodes: ontol_hir::Nodes = Default::default();
                        nodes.push(unifier.mk_node(
                            ontol_hir::Kind::MatchRegex(
                                ontol_hir::Iter(true),
                                scope_var.0,
                                regex_def_id,
                                match_arms,
                            ),
                            regex_hir_meta,
                        ));
                        nodes.extend(hir_props);

                        // wrap everything in sequence binders
                        for (label, seq_type_infer) in seq_type_inferers {
                            let output_seq_var = seq_type_infer.output_seq_var;
                            let sequence_ty = seq_type_infer.infer(unifier.types);
                            let sequence_meta = typed_hir::Meta {
                                ty: sequence_ty,
                                // FIXME: SPAN
                                span: NO_SPAN,
                            };

                            let let_def = unifier.mk_node(
                                ontol_hir::Kind::Sequence(
                                    ontol_hir::Binder { var: Var(label.0) }
                                        .with_meta(sequence_meta),
                                    ontol_hir::Nodes::default(),
                                ),
                                sequence_meta,
                            );

                            let let_node = unifier.mk_node(
                                ontol_hir::Kind::Let(
                                    ontol_hir::Binder {
                                        var: output_seq_var.0,
                                    }
                                    .with_meta(UNIT_META),
                                    let_def,
                                    nodes,
                                ),
                                UNIT_META,
                            );

                            nodes = Default::default();
                            nodes.push(let_node);
                        }

                        builder.output.extend(nodes);
                    } else {
                        // normal, one-shot regex
                        let mut match_arms: Vec<ontol_hir::CaptureMatchArm<'m, TypedHir>> = vec![];

                        // alternations:
                        for alt_idx in table.dependees(Some(scope_var)) {
                            let alt_scope_map = table.scope_map_mut(alt_idx);
                            let alt_scope_var = alt_scope_map.scope.meta().scope_var;

                            let mut match_arm = ontol_hir::CaptureMatchArm {
                                capture_groups: vec![],
                                nodes: ontol_hir::Nodes::default(),
                            };
                            let mut captured_scope = VarSet::default();

                            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                                let cap_scope_map = &mut table.scope_map_mut(cap_scope_idx);
                                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                                let flat_scope::Kind::RegexCapture(cap_index) =
                                    cap_scope_map.scope.kind()
                                else {
                                    panic!("Expected regex capture");
                                };

                                match_arm.capture_groups.push(ontol_hir::CaptureGroup {
                                    index: *cap_index,
                                    binder: TypedHirData(
                                        ontol_hir::Binder {
                                            var: cap_scope_var.0,
                                        },
                                        cap_scope_map.scope.meta().hir_meta,
                                    ),
                                });
                                captured_scope.insert(cap_scope_var.0);
                            }

                            match_arm.nodes.extend(unify_scope_structural(
                                main_scope,
                                selector,
                                StructuralOrigin::DependeesOf(alt_scope_var),
                                captured_scope,
                                table,
                                unifier,
                                level.next(),
                            )?);

                            match_arms.push(match_arm);
                        }

                        builder.output.push(unifier.mk_node(
                            ontol_hir::Kind::MatchRegex(
                                ontol_hir::Iter(false),
                                scope_var.0,
                                regex_def_id,
                                match_arms,
                            ),
                            regex_hir_meta,
                        ));
                    }
                }
                other => return Err(unifier_todo(smart_format!("structural scope: {other:?}"))),
            }
        }

        indexes = next_indexes;
    }

    Ok(builder.build(unifier))
}

fn apply_lateral_scope<'m>(
    main_scope: MainScope,
    assignments: Vec<ScopedAssignment<'m>>,
    in_scope_fn: &dyn Fn() -> VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<Vec<ontol_hir::Node>> {
    if assignments.is_empty() {
        return Ok(vec![]);
    }

    let in_scope = in_scope_fn();

    let mut scope_groups: FnvHashMap<ScopeVar, ScopeGroup<'m>> = Default::default();
    let mut ungrouped = vec![];
    let mut nodes = vec![];

    for assignment in assignments {
        if assignment.expr.free_vars().0.is_subset(&in_scope.0) {
            debug!(
                "{level}ungrouped assignment {}",
                assignment.expr.kind().debug_short()
            );
            ungrouped.push(assignment);
        } else {
            let introduced_var = Var(assignment
                .expr
                .free_vars()
                .0
                .difference(&in_scope.0)
                .next()
                .unwrap() as u32);

            if let Some(data_point_index) = table.find_data_point(introduced_var) {
                let scope_map = &mut table.scope_map_mut(data_point_index);
                let scope_group = scope_groups
                    .entry(scope_map.scope.meta().scope_var)
                    .or_default();

                scope_group.introducing.union_with(&[introduced_var].into());
                scope_group.assignments.push(assignment);
            } else {
                debug!("{level} no data point");
                ungrouped.push(assignment);
            }
        }
    }

    let mut needs_regroup_check = true;

    while needs_regroup_check {
        let mut new_scope_groups: FnvHashMap<ScopeVar, ScopeGroup<'m>> = FnvHashMap::default();
        needs_regroup_check = false;

        for (scope_var, scope_group) in scope_groups {
            let (_, scope_map) = table.find_scope_map_by_scope_var(scope_var).unwrap();

            match scope_map.scope.kind() {
                flat_scope::Kind::PropVariant(_, _, struct_var, _) => {
                    let struct_var = *struct_var;
                    if in_scope.contains(struct_var) {
                        new_scope_groups.insert(scope_var, scope_group);
                    } else if let Some(scope_map) = table.find_upstream_data_point(scope_var) {
                        let supergroup = new_scope_groups
                            .entry(scope_map.scope.meta().scope_var)
                            .or_default();

                        supergroup.subgroups.insert(scope_var, scope_group);
                        supergroup.introducing.union_with(&[struct_var].into());

                        needs_regroup_check = true;
                    } else {
                        return Err(UnifierError::NonUniqueVariableDatapoints(
                            [scope_var.0].into(),
                        ));
                    }
                }
                other => return Err(unifier_todo(smart_format!("{other:?}"))),
            }
        }

        scope_groups = new_scope_groups;
    }

    for (scope_var, scope_group) in scope_groups {
        apply_scope_group(
            main_scope,
            scope_var,
            scope_group,
            &in_scope,
            &mut nodes,
            table,
            unifier,
            level,
        )?;
    }

    for assignment in ungrouped {
        nodes.push(
            ScopedExprToNode {
                table,
                unifier,
                scope_var: Some(assignment.scope_var),
                level,
            }
            .scoped_expr_to_node(assignment.expr, &in_scope, main_scope)?,
        );
    }

    Ok(nodes)
}

#[allow(clippy::too_many_arguments)]
fn apply_scope_group<'m>(
    main_scope: MainScope,
    scope_var: ScopeVar,
    scope_group: ScopeGroup<'m>,
    in_scope: &VarSet,
    output: &mut Vec<ontol_hir::Node>,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<()> {
    debug!(
        "{level}apply scope group {scope_var:?} subgroups={}",
        scope_group.subgroups.len()
    );

    let (_, scope_map) = table.find_scope_map_by_scope_var(scope_var).unwrap();
    let scope_var = scope_map.scope.meta().scope_var;

    let mut builder = LevelBuilder::default();

    match scope_map.scope.kind() {
        flat_scope::Kind::PropVariant(_, optional, struct_var, property_id) => {
            let prop_key = (*optional, *struct_var, *property_id);
            // debug!("{level}lateral introducing {introduced_var}");

            let next_in_scope = &in_scope.union(&scope_group.introducing);

            let mut body = vec![];

            for (introduced_var, subgroup) in scope_group.subgroups {
                apply_scope_group(
                    main_scope,
                    introduced_var,
                    subgroup,
                    next_in_scope,
                    &mut body,
                    table,
                    unifier,
                    level.next(),
                )?;
            }

            body.extend(apply_lateral_scope(
                main_scope,
                scope_group.assignments,
                &|| next_in_scope.clone(),
                table,
                unifier,
                level.next(),
            )?);

            let bindings = table.rel_val_bindings(scope_var);
            builder.add_prop_variant_scope(prop_key, bindings, body);
        }
        other => return Err(unifier_todo(smart_format!("{other:?}"))),
    }

    output.extend(builder.build(unifier));

    Ok(())
}

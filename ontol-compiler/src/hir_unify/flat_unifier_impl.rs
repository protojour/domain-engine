use fnv::FnvHashMap;
use ontol_runtime::{
    smart_format,
    var::{Var, VarSet},
};
use tracing::debug;

use crate::{
    hir_unify::{
        expr,
        flat_level_builder::LevelBuilder,
        flat_scope::{self},
        flat_unifier::unifier_todo,
        flat_unifier_regex::unify_regex,
        flat_unifier_table::IsInScope,
    },
    typed_hir::{IntoTypedHirData, UNIT_META},
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

pub(super) fn unify_root<'m>(
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

    debug!("unify root index={index}");

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
                (
                    MainScope::Value(scope_meta.scope_var),
                    ExprSelector::Struct(binder.hir().var, scope_var),
                    Level(0),
                ),
                StructuralOrigin::DependeesOf(scope_var),
                next_in_scope.clone(),
                table,
                unifier,
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
        (
            Some(Assignment {
                expr: expr::Expr(expr::Kind::DestructuredSeq(label), meta),
                ..
            }),
            flat_scope::Kind::Struct,
        ) => {
            let next_in_scope = in_scope.union_one(scope_meta.scope_var.0);
            let body = unify_scope_structural(
                (
                    MainScope::Value(scope_meta.scope_var),
                    ExprSelector::SeqItem(label),
                    Level(0),
                ),
                StructuralOrigin::DependeesOf(scope_var),
                next_in_scope.clone(),
                table,
                unifier,
            )?;

            let node = UnifiedNode {
                typed_binder: Some(
                    ontol_hir::Binder {
                        var: scope_meta.scope_var.0,
                    }
                    .with_meta(scope_meta.hir_meta),
                ),
                node: unifier.mk_node(
                    ontol_hir::Kind::Sequence(
                        ontol_hir::Binder {
                            var: label.0.into(),
                        }
                        .with_meta(scope_meta.hir_meta),
                        body.into(),
                    ),
                    meta.hir_meta,
                ),
            };

            Ok(node)
        }
        other => Err(unifier_todo(smart_format!("Handle pair {other:?}"))),
    }
}

pub(super) fn unify_scope_structural<'m>(
    (main_scope, selector, level): (MainScope, ExprSelector, Level),
    origin: StructuralOrigin,
    in_scope: VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<Vec<ontol_hir::Node>> {
    let mut indexes = match origin {
        StructuralOrigin::DependeesOf(parent_scope_var) => table.dependees(Some(parent_scope_var)),
        StructuralOrigin::Start => vec![0],
    };

    let mut builder = LevelBuilder::default();

    debug!("{level}USS main_scope={main_scope:?} {selector:?} origin={origin:?}");

    while !indexes.is_empty() {
        let mut next_indexes = vec![];

        debug!(
            "{level}indexes={:?}",
            indexes
                .iter()
                .map(|idx| table.scope_map_mut(*idx).scope.meta().scope_var.0)
                .collect::<Vec<_>>()
        );

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
                            (main_scope, selector, level.next()),
                            StructuralOrigin::DependeesOf(scope_var),
                            inner_scope.union(&bindings.var_set()),
                            table,
                            unifier,
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
                            (main_scope, selector, level.next()),
                            StructuralOrigin::DependeesOf(scope_var),
                            inner_scope,
                            table,
                            unifier,
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
                    unify_regex(
                        (main_scope, selector, level),
                        (index, scope_var),
                        (
                            *opt_seq_label,
                            *regex_def_id,
                            scope_map.scope.meta().hir_meta,
                        ),
                        &in_scope,
                        (&mut builder, &mut next_indexes),
                        table,
                        unifier,
                    )?;
                }
                other => return Err(unifier_todo(smart_format!("structural scope: {other:?}"))),
            }
        }

        indexes = next_indexes;
    }

    Ok(builder.build(unifier))
}

pub(super) fn apply_lateral_scope<'m>(
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

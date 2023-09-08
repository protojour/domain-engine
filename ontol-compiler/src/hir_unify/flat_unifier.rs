#![allow(clippy::only_used_in_recursion)]

use std::fmt::Display;

use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    hir_unify::{flat_unifier_table::IsInScope, CLASSIC_UNIFIER_FALLBACK},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{self, TypedBinder, TypedHirNode},
    types::{Type, Types},
    NO_SPAN,
};

use super::{
    dep_tree::Expression,
    expr,
    flat_level_builder::LevelBuilder,
    flat_scope::{self, OutputVar, ScopeVar},
    flat_unifier_table::{Assignment, ExprSelector, ScopedAssignment, Table},
    unifier::UnifiedNode,
    UnifierError, UnifierResult, VarSet,
};

pub struct FlatUnifier<'a, 'm> {
    #[allow(unused)]
    pub(super) types: &'a mut Types<'m>,
    pub(super) var_allocator: ontol_hir::VarAllocator,
}

enum AssignResult<'m> {
    Success,
    Unassigned(expr::Expr<'m>),
}

#[derive(Clone, Copy, Debug)]
enum MainScope {
    Const,
    Value(ScopeVar),
    Sequence(ScopeVar, OutputVar),
}

#[derive(Clone, Copy)]
struct Level(pub u32);

impl Level {
    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for _ in 0..self.0 {
            write!(f, "  ")?;
        }
        Ok(())
    }
}

impl<'a, 'm> FlatUnifier<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>, var_allocator: ontol_hir::VarAllocator) -> Self {
        Self {
            types,
            var_allocator,
        }
    }

    pub(super) fn unify(
        &mut self,
        flat_scope: flat_scope::FlatScope<'m>,
        expr: expr::Expr<'m>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        if false {
            debug!("flat_scope:\n{flat_scope}");
        }

        let mut table = Table::new(flat_scope);

        let result = self.assign_to_scope(expr, &mut table);

        // Debug even if assign_to_scope failed
        for scope_map in table.table_mut() {
            debug!("{}", scope_map.scope);
            for assignment in &scope_map.assignments {
                debug!(
                    "  - {} lateral={:?}",
                    assignment.expr.kind().debug_short(),
                    assignment.lateral_deps
                );
            }
        }

        result?;

        unify_single(None, Default::default(), &mut table, self)
    }

    fn assign_to_scope(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        table: &mut Table<'m>,
    ) -> UnifierResult<AssignResult<'m>> {
        match kind {
            expr::Kind::Var(var) => {
                if let Some(index) = table.find_var_index(ScopeVar(var)) {
                    table.scope_map_mut(index).assignments.push(Assignment {
                        expr: expr::Expr(kind, meta),
                        lateral_deps: Default::default(),
                    });
                } else {
                    table.const_expr = Some(expr::Expr(kind, meta));
                }
                Ok(AssignResult::Success)
            }
            kind @ expr::Kind::Struct { .. } => {
                let expr = self.destructure_expr(expr::Expr(kind, meta), table)?;

                // BUG: Not 0?
                table.scope_map_mut(0).assignments.push(Assignment {
                    expr,
                    lateral_deps: Default::default(),
                });

                Ok(AssignResult::Success)
            }
            expr::Kind::Prop(prop) => {
                // debug!(
                //     "assigning prop {:?}, free_vars={:?}",
                //     prop.prop_id, prop.free_vars
                // );
                // debug!("prop: {prop:?}");

                let prop = {
                    let expr::Prop {
                        optional,
                        struct_var,
                        prop_id,
                        seq,
                        variant,
                        free_vars,
                    } = *prop;
                    let variant = match variant {
                        expr::PropVariant::Seq { label, elements } => {
                            // recursively assign seq elements
                            for (index, element) in elements.into_iter().enumerate() {
                                let mut free_vars = VarSet::default();
                                free_vars.union_with(&element.attribute.rel.meta().free_vars);
                                free_vars.union_with(&element.attribute.val.meta().free_vars);

                                let element_expr = expr::Expr(
                                    expr::Kind::SeqItem(
                                        label,
                                        index,
                                        expr::Iter(element.iter),
                                        Box::new(element.attribute),
                                    ),
                                    expr::Meta {
                                        free_vars,
                                        hir_meta: self.unit_meta(),
                                    },
                                );

                                self.assign_to_scope(element_expr, table)?;

                                if element.iter {}

                                // element.attribute
                            }
                            expr::PropVariant::Seq {
                                label,
                                elements: vec![],
                            }
                        }
                        expr::PropVariant::Singleton(attr) => {
                            let rel = self.destructure_expr(attr.rel, table)?;
                            let val = self.destructure_expr(attr.val, table)?;
                            // let rel = attr.rel;
                            // let val = attr.val;
                            expr::PropVariant::Singleton(ontol_hir::Attribute { rel, val })
                        }
                    };
                    expr::Prop {
                        optional,
                        struct_var,
                        prop_id,
                        variant,
                        seq,
                        free_vars,
                    }
                };

                let is_seq = matches!(&prop.variant, expr::PropVariant::Seq { .. });

                match table.find_assignment_slot(&prop.free_vars, is_seq) {
                    Some(assignment_slot) => {
                        assignment_slot.scope_map.assignments.push(Assignment {
                            expr: expr::Expr(expr::Kind::Prop(Box::new(prop)), meta),
                            lateral_deps: assignment_slot.lateral_deps,
                        });
                        Ok(AssignResult::Success)
                    }
                    None => Ok(AssignResult::Unassigned(expr::Expr(
                        expr::Kind::Prop(Box::new(prop)),
                        meta,
                    ))),
                }
            }
            expr::Kind::SeqItem(label, index, iter, attr) => {
                if iter.0 {
                    let iter_scope_map_idx = table
                        .dependees(Some(ScopeVar(ontol_hir::Var(label.0))))
                        .into_iter()
                        .find(|idx| {
                            let scope_map = &table.table_mut()[*idx];
                            matches!(scope_map.scope.kind(), flat_scope::Kind::IterElement(..))
                        });

                    if let Some(iter_scope_map_idx) = iter_scope_map_idx {
                        let rel = self.destructure_expr(attr.rel, table)?;
                        let val = self.destructure_expr(attr.val, table)?;

                        let iter_scope_map = &mut table.table_mut()[iter_scope_map_idx];

                        iter_scope_map.assignments.push(Assignment {
                            expr: expr::Expr(
                                expr::Kind::SeqItem(
                                    label,
                                    index,
                                    iter,
                                    Box::new(ontol_hir::Attribute { rel, val }),
                                ),
                                meta,
                            ),
                            lateral_deps: VarSet::default(),
                        });

                        Ok(AssignResult::Success)
                    } else {
                        Err(unifier_todo(smart_format!("Not able to find iter scope")))
                    }
                } else {
                    Err(unifier_todo(smart_format!("Handle non-iter seq element")))
                }
            }
            e => Err(unifier_todo(smart_format!("expr kind: {e:?}"))),
        }
    }

    fn destructure_expr(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        table: &mut Table<'m>,
    ) -> UnifierResult<expr::Expr<'m>> {
        match kind {
            expr::Kind::Struct {
                binder,
                flags,
                props,
            } => {
                let mut retained_props: Vec<expr::Prop> = vec![];
                for prop in props {
                    let free_vars = prop.free_vars.clone();
                    let unit_ty = self
                        .types
                        .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()));
                    let assign_result = self.assign_to_scope(
                        expr::Expr(
                            expr::Kind::Prop(Box::new(prop)),
                            expr::Meta {
                                free_vars,
                                hir_meta: typed_hir::Meta {
                                    ty: unit_ty,
                                    span: NO_SPAN,
                                },
                            },
                        ),
                        table,
                    )?;
                    if let AssignResult::Unassigned(expr::Expr(kind, _)) = assign_result {
                        let expr::Kind::Prop(prop) = kind else {
                            panic!();
                        };
                        retained_props.push(*prop);
                    }
                }

                Ok(expr::Expr(
                    expr::Kind::Struct {
                        binder,
                        flags,
                        props: retained_props,
                    },
                    meta,
                ))
            }
            kind => Ok(expr::Expr(kind, meta)),
        }
    }

    pub fn unit_meta(&mut self) -> typed_hir::Meta<'m> {
        typed_hir::Meta {
            ty: self.types.unit_type(),
            span: NO_SPAN,
        }
    }
}

fn unify_single<'m>(
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
            let typed_binder = Some(TypedBinder {
                var: scope_map.scope.meta().scope_var.0,
                meta: scope_map.scope.meta().hir_meta,
            });

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
            let mut body = unify_scope_structural(
                MainScope::Value(scope_meta.scope_var),
                ExprSelector::Struct(binder.var, scope_var),
                StructuralOrigin::DependeesOf(scope_var),
                next_in_scope.clone(),
                table,
                unifier,
                Level(0),
            )?;

            for prop in props {
                let free_vars = prop.free_vars.clone();
                let unit_meta = unifier.unit_meta();
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
                            hir_meta: unit_meta,
                        },
                    ),
                    &next_in_scope,
                    MainScope::Value(scope_var),
                )?;

                body.push(prop_node);
            }

            let node = UnifiedNode {
                typed_binder: Some(TypedBinder {
                    var: scope_meta.scope_var.0,
                    meta: scope_meta.hir_meta,
                }),
                node: TypedHirNode(ontol_hir::Kind::Struct(binder, flags, body), meta.hir_meta),
            };

            Ok(node)
        }
        other => Err(unifier_todo(smart_format!("Handle pair {other:?}"))),
    }
}

#[derive(Clone, Copy, Debug)]
enum StructuralOrigin {
    DependeesOf(ScopeVar),
    Start,
}

fn unify_scope_structural<'m>(
    main_scope: MainScope,
    selector: ExprSelector,
    origin: StructuralOrigin,
    in_scope: VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<Vec<TypedHirNode<'m>>> {
    let mut indexes = match origin {
        StructuralOrigin::DependeesOf(parent_scope_var) => table.dependees(Some(parent_scope_var)),
        StructuralOrigin::Start => vec![0],
    };

    let mut builder = LevelBuilder::default();

    debug!("{level}USS main_scope={main_scope:?} {selector:?} origin={origin:?}");

    while !indexes.is_empty() {
        let mut next_indexes = vec![];

        debug!("{level}  indexes={indexes:?}");

        for index in indexes {
            let scope_map = &mut table.scope_map_mut(index);
            let scope_var = scope_map.scope.meta().scope_var;

            match scope_map.scope.kind() {
                flat_scope::Kind::PropRelParam
                | flat_scope::Kind::PropValue
                | flat_scope::Kind::Struct
                | flat_scope::Kind::Var => {
                    builder.output.extend(apply_lateral_scope(
                        main_scope,
                        scope_map.take_assignments(),
                        &|| in_scope.clone(),
                        table,
                        unifier,
                        level,
                    )?);

                    next_indexes.extend(table.dependees(Some(scope_var)));
                }
                flat_scope::Kind::PropVariant(optional, prop_struct_var, property_id) => {
                    let property_id = *property_id;
                    let prop_key = (*optional, *prop_struct_var, property_id);
                    let mut body = vec![];
                    let inner_scope = in_scope.union(&scope_map.scope.meta().pub_vars);

                    let assignments = scope_map.select_assignments(selector);
                    let assignments_len = assignments.len();

                    body.extend(apply_lateral_scope(
                        main_scope,
                        assignments,
                        &|| inner_scope.clone(),
                        table,
                        unifier,
                        level,
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
                    let inner_scope = in_scope.union(&scope_map.scope.meta().pub_vars);

                    body.extend(apply_lateral_scope(
                        main_scope,
                        scope_map.select_assignments(selector),
                        &|| inner_scope.clone(),
                        table,
                        unifier,
                        level,
                    )?);

                    if !in_scope.contains(ontol_hir::Var(label.label.0)) {
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
                                    next_in_scope.insert(binder.var);
                                }
                                if let ontol_hir::Binding::Binder(binder) = &bindings.val {
                                    next_in_scope.insert(binder.var);
                                }
                                next_in_scope
                            },
                            table,
                            unifier,
                            level,
                        )?;

                        if !push_nodes.is_empty() {
                            builder.output.push(TypedHirNode(
                                ontol_hir::Kind::ForEach(
                                    ontol_hir::Var(label.0),
                                    (bindings.rel, bindings.val),
                                    push_nodes,
                                ),
                                unifier.unit_meta(),
                            ));
                        }
                    }
                }
                other => return Err(unifier_todo(smart_format!("structural scope: {other:?}"))),
            }
        }

        indexes = next_indexes;
    }

    Ok(builder.build(unifier))
}

#[derive(Default)]
struct ScopeGroup<'m> {
    assignments: Vec<ScopedAssignment<'m>>,
    introducing: VarSet,
    subgroups: FnvHashMap<ScopeVar, ScopeGroup<'m>>,
}

fn apply_lateral_scope<'m>(
    main_scope: MainScope,
    assignments: Vec<ScopedAssignment<'m>>,
    in_scope_fn: &dyn Fn() -> VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<Vec<TypedHirNode<'m>>> {
    if assignments.is_empty() {
        return Ok(vec![]);
    }

    let in_scope = in_scope_fn();

    let mut scope_groups: FnvHashMap<ScopeVar, ScopeGroup<'m>> = Default::default();
    let mut ungrouped = vec![];
    let mut nodes = vec![];

    for assignment in assignments {
        if assignment.expr.free_vars().0.is_subset(&in_scope.0) {
            nodes.push(
                ScopedExprToNode {
                    table,
                    unifier,
                    scope_var: Some(assignment.scope_var),
                    level,
                }
                .scoped_expr_to_node(assignment.expr, &in_scope, main_scope)?,
            );
        } else {
            let introduced_var = ontol_hir::Var(
                assignment
                    .expr
                    .free_vars()
                    .0
                    .difference(&in_scope.0)
                    .next()
                    .unwrap() as u32,
            );

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
            let scope_map = &mut table.find_scope_map_by_scope_var(scope_var).unwrap();

            match scope_map.scope.kind() {
                flat_scope::Kind::PropVariant(_, struct_var, _) => {
                    let struct_var = *struct_var;
                    if in_scope.contains(struct_var) {
                        new_scope_groups.insert(scope_var, scope_group);
                    } else {
                        if let Some(scope_map) = table.find_upstream_data_point(scope_var) {
                            let supergroup = new_scope_groups
                                .entry(scope_map.scope.meta().scope_var)
                                .or_default();

                            supergroup.subgroups.insert(scope_var, scope_group);
                            supergroup.introducing.union_with(&[struct_var].into());

                            needs_regroup_check = true;
                        } else {
                            unreachable!()
                        }
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

fn apply_scope_group<'m>(
    main_scope: MainScope,
    scope_var: ScopeVar,
    scope_group: ScopeGroup<'m>,
    in_scope: &VarSet,
    output: &mut Vec<TypedHirNode<'m>>,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<()> {
    debug!(
        "{level}apply scope group {scope_var:?} subgroups={}",
        scope_group.subgroups.len()
    );

    let scope_map = table.find_scope_map_by_scope_var(scope_var).unwrap();
    let scope_var = scope_map.scope.meta().scope_var;

    let mut builder = LevelBuilder::default();

    match scope_map.scope.kind() {
        flat_scope::Kind::PropVariant(optional, struct_var, property_id) => {
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

struct ScopedExprToNode<'t, 'u, 'a, 'm> {
    table: &'t mut Table<'m>,
    unifier: &'u mut FlatUnifier<'a, 'm>,
    scope_var: Option<ScopeVar>,
    level: Level,
}

impl<'t, 'u, 'a, 'm> ScopedExprToNode<'t, 'u, 'a, 'm> {
    /// Convert an expression that has all its exposed free variables
    /// in scope, to a HIR node.
    fn scoped_expr_to_node(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        in_scope: &VarSet,
        main_scope: MainScope,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let next_main_scope = match main_scope {
            MainScope::Const => MainScope::Const,
            MainScope::Value(scope_var) => MainScope::Value(scope_var),
            MainScope::Sequence(scope_var, _) => MainScope::Value(scope_var),
        };

        match kind {
            expr::Kind::Var(var) => Ok(TypedHirNode(ontol_hir::Kind::Var(var), meta.hir_meta)),
            expr::Kind::Unit => Ok(TypedHirNode(ontol_hir::Kind::Unit, meta.hir_meta)),
            expr::Kind::I64(int) => Ok(TypedHirNode(ontol_hir::Kind::I64(int), meta.hir_meta)),
            expr::Kind::F64(float) => Ok(TypedHirNode(ontol_hir::Kind::F64(float), meta.hir_meta)),
            expr::Kind::Prop(prop) => Ok(TypedHirNode(
                ontol_hir::Kind::Prop(
                    ontol_hir::Optional(false),
                    prop.struct_var,
                    prop.prop_id,
                    match prop.variant {
                        expr::PropVariant::Singleton(attr) => {
                            vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel: Box::new(self.scoped_expr_to_node(
                                    attr.rel,
                                    in_scope,
                                    next_main_scope,
                                )?),
                                val: Box::new(self.scoped_expr_to_node(
                                    attr.val,
                                    in_scope,
                                    next_main_scope,
                                )?),
                            })]
                        }
                        expr::PropVariant::Seq { label, elements } => {
                            assert!(elements.is_empty());
                            let sequence_node = find_and_unify_sequence(
                                prop.struct_var,
                                label,
                                in_scope,
                                self.table,
                                self.unifier,
                                self.level.next(),
                            )?;

                            vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel: Box::new(TypedHirNode(
                                    ontol_hir::Kind::Unit,
                                    self.unifier.unit_meta(),
                                )),
                                val: Box::new(sequence_node),
                            })]
                        }
                    },
                ),
                meta.hir_meta,
            )),
            expr::Kind::Call(expr::Call(proc, args)) => {
                let mut hir_args = Vec::with_capacity(args.len());
                for arg in args {
                    hir_args.push(self.scoped_expr_to_node(arg, in_scope, next_main_scope)?);
                }
                Ok(TypedHirNode(
                    ontol_hir::Kind::Call(proc, hir_args),
                    meta.hir_meta,
                ))
            }
            expr::Kind::Map(arg) => {
                let hir_arg = self.scoped_expr_to_node(*arg, in_scope, next_main_scope)?;
                Ok(TypedHirNode(
                    ontol_hir::Kind::Map(Box::new(hir_arg)),
                    meta.hir_meta,
                ))
            }
            expr::Kind::Struct {
                binder,
                flags,
                props,
            } => {
                let level = self.level;
                let next_in_scope = in_scope.union_one(binder.var);
                let mut body = vec![];

                debug!(
                    "{level}Make struct {} scope_var={:?} in_scope={:?}",
                    binder.var, self.scope_var, in_scope
                );

                if let Some(_) = self.scope_var {
                    body.extend(unify_scope_structural(
                        match main_scope {
                            MainScope::Const => unreachable!(),
                            MainScope::Value(scope_var) => MainScope::Value(scope_var),
                            MainScope::Sequence(scope_var, _) => MainScope::Value(scope_var),
                        },
                        match main_scope {
                            MainScope::Const => unreachable!(),
                            MainScope::Value(scope_var) => {
                                ExprSelector::Struct(binder.var, scope_var)
                            }
                            MainScope::Sequence(scope_var, _) => {
                                ExprSelector::Struct(binder.var, scope_var)
                            }
                        },
                        StructuralOrigin::Start,
                        next_in_scope.clone(),
                        self.table,
                        self.unifier,
                        self.level.next(),
                    )?);
                }

                // let mut body = Vec::with_capacity(props.len());
                for prop in props {
                    match prop.variant {
                        expr::PropVariant::Singleton(attr) => {
                            let rel = Box::new(self.scoped_expr_to_node(
                                attr.rel,
                                in_scope,
                                next_main_scope,
                            )?);
                            let val = Box::new(self.scoped_expr_to_node(
                                attr.val,
                                in_scope,
                                next_main_scope,
                            )?);
                            let unit_meta = self.unifier.unit_meta();
                            body.push(TypedHirNode(
                                ontol_hir::Kind::Prop(
                                    ontol_hir::Optional(false),
                                    prop.struct_var,
                                    prop.prop_id,
                                    vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                        rel,
                                        val,
                                    })],
                                ),
                                unit_meta,
                            ));
                        }
                        expr::PropVariant::Seq { .. } => {
                            return Err(unifier_todo(smart_format!("seq prop")))
                        }
                    }
                }
                Ok(TypedHirNode(
                    ontol_hir::Kind::Struct(binder, flags, body),
                    meta.hir_meta,
                ))
            }
            expr::Kind::SeqItem(_label, _index, _iter, attr) => {
                let MainScope::Sequence(_, output_var) = main_scope else {
                    panic!("Unsupported context for seq-item");
                };

                let rel =
                    Box::new(self.scoped_expr_to_node(attr.rel, in_scope, next_main_scope)?);
                let val =
                    Box::new(self.scoped_expr_to_node(attr.val, in_scope, next_main_scope)?);

                Ok(TypedHirNode(
                    ontol_hir::Kind::SeqPush(output_var.0, ontol_hir::Attribute { rel, val }),
                    self.unifier.unit_meta(),
                ))
            }
            other => Err(unifier_todo(smart_format!("leaf expr to node: {other:?}"))),
        }
    }
}

fn find_and_unify_sequence<'m>(
    struct_var: ontol_hir::Var,
    label: ontol_hir::Label,
    in_scope: &VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<TypedHirNode<'m>> {
    let output_seq_var = table
        .find_scope_map_by_scope_var(ScopeVar(ontol_hir::Var(label.0)))
        .and_then(|scope_map| match scope_map.scope.kind() {
            flat_scope::Kind::SeqPropVariant(_, output_var, _, _, _, _) => Some(*output_var),
            _ => None,
        })
        .unwrap();

    let next_in_scope = in_scope.union_one(output_seq_var.0);

    let scope_var = ScopeVar(ontol_hir::Var(label.0));

    let sequence_body = unify_scope_structural(
        MainScope::Value(scope_var),
        ExprSelector::Struct(struct_var, scope_var),
        StructuralOrigin::DependeesOf(scope_var),
        next_in_scope,
        table,
        unifier,
        level.next(),
    )?;

    let unit_ty = unifier.unit_meta().ty;

    let seq_ty = unifier.types.intern(Type::Seq(
        unit_ty,
        unit_ty,
        // first_element.attribute.rel.hir_meta().ty,
        // first_element.attribute.val.hir_meta().ty,
    ));

    let sequence_node = TypedHirNode(
        ontol_hir::Kind::Sequence(
            TypedBinder {
                var: output_seq_var.0,
                meta: typed_hir::Meta {
                    ty: seq_ty,
                    span: NO_SPAN,
                },
            },
            sequence_body,
        ),
        typed_hir::Meta {
            ty: seq_ty,
            span: NO_SPAN,
        },
    );

    Ok(sequence_node)
}

pub(super) fn unifier_todo(msg: String) -> UnifierError {
    if !CLASSIC_UNIFIER_FALLBACK {
        todo!("{msg}");
    }
    UnifierError::TODO(msg)
}

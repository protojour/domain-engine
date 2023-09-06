#![allow(clippy::only_used_in_recursion)]

use indexmap::IndexMap;
use ontol_runtime::{smart_format, value::PropertyId, DefId};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    hir_unify::CLASSIC_UNIFIER_FALLBACK,
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{self, TypedBinder, TypedHir, TypedHirNode},
    types::{Type, Types},
    NO_SPAN,
};

use super::{
    dep_tree::Expression,
    expr, flat_scope,
    flat_unifier_table::{Assignment, Table},
    unifier::UnifiedNode,
    UnifierError, UnifierResult, VarSet,
};

pub struct FlatUnifier<'a, 'm> {
    #[allow(unused)]
    pub(super) types: &'a mut Types<'m>,
    pub(super) var_allocator: ontol_hir::VarAllocator,
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
        if true {
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

        unify_single(None, Default::default(), &mut table, self.types)
    }

    fn assign_to_scope(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        table: &mut Table<'m>,
    ) -> UnifierResult<()> {
        match kind {
            expr::Kind::Var(var) => {
                if let Some(index) = table.find_var_index(var) {
                    table.scope_map_mut(index).assignments.push(Assignment {
                        expr: expr::Expr(kind, meta),
                        lateral_deps: Default::default(),
                    });
                } else {
                    table.const_expr = Some(expr::Expr(kind, meta));
                }
            }
            expr::Kind::Struct {
                binder,
                flags,
                props,
            } => {
                // FIXME: Insert according to required scope, instead of scope 0.
                table.scope_map_mut(0).assignments.push(Assignment {
                    expr: expr::Expr(
                        expr::Kind::Struct {
                            binder,
                            flags,
                            props: vec![],
                        },
                        meta.clone(),
                    ),
                    lateral_deps: Default::default(),
                });

                for prop in props {
                    let free_vars = prop.free_vars.clone();
                    let unit_ty = self
                        .types
                        .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()));
                    self.assign_to_scope(
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
                }
            }
            expr::Kind::Prop(prop) => {
                debug!(
                    "assigning prop {:?}, free_vars={:?}",
                    prop.prop_id, prop.free_vars
                );
                let assignment_slot = table.find_assignment_slot(&prop.free_vars);
                let assignments = &mut assignment_slot.scope_map.assignments;

                let struct_var = prop.struct_var;
                let mut prop = Some(*prop);

                // Check just adding back to the original struct that owned this property:
                for assignment in assignments.iter_mut() {
                    if let expr::Kind::Struct { binder, props, .. } = &mut assignment.expr.0 {
                        if binder.var == struct_var {
                            props.push(prop.take().unwrap());
                            break;
                        }
                    }
                }

                if let Some(prop) = prop {
                    assignments.push(Assignment {
                        expr: expr::Expr(expr::Kind::Prop(Box::new(prop)), meta),
                        lateral_deps: assignment_slot.lateral_deps,
                    });
                }
            }
            e => return Err(unifier_todo(smart_format!("expr kind: {e:?}"))),
        }

        Ok(())
    }
}

fn unify_single<'m>(
    parent_scope_var: Option<ontol_hir::Var>,
    in_scope: VarSet,
    table: &mut Table<'m>,
    types: &mut Types<'m>,
) -> UnifierResult<UnifiedNode<'m>> {
    if let Some(const_expr) = table.const_expr.take() {
        return Ok(UnifiedNode {
            typed_binder: None,
            node: leaf_expr_to_node(const_expr, types)?,
        });
    }

    if let Some(var) = parent_scope_var {
        if in_scope.contains(var) {
            panic!("Variable is already in scope");
        }
    }

    let indexes = table.dependees(parent_scope_var);
    let Some(index) = indexes.into_iter().next() else {
        panic!("multiple indexes");
    };

    let scope_map = &mut table.scope_map_mut(index);
    let scope_meta = scope_map.scope.meta().clone();

    match (scope_map.take_single_assignment(), scope_map.scope.kind()) {
        (
            Some(Assignment {
                expr: expr::Expr(kind, meta),
                ..
            }),
            flat_scope::Kind::Var,
        ) => {
            let inner_node = leaf_expr_to_node(expr::Expr(kind, meta), types)?;

            Ok(UnifiedNode {
                typed_binder: Some(TypedBinder {
                    var: scope_map.scope.meta().var,
                    meta: scope_map.scope.meta().hir_meta,
                }),
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
                            props: _,
                        },
                        meta,
                    ),
                ..
            }),
            flat_scope::Kind::Struct,
        ) => {
            let next_in_scope = in_scope.union_one(scope_meta.var);
            let body = unify_scope_structural(scope_meta.var, next_in_scope, table, types)?;

            let node = UnifiedNode {
                typed_binder: Some(TypedBinder {
                    var: scope_meta.var,
                    meta: scope_meta.hir_meta,
                }),
                node: TypedHirNode(ontol_hir::Kind::Struct(binder, flags, body), meta.hir_meta),
            };

            Ok(node)
        }
        other => Err(unifier_todo(smart_format!("Handle pair {other:?}"))),
    }
}

fn unify_scope_structural<'m>(
    parent_scope_var: ontol_hir::Var,
    in_scope: VarSet,
    table: &mut Table<'m>,
    types: &mut Types<'m>,
) -> UnifierResult<Vec<TypedHirNode<'m>>> {
    let indexes = table.dependees(Some(parent_scope_var));

    let mut builder = LevelBuilder::default();

    for index in indexes {
        let scope_map = &mut table.scope_map_mut(index);
        let scope_var = scope_map.scope.meta().var;

        match scope_map.scope.kind() {
            flat_scope::Kind::PropRelParam
            | flat_scope::Kind::PropValue
            | flat_scope::Kind::Struct
            | flat_scope::Kind::Var => {
                builder.output.extend(apply_lateral_scope(
                    std::mem::take(&mut scope_map.assignments),
                    || in_scope.clone(),
                    table,
                    types,
                )?);

                let nodes =
                    unify_scope_structural(scope_var, in_scope.union_one(scope_var), table, types)?;
                builder.output.extend(nodes);
            }
            flat_scope::Kind::PropVariant(optional, struct_var, property_id) => {
                let prop_key = (*optional, *struct_var, *property_id);
                let mut body = vec![];
                let inner_scope = in_scope.union(&scope_map.scope.meta().pub_vars);

                body.extend(apply_lateral_scope(
                    std::mem::take(&mut scope_map.assignments),
                    || inner_scope.clone(),
                    table,
                    types,
                )?);

                body.extend(unify_scope_structural(
                    scope_var,
                    inner_scope,
                    table,
                    types,
                )?);

                builder.add_prop_variant_scope(scope_var, prop_key, body, table);
            }
            other => return Err(unifier_todo(smart_format!("{other:?}"))),
        }
    }

    Ok(builder.build(types))
}

fn apply_lateral_scope<'m>(
    assignments: Vec<Assignment<'m>>,
    in_scope_fn: impl FnOnce() -> VarSet,
    table: &mut Table<'m>,
    types: &mut Types<'m>,
) -> UnifierResult<Vec<TypedHirNode<'m>>> {
    if assignments.is_empty() {
        return Ok(vec![]);
    }

    let in_scope = in_scope_fn();

    for assignment in &assignments {
        debug!(
            "assignment free_vars: {:?} in_scope: {:?}",
            assignment.expr.meta().free_vars,
            in_scope
        );

        if !assignment.expr.free_vars().0.is_subset(&in_scope.0) {
            panic!("Not in scope: {}", assignment.expr.kind().debug_short());
        }
    }

    let mut nodes = Vec::with_capacity(assignments.len());

    for assignment in assignments {
        nodes.push(leaf_expr_to_node(assignment.expr, types)?);
    }

    Ok(nodes)
}

fn leaf_expr_to_node<'m>(
    expr::Expr(kind, meta): expr::Expr<'m>,
    types: &mut Types<'m>,
) -> UnifierResult<TypedHirNode<'m>> {
    match kind {
        expr::Kind::Unit => Ok(TypedHirNode(ontol_hir::Kind::Unit, meta.hir_meta)),
        expr::Kind::Var(var) => Ok(TypedHirNode(ontol_hir::Kind::Var(var), meta.hir_meta)),
        expr::Kind::Prop(prop) => Ok(TypedHirNode(
            ontol_hir::Kind::Prop(
                ontol_hir::Optional(false),
                prop.struct_var,
                prop.prop_id,
                match prop.variant {
                    expr::PropVariant::Singleton(attr) => {
                        vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                            rel: Box::new(leaf_expr_to_node(attr.rel, types)?),
                            val: Box::new(leaf_expr_to_node(attr.val, types)?),
                        })]
                    }
                    expr::PropVariant::Seq { .. } => todo!("seq"),
                },
            ),
            meta.hir_meta,
        )),
        expr::Kind::Call(expr::Call(proc, args)) => {
            let mut hir_args = Vec::with_capacity(args.len());
            for arg in args {
                hir_args.push(leaf_expr_to_node(arg, types)?);
            }
            Ok(TypedHirNode(
                ontol_hir::Kind::Call(proc, hir_args),
                meta.hir_meta,
            ))
        }
        expr::Kind::Map(arg) => {
            let hir_arg = leaf_expr_to_node(*arg, types)?;
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
            let mut hir_props = Vec::with_capacity(props.len());
            for prop in props {
                match prop.variant {
                    expr::PropVariant::Singleton(attr) => {
                        let rel = Box::new(leaf_expr_to_node(attr.rel, types)?);
                        let val = Box::new(leaf_expr_to_node(attr.val, types)?);
                        let unit_meta = unit_meta(types);
                        hir_props.push(TypedHirNode(
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
                ontol_hir::Kind::Struct(binder, flags, hir_props),
                meta.hir_meta,
            ))
        }
        other => Err(unifier_todo(smart_format!("leaf expr to node: {other:?}"))),
    }
}

#[derive(Default)]
struct MergedMatchArms<'m> {
    optional: ontol_hir::Optional,
    match_arms: Vec<ontol_hir::PropMatchArm<'m, TypedHir>>,
}

#[derive(Default)]
struct LevelBuilder<'m> {
    output: Vec<TypedHirNode<'m>>,
    merged_match_arms_table: IndexMap<(ontol_hir::Var, PropertyId), MergedMatchArms<'m>>,
}

impl<'m> LevelBuilder<'m> {
    fn build(mut self, types: &mut Types<'m>) -> Vec<TypedHirNode<'m>> {
        for ((struct_var, property_id), mut merged_match_arms) in self.merged_match_arms_table {
            if merged_match_arms.optional.0 {
                merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                    pattern: ontol_hir::PropPattern::Absent,
                    nodes: vec![],
                });
            }

            let meta = unit_meta(types);
            self.output.push(TypedHirNode(
                ontol_hir::Kind::MatchProp(struct_var, property_id, merged_match_arms.match_arms),
                meta,
            ));
        }

        self.output
    }

    fn add_prop_variant_scope(
        &mut self,
        scope_var: ontol_hir::Var,
        (optional, struct_var, property_id): (ontol_hir::Optional, ontol_hir::Var, PropertyId),
        body: Vec<TypedHirNode<'m>>,
        table: &mut Table<'m>,
    ) {
        let var_attribute = table.scope_prop_variant_bindings(scope_var);

        fn make_binding<'m>(
            scope_node: Option<&flat_scope::ScopeNode<'m>>,
        ) -> ontol_hir::Binding<'m, TypedHir> {
            match scope_node {
                Some(scope_node) => ontol_hir::Binding::Binder(TypedBinder {
                    var: scope_node.meta().var,
                    meta: scope_node.meta().hir_meta,
                }),
                None => ontol_hir::Binding::Wildcard,
            }
        }

        let rel_binding = make_binding(
            var_attribute
                .rel
                .and_then(|var| table.find_scope_var_child(var)),
        );
        let val_binding = make_binding(
            var_attribute
                .val
                .and_then(|var| table.find_scope_var_child(var)),
        );

        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
            pattern: ontol_hir::PropPattern::Attr(rel_binding, val_binding),
            nodes: body,
        });
    }
}

fn unit_meta<'m>(types: &mut Types<'m>) -> typed_hir::Meta<'m> {
    typed_hir::Meta {
        ty: types.unit_type(),
        span: NO_SPAN,
    }
}

pub(super) fn unifier_todo(msg: String) -> UnifierError {
    if !CLASSIC_UNIFIER_FALLBACK {
        todo!("{msg}");
    }
    UnifierError::TODO(msg)
}

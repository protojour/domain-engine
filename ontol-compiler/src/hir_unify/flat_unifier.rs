#![allow(clippy::only_used_in_recursion)]

use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    hir_unify::CLASSIC_UNIFIER_FALLBACK,
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
    flat_scope::{self, OutputVar},
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

        unify_single(None, Default::default(), &mut table, self)
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
            kind @ expr::Kind::Struct { .. } => {
                let expr = self.destructure_expr(expr::Expr(kind, meta), table)?;
                table.scope_map_mut(0).assignments.push(Assignment {
                    expr,
                    lateral_deps: Default::default(),
                });
            }
            expr::Kind::Prop(prop) => {
                // debug!(
                //     "assigning prop {:?}, free_vars={:?}",
                //     prop.prop_id, prop.free_vars
                // );
                // debug!("prop: {prop:?}");
                let struct_var = prop.struct_var;

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
                        singleton => singleton,
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

                let assignment_slot = table.find_assignment_slot(&prop.free_vars);
                let assignments = &mut assignment_slot.scope_map.assignments;

                let mut prop = Some(prop);

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
            expr::Kind::SeqItem(label, index, iter, attr) => {
                if iter.0 {
                    let iter_scope_map_idx = table
                        .dependees(Some(ontol_hir::Var(label.0)))
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
                    } else {
                        return Err(unifier_todo(smart_format!("Not able to find iter scope")));
                    }
                } else {
                    return Err(unifier_todo(smart_format!("Handle non-iter seq element")));
                }
            }
            e => return Err(unifier_todo(smart_format!("expr kind: {e:?}"))),
        }

        Ok(())
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

                Ok(expr::Expr(
                    expr::Kind::Struct {
                        binder,
                        flags,
                        props: vec![],
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
    parent_scope_var: Option<ontol_hir::Var>,
    in_scope: VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<UnifiedNode<'m>> {
    if let Some(const_expr) = table.const_expr.take() {
        return Ok(UnifiedNode {
            typed_binder: None,
            node: scoped_expr_to_node(const_expr, &in_scope, ExprContext::Value, table, unifier)?,
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
            let typed_binder = Some(TypedBinder {
                var: scope_map.scope.meta().var,
                meta: scope_map.scope.meta().hir_meta,
            });

            let inner_node = scoped_expr_to_node(
                expr::Expr(kind, meta),
                &in_scope,
                ExprContext::Value,
                table,
                unifier,
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
                            props: _,
                        },
                        meta,
                    ),
                ..
            }),
            flat_scope::Kind::Struct,
        ) => {
            let next_in_scope = in_scope.union_one(scope_meta.var);
            let body = unify_scope_structural(scope_meta.var, next_in_scope, table, unifier)?;

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
    unifier: &mut FlatUnifier<'_, 'm>,
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
                    &|| in_scope.clone(),
                    ExprContext::Value,
                    table,
                    unifier,
                )?);

                let nodes = unify_scope_structural(
                    scope_var,
                    in_scope.union_one(scope_var),
                    table,
                    unifier,
                )?;
                builder.output.extend(nodes);
            }
            flat_scope::Kind::PropVariant(optional, struct_var, property_id) => {
                let prop_key = (*optional, *struct_var, *property_id);
                let mut body = vec![];
                let inner_scope = in_scope.union(&scope_map.scope.meta().pub_vars);

                body.extend(apply_lateral_scope(
                    std::mem::take(&mut scope_map.assignments),
                    &|| inner_scope.clone(),
                    ExprContext::Value,
                    table,
                    unifier,
                )?);

                body.extend(unify_scope_structural(
                    scope_var,
                    inner_scope,
                    table,
                    unifier,
                )?);

                builder.add_prop_variant_scope(scope_var, prop_key, body, table);
            }
            flat_scope::Kind::SeqPropVariant(
                _label,
                _output_var,
                optional,
                struct_var,
                property_id,
            ) => {
                let prop_key = (*optional, *struct_var, *property_id);
                let mut body = vec![];
                let inner_scope = in_scope.union(&scope_map.scope.meta().pub_vars);

                body.extend(apply_lateral_scope(
                    std::mem::take(&mut scope_map.assignments),
                    &|| inner_scope.clone(),
                    ExprContext::Value,
                    table,
                    unifier,
                )?);

                body.extend(unify_scope_structural(
                    scope_var,
                    inner_scope,
                    table,
                    unifier,
                )?);

                builder.add_seq_prop_variant_scope(scope_var, prop_key, body, table);
            }
            flat_scope::Kind::IterElement(label, output_var) => {
                let label = *label;
                let push_nodes = apply_lateral_scope(
                    std::mem::take(&mut scope_map.assignments),
                    &|| in_scope.clone(),
                    ExprContext::Sequence(*output_var),
                    table,
                    unifier,
                )?;

                if !push_nodes.is_empty() {
                    builder.output.push(TypedHirNode(
                        ontol_hir::Kind::ForEach(
                            ontol_hir::Var(label.0),
                            table.rel_val_bindings(scope_var),
                            push_nodes,
                        ),
                        unifier.unit_meta(),
                    ));
                }
            }
            other => return Err(unifier_todo(smart_format!("structural scope: {other:?}"))),
        }
    }

    Ok(builder.build(unifier))
}

#[derive(Clone, Copy)]
enum ExprContext {
    Value,
    Sequence(OutputVar),
}

fn apply_lateral_scope<'m>(
    assignments: Vec<Assignment<'m>>,
    in_scope_fn: &dyn Fn() -> VarSet,
    context: ExprContext,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<Vec<TypedHirNode<'m>>> {
    if assignments.is_empty() {
        return Ok(vec![]);
    }

    let in_scope = in_scope_fn();

    let mut scope_groups: FnvHashMap<ontol_hir::Var, Vec<Assignment<'m>>> = Default::default();
    let mut nodes = Vec::with_capacity(assignments.len());

    for assignment in assignments {
        debug!(
            "assignment free_vars: {:?} in_scope: {:?}",
            assignment.expr.meta().free_vars,
            in_scope
        );

        if assignment.expr.free_vars().0.is_subset(&in_scope.0) {
            nodes.push(scoped_expr_to_node(
                assignment.expr,
                &in_scope,
                context,
                table,
                unifier,
            )?);
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

            scope_groups
                .entry(introduced_var)
                .or_default()
                .push(assignment);
        }
    }

    for (introduced_var, assignments) in scope_groups {
        if let Some(data_point_index) = table.find_data_point(introduced_var) {
            let scope_map = &mut table.scope_map_mut(data_point_index);
            let scope_var = scope_map.scope.meta().var;

            let mut builder = LevelBuilder::default();

            match scope_map.scope.kind() {
                flat_scope::Kind::PropVariant(optional, struct_var, property_id) => {
                    let prop_key = (*optional, *struct_var, *property_id);
                    let mut body = vec![];

                    body.extend(apply_lateral_scope(
                        assignments,
                        &|| in_scope.union_one(introduced_var),
                        context,
                        table,
                        unifier,
                    )?);

                    builder.add_prop_variant_scope(scope_var, prop_key, body, table);
                }
                other => return Err(unifier_todo(smart_format!("{other:?}"))),
            }

            nodes.extend(builder.build(unifier));
        } else {
            for assignment in assignments {
                nodes.push(scoped_expr_to_node(
                    assignment.expr,
                    &in_scope,
                    context,
                    table,
                    unifier,
                )?);
            }
        }
    }

    Ok(nodes)
}

/// Convert an expression that has all its exposed free variables
/// in scope, to a HIR node.
fn scoped_expr_to_node<'m>(
    expr::Expr(kind, meta): expr::Expr<'m>,
    in_scope: &VarSet,
    context: ExprContext,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<TypedHirNode<'m>> {
    let next_ctx = ExprContext::Value;
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
                            rel: Box::new(scoped_expr_to_node(
                                attr.rel, in_scope, next_ctx, table, unifier,
                            )?),
                            val: Box::new(scoped_expr_to_node(
                                attr.val, in_scope, next_ctx, table, unifier,
                            )?),
                        })]
                    }
                    expr::PropVariant::Seq { label, elements } => {
                        assert!(elements.is_empty());
                        let sequence_node =
                            find_and_unify_sequence(label, in_scope, table, unifier)?;

                        vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                            rel: Box::new(TypedHirNode(ontol_hir::Kind::Unit, unifier.unit_meta())),
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
                hir_args.push(scoped_expr_to_node(
                    arg, in_scope, next_ctx, table, unifier,
                )?);
            }
            Ok(TypedHirNode(
                ontol_hir::Kind::Call(proc, hir_args),
                meta.hir_meta,
            ))
        }
        expr::Kind::Map(arg) => {
            let hir_arg = scoped_expr_to_node(*arg, in_scope, next_ctx, table, unifier)?;
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
                // panic!("PROPS IN EXPR STRUCT");
                match prop.variant {
                    expr::PropVariant::Singleton(attr) => {
                        let rel = Box::new(scoped_expr_to_node(
                            attr.rel, in_scope, next_ctx, table, unifier,
                        )?);
                        let val = Box::new(scoped_expr_to_node(
                            attr.val, in_scope, next_ctx, table, unifier,
                        )?);
                        let unit_meta = unifier.unit_meta();
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
        expr::Kind::SeqItem(_label, _index, _iter, attr) => {
            let ExprContext::Sequence(output_var) = context else {
                panic!("Unsupported context for seq-item");
            };

            let rel = Box::new(scoped_expr_to_node(
                attr.rel, in_scope, next_ctx, table, unifier,
            )?);
            let val = Box::new(scoped_expr_to_node(
                attr.val, in_scope, next_ctx, table, unifier,
            )?);

            Ok(TypedHirNode(
                ontol_hir::Kind::SeqPush(output_var.0, ontol_hir::Attribute { rel, val }),
                unifier.unit_meta(),
            ))
        }
        other => Err(unifier_todo(smart_format!("leaf expr to node: {other:?}"))),
    }
}

fn find_and_unify_sequence<'m>(
    label: ontol_hir::Label,
    in_scope: &VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<TypedHirNode<'m>> {
    let sequence_body =
        unify_scope_structural(ontol_hir::Var(label.0), in_scope.clone(), table, unifier)?;

    let output_seq_var = table
        .find_scope_map_by_scope_var(ontol_hir::Var(label.0))
        .and_then(|scope_map| match scope_map.scope.kind() {
            flat_scope::Kind::SeqPropVariant(_, output_var, _, _, _) => Some(*output_var),
            _ => None,
        })
        .unwrap();

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

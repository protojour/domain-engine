use ontol_hir::{Optional, StructFlags};
use ontol_runtime::{
    smart_format,
    var::{Var, VarSet},
    DefId,
};
use tracing::{debug, warn};

use crate::{
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{self, UNIT_META},
    types::{Type, UNIT_TYPE},
    NO_SPAN, USE_FLAT_SEQ_HANDLING,
};

use super::{
    expr,
    flat_scope::{self, PropDepth, ScopeVar},
    flat_unifier::{unifier_todo, FlatUnifier},
    flat_unifier_table::{Assignment, AssignmentSlot, ScopeFilter, Table},
    UnifierError, UnifierResult,
};

pub(super) enum AssignResult<'m> {
    Assigned(usize),
    AssignedWithLabel(usize, ontol_hir::Label),
    Unassigned(expr::Expr<'m>),
}

enum IterAssignmentResult {
    Assigned(usize, ontol_hir::Label),
    FreeIterLabel,
    Error(UnifierError),
}

impl<'a, 'm> FlatUnifier<'a, 'm> {
    pub(super) fn assign_to_scope(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        depth: PropDepth,
        mut filter: ScopeFilter,
        table: &mut Table<'m>,
    ) -> UnifierResult<AssignResult<'m>> {
        match kind {
            expr::Kind::Var(var) => {
                if let Some(index) = table.find_var_index(ScopeVar(var)) {
                    table.scope_maps[index]
                        .assignments
                        .push(Assignment::new(expr::Expr(kind, meta)));
                    Ok(AssignResult::Assigned(index))
                } else {
                    table.const_expr = Some(expr::Expr(kind, meta));
                    Ok(AssignResult::Assigned(0))
                }
            }
            kind @ expr::Kind::Struct { .. } => {
                let expr = self.destructure_expr(expr::Expr(kind, meta), depth, &filter, table)?;

                // BUG: Not 0?
                table.scope_maps[0].assignments.push(Assignment::new(expr));

                Ok(AssignResult::Assigned(0))
            }
            expr::Kind::IterSet(label, attr) if USE_FLAT_SEQ_HANDLING => {
                let assign_result = self.assign_to_scope(
                    expr::Expr(
                        expr::Kind::SetElement(label, 0, ontol_hir::Iter(true), attr),
                        meta.clone(),
                    ),
                    depth,
                    filter.clone(),
                    table,
                )?;
                match assign_result {
                    AssignResult::Unassigned(expr::Expr(kind, meta)) => {
                        let expr::Kind::SetElement(_, _, _, attr) = kind else {
                            panic!();
                        };
                        let seq_val_ty = match meta.hir_meta.ty {
                            Type::Seq(_, seq_val_ty) => seq_val_ty,
                            other => {
                                warn!("Not a sequence: {other:?}");
                                other
                            }
                        };

                        // This should be a match-struct in a sequence
                        match (attr.rel, attr.val) {
                            (
                                expr::Expr(expr::Kind::Unit, _),
                                expr::Expr(
                                    expr::Kind::Struct {
                                        binder,
                                        flags,
                                        props,
                                        opt_output_type: _,
                                    },
                                    val_meta,
                                ),
                            ) if flags.contains(StructFlags::MATCH) => {
                                let expr_meta = expr::Meta {
                                    hir_meta: typed_hir::Meta {
                                        ty: self
                                            .types
                                            .intern(Type::Seq(&UNIT_TYPE, val_meta.hir_meta.ty)),
                                        span: val_meta.hir_meta.span,
                                    },
                                    free_vars: val_meta.free_vars,
                                };
                                self.assign_to_scope(
                                    expr::Expr(
                                        expr::Kind::Struct {
                                            binder,
                                            flags,
                                            props,
                                            opt_output_type: if seq_val_ty != val_meta.hir_meta.ty {
                                                Some(meta.hir_meta.ty)
                                            } else {
                                                None
                                            },
                                        },
                                        expr_meta,
                                    ),
                                    depth,
                                    filter,
                                    table,
                                )
                            }
                            (rel, val) => Ok(AssignResult::Unassigned(expr::Expr(
                                expr::Kind::IterSet(
                                    label,
                                    Box::new(ontol_hir::Attribute { rel, val }),
                                ),
                                meta,
                            ))),
                        }
                    }
                    AssignResult::Assigned(index) | AssignResult::AssignedWithLabel(index, ..) => {
                        let output_var = match &table.scope_maps[index].scope.kind() {
                            flat_scope::Kind::SeqPropVariant(_, output_var, ..) => *output_var,
                            flat_scope::Kind::IterElement(_, output_var) => *output_var,
                            other => panic!("{other:?}"),
                        };

                        // BUG: Not 0?
                        table.scope_maps[0]
                            .assignments
                            .push(Assignment::new(expr::Expr(
                                expr::Kind::DestructuredSeq(label, output_var),
                                meta,
                            )));

                        Ok(AssignResult::Assigned(0))
                    }
                }
            }
            expr::Kind::Prop(prop) => {
                let expr::Prop {
                    optional,
                    struct_var,
                    prop_id,
                    seq,
                    variant,
                    free_vars,
                } = *prop;

                // debug!(
                //     "assigning prop {:?}, free_vars={:?}",
                //     prop.prop_id, prop.free_vars
                // );
                // debug!("prop: {prop:?}");

                debug!(
                    "find assignment slot for {}[{}], fv={:?} filter={:?}",
                    prop.struct_var, prop.prop_id, free_vars, filter.req_constraints
                );

                // Provide the sequence label to the assignment slot search
                let seq_label = match &variant {
                    expr::PropVariant::Set {
                        label, elements, ..
                    } => {
                        if elements.iter().any(|(iter, _)| iter.0) {
                            Some(ScopeVar(Var(label.0)))
                        } else {
                            None
                        }
                    }
                    expr::PropVariant::Singleton(..) | expr::PropVariant::Predicate(_) => None,
                };

                let slot =
                    table.find_assignment_slot(&free_vars, seq_label, prop.optional, &mut filter);

                // Split off/assign individual property variants
                let variant = match variant {
                    expr::PropVariant::Set {
                        mut label,
                        elements,
                    } => {
                        let label_filter = filter.with_constraint(ScopeVar(Var(label.0)));

                        let mut non_iter_elements: Vec<(
                            ontol_hir::Iter,
                            ontol_hir::Attribute<expr::Expr<'m>>,
                        )> = vec![];

                        // recursively assign seq elements
                        // The elements are consumed here and assigned to the scope table.
                        for (index, (iter, attr)) in elements.into_iter().enumerate() {
                            if true {
                                let mut free_vars = VarSet::default();
                                free_vars.union_with(&attr.rel.meta().free_vars);
                                free_vars.union_with(&attr.val.meta().free_vars);

                                let element_expr = expr::Expr(
                                    expr::Kind::SetElement(label, index, iter, Box::new(attr)),
                                    expr::Meta {
                                        free_vars,
                                        hir_meta: UNIT_META,
                                    },
                                );

                                match self.assign_to_scope(
                                    element_expr,
                                    depth.next(),
                                    label_filter.clone(),
                                    table,
                                )? {
                                    AssignResult::AssignedWithLabel(_, final_label) => {
                                        label = final_label;
                                    }
                                    AssignResult::Assigned(_) => {
                                        // non-iterated
                                    }
                                    AssignResult::Unassigned(expr) => {
                                        panic!("{expr:?} was unassigned");
                                    }
                                }
                            } else {
                                non_iter_elements.push((ontol_hir::Iter(false), attr));
                            }
                        }

                        expr::PropVariant::Set {
                            label,
                            elements: vec![],
                        }
                    }
                    expr::PropVariant::Singleton(attr) => {
                        let rel = self.destructure_expr(attr.rel, depth.next(), &filter, table)?;
                        let val = self.destructure_expr(attr.val, depth.next(), &filter, table)?;
                        expr::PropVariant::Singleton(ontol_hir::Attribute { rel, val })
                    }
                    expr::PropVariant::Predicate(_) => {
                        todo!()
                    }
                };

                Ok(Self::assign_to_assignment_slot(
                    slot,
                    expr::Expr(
                        expr::Kind::Prop(Box::new(expr::Prop {
                            optional,
                            struct_var,
                            prop_id,
                            variant,
                            seq,
                            free_vars,
                        })),
                        meta,
                    ),
                    table,
                ))
            }
            expr::Kind::SetElement(label, index, iter, attr) => {
                // Find the scope var that matches the label
                let label_scope_var = ScopeVar(Var(label.0));
                let (assignment_idx, final_label) =
                    match self.find_iter_assignment_slot(label_scope_var, &meta.free_vars, table) {
                        IterAssignmentResult::Assigned(assignment_idx, final_label) => {
                            (assignment_idx, final_label)
                        }
                        IterAssignmentResult::FreeIterLabel => {
                            return match &attr.val.kind() {
                                expr::Kind::Struct { flags, .. }
                                    if flags.contains(StructFlags::MATCH) =>
                                {
                                    Ok(AssignResult::Unassigned(expr::Expr(
                                        expr::Kind::SetElement(label, index, iter, attr),
                                        meta,
                                    )))
                                }
                                _ => {
                                    let slot = table.find_assignment_slot(
                                        &meta.free_vars,
                                        None,
                                        Optional(false),
                                        &mut filter,
                                    );
                                    Ok(Self::assign_to_assignment_slot(
                                        slot,
                                        expr::Expr(
                                            expr::Kind::SetElement(label, index, iter, attr),
                                            meta,
                                        ),
                                        table,
                                    ))
                                }
                            };
                        }
                        IterAssignmentResult::Error(error) => return Err(error),
                    };

                let rel = self.destructure_expr(attr.rel, depth, &filter, table)?;
                let val = self.destructure_expr(attr.val, depth, &filter, table)?;

                let iter_scope_map = &mut table.scope_maps[assignment_idx];

                iter_scope_map.assignments.push(Assignment::new(expr::Expr(
                    expr::Kind::SetElement(
                        final_label,
                        index,
                        iter,
                        Box::new(ontol_hir::Attribute { rel, val }),
                    ),
                    meta,
                )));

                Ok(AssignResult::AssignedWithLabel(assignment_idx, final_label))
            }
            e => Err(unifier_todo(smart_format!("expr kind: {e:?}"))),
        }
    }

    fn destructure_expr(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        depth: PropDepth,
        filter: &ScopeFilter,
        table: &mut Table<'m>,
    ) -> UnifierResult<expr::Expr<'m>> {
        match kind {
            expr::Kind::Struct {
                binder,
                flags,
                props,
                opt_output_type,
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
                        depth.next(),
                        filter.clone(),
                        table,
                    )?;
                    if let AssignResult::Unassigned(expr::Expr(kind, _)) = assign_result {
                        debug!("unassigned: {kind:?}");
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
                        opt_output_type,
                    },
                    meta,
                ))
            }
            kind => Ok(expr::Expr(kind, meta)),
        }
    }

    // This function can also reassign the SeqItem label
    fn find_iter_assignment_slot(
        &mut self,
        label_scope_var: ScopeVar,
        expr_free_vars: &VarSet,
        table: &mut Table<'m>,
    ) -> IterAssignmentResult {
        let Some((_idx, scope_map)) = table.find_scope_map_by_scope_var(label_scope_var) else {
            return IterAssignmentResult::FreeIterLabel;
        };

        match scope_map.scope.kind() {
            // For SeqPropVariant, find the scope child that iterates elements
            flat_scope::Kind::SeqPropVariant(label, ..) => {
                let label = *label;
                match table
                    .dependees(Some(label_scope_var))
                    .into_iter()
                    .find(|idx| {
                        let scope_map = &table.scope_maps[*idx];
                        matches!(scope_map.scope.kind(), flat_scope::Kind::IterElement(..))
                    })
                    .map(|idx| (idx, *label.hir()))
                {
                    Some((idx, label)) => IterAssignmentResult::Assigned(idx, label),
                    None => IterAssignmentResult::Error(unifier_todo(smart_format!(
                        "Unable to find IterElement scope under SeqPropVariant"
                    ))),
                }
            }
            flat_scope::Kind::Regex(Some(_label), _) => {
                // looping regex. Need to assign to the RegexAlternation that matches..
                match table
                    .dependees(Some(label_scope_var))
                    .into_iter()
                    .find(|idx| {
                        let alternation_scope_map = &table.scope_maps[*idx];

                        alternation_scope_map
                            .scope
                            .meta()
                            .defs
                            .0
                            .is_subset(&expr_free_vars.0)
                    })
                    .map(|idx| {
                        // allocate a new label so it can be used independently of other sequences
                        (idx, ontol_hir::Label(self.var_allocator.alloc().0))
                    }) {
                    Some((idx, label)) => IterAssignmentResult::Assigned(idx, label),
                    None => IterAssignmentResult::Error(unifier_todo(smart_format!(
                        "Unable to locate loge"
                    ))),
                }
            }
            _ => IterAssignmentResult::Error(unifier_todo(smart_format!(
                "Not able to find iter scope B"
            ))),
        }
    }

    fn assign_to_assignment_slot(
        slot: Option<AssignmentSlot>,
        expr: expr::Expr<'m>,
        table: &mut Table<'m>,
    ) -> AssignResult<'m> {
        if let Some(slot) = slot {
            let scope_map = &mut table.scope_maps[slot.index];
            scope_map
                .assignments
                .push(Assignment::new(expr).with_lateral_deps(slot.lateral_deps));
            AssignResult::Assigned(slot.index)
        } else {
            AssignResult::Unassigned(expr)
        }
    }
}

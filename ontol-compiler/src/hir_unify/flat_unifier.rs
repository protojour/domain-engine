#![allow(clippy::only_used_in_recursion)]

use std::fmt::Display;

use indexmap::IndexMap;
use ontol_hir::{EvalCondTerm, StructFlags};
use ontol_runtime::{condition::Clause, var::Var};
use smartstring::alias::String;
use tracing::{debug, warn};

use crate::{
    hir_unify::CLASSIC_UNIFIER_FALLBACK,
    relation::Relations,
    typed_hir::{self, Meta, TypedHir, TypedHirData, UNIT_META},
    types::{Type, TypeRef, Types, UNIT_TYPE},
    NO_SPAN,
};

use super::{
    expr,
    flat_scope::{self, OutputVar, PropDepth, ScopeVar},
    flat_unifier_impl,
    flat_unifier_table::{ScopeFilter, Table},
    seq_type_infer::SeqTypeInfer,
    unifier::UnifiedNode,
    UnifierError, UnifierResult,
};

#[derive(Clone, Copy)]
pub(super) enum ExprMode {
    Expr,
    Condition(Var, ConditionRoot),
}
#[derive(Clone, Copy)]
pub struct ConditionRoot(pub bool);

#[derive(Clone, Copy, Debug)]
pub(super) enum MainScope<'a, 'm> {
    Const,
    Value(ScopeVar),
    Sequence(ScopeVar, OutputVar),
    MultiSequence(&'a IndexMap<ontol_hir::Label, SeqTypeInfer<'m>>),
}

impl<'a, 'm> MainScope<'a, 'm> {
    pub fn next(self) -> Self {
        match self {
            Self::Const => Self::Const,
            Self::Value(scope_var) => Self::Value(scope_var),
            Self::Sequence(scope_var, _) => Self::Value(scope_var),
            Self::MultiSequence(_) => {
                panic!("MultiSequence must be special cased")
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Level(pub u32);

impl Level {
    pub fn next(self) -> Self {
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

#[derive(Clone, Copy, Debug)]
pub(super) enum StructuralOrigin {
    DependeesOf(ScopeVar),
    Start,
}

pub struct FlatUnifier<'a, 'm> {
    #[allow(unused)]
    pub(super) types: &'a mut Types<'m>,
    pub(super) relations: &'a Relations,
    pub(super) var_allocator: ontol_hir::VarAllocator,
    pub(super) hir_arena: ontol_hir::arena::Arena<'m, TypedHir>,

    condition_root: Option<Var>,
    match_struct_depth: usize,
}

impl<'a, 'm> FlatUnifier<'a, 'm> {
    pub fn new(
        types: &'a mut Types<'m>,
        relations: &'a Relations,
        var_allocator: ontol_hir::VarAllocator,
    ) -> Self {
        Self {
            types,
            relations,
            var_allocator,
            hir_arena: Default::default(),
            condition_root: None,
            match_struct_depth: 0,
        }
    }

    pub(super) fn expr_mode(&self) -> ExprMode {
        match (self.condition_root, self.match_struct_depth) {
            (Some(root), 1) => ExprMode::Condition(root, ConditionRoot(true)),
            (Some(root), _) => ExprMode::Condition(root, ConditionRoot(false)),
            _ => ExprMode::Expr,
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

        if false {
            debug!("expr: {expr:#?}");
        }

        let mut table = Table::new(flat_scope);

        let result = self.assign_to_scope(expr, PropDepth(0), ScopeFilter::default(), &mut table);

        // Debug even if assign_to_scope failed
        for scope_map in &mut table.scope_maps {
            debug!("{}", scope_map.scope);
            for assignment in &scope_map.assignments {
                debug!(
                    "  - {} free={:?} lateral={:?}",
                    assignment.expr.kind().debug_short(),
                    assignment.expr.meta().free_vars,
                    assignment.lateral_deps
                );
            }
        }

        result?;

        flat_unifier_impl::unify_root(None, Default::default(), &mut table, self)
    }

    #[inline]
    pub(super) fn mk_node(
        &mut self,
        kind: ontol_hir::Kind<'m, TypedHir>,
        meta: Meta<'m>,
    ) -> ontol_hir::Node {
        self.hir_arena.add(TypedHirData(kind, meta))
    }

    pub fn maybe_map_node(
        &mut self,
        input: ontol_hir::Node,
        output_type: Option<TypeRef<'m>>,
    ) -> ontol_hir::Node {
        let input_meta = *self.hir_arena[input].meta();

        if let Some(output_type) = output_type {
            match output_type {
                Type::Seq(_outer_rel_type, outer_val_type) => {
                    // TODO: Should maybe handle rel types
                    let inner_val_type = match input_meta.ty {
                        Type::Seq(_inner_rel_type, inner_val_type) => inner_val_type,
                        other => {
                            warn!("Inner type should be sequence!");
                            other
                        }
                    };

                    let unit_meta = typed_hir::Meta {
                        ty: &UNIT_TYPE,
                        span: input_meta.span,
                    };

                    let source_seq_var = self.var_allocator.alloc();
                    let target_seq_var = self.var_allocator.alloc();
                    let item_var = self.var_allocator.alloc();

                    let item_map = {
                        let item_var_ref = self.mk_node(
                            ontol_hir::Kind::Var(item_var),
                            typed_hir::Meta {
                                ty: inner_val_type,
                                span: input_meta.span,
                            },
                        );

                        self.mk_node(
                            ontol_hir::Kind::Map(item_var_ref),
                            typed_hir::Meta {
                                ty: outer_val_type,
                                span: input_meta.span,
                            },
                        )
                    };

                    let for_each = {
                        let push = {
                            let unit_node = self.mk_node(ontol_hir::Kind::Unit, UNIT_META);
                            self.mk_node(
                                ontol_hir::Kind::SeqPush(
                                    target_seq_var,
                                    ontol_hir::Attribute {
                                        rel: unit_node,
                                        val: item_map,
                                    },
                                ),
                                unit_meta,
                            )
                        };

                        self.mk_node(
                            ontol_hir::Kind::ForEach(
                                source_seq_var,
                                (
                                    ontol_hir::Binding::Wildcard,
                                    ontol_hir::Binding::Binder(typed_hir::TypedHirData(
                                        ontol_hir::Binder { var: item_var },
                                        typed_hir::Meta {
                                            ty: inner_val_type,
                                            span: input_meta.span,
                                        },
                                    )),
                                ),
                                [push].into_iter().collect(),
                            ),
                            unit_meta,
                        )
                    };

                    {
                        let source_let = self.mk_node(
                            ontol_hir::Kind::Let(
                                typed_hir::TypedHirData(
                                    ontol_hir::Binder {
                                        var: source_seq_var,
                                    },
                                    input_meta,
                                ),
                                input,
                                [for_each].into_iter().collect(),
                            ),
                            input_meta,
                        );
                        self.mk_node(
                            ontol_hir::Kind::Sequence(
                                typed_hir::TypedHirData(
                                    ontol_hir::Binder {
                                        var: target_seq_var,
                                    },
                                    typed_hir::Meta {
                                        ty: output_type,
                                        span: NO_SPAN,
                                    },
                                ),
                                [source_let].into_iter().collect(),
                            ),
                            typed_hir::Meta {
                                ty: output_type,
                                span: input_meta.span,
                            },
                        )
                    }
                }
                _ => self.mk_node(
                    ontol_hir::Kind::Map(input),
                    typed_hir::Meta {
                        ty: output_type,
                        span: input_meta.span,
                    },
                ),
            }
        } else {
            input
        }
    }

    pub fn push_struct_expr_flags(
        &mut self,
        struct_binder: Var,
        flags: StructFlags,
        ty: TypeRef,
    ) -> UnifierResult<()> {
        if flags.contains(StructFlags::MATCH) {
            // Error is used in unifier tests
            if !matches!(ty, Type::Error) {
                let def_ty = match ty {
                    Type::Seq(_, val_ty) => val_ty,
                    other => other,
                };

                if !matches!(def_ty, Type::Error) {
                    let def_id = def_ty
                        .get_single_def_id()
                        .ok_or(UnifierError::NonEntityQuery)?;
                    let _ = self
                        .relations
                        .identified_by(def_id)
                        .ok_or(UnifierError::NonEntityQuery)?;
                }
            }
            if self.match_struct_depth == 0 {
                self.condition_root = Some(struct_binder);
            }
            self.match_struct_depth += 1;
        } else if self.condition_root.is_some() {
            self.match_struct_depth += 1;
        }
        Ok(())
    }

    pub fn pop_struct_expr_flags(&mut self, flags: StructFlags) {
        if flags.contains(StructFlags::MATCH) {
            self.match_struct_depth -= 1;
            if self.match_struct_depth == 0 {
                self.condition_root = None;
            }
        } else if self.condition_root.is_some() {
            self.match_struct_depth -= 1;
        }
    }

    pub fn push_struct_cond_clauses(
        &mut self,
        cond_var: Var,
        condition_root: ConditionRoot,
        struct_binder: TypedHirData<'m, ontol_hir::Binder>,
        body: &mut ontol_hir::Nodes,
    ) {
        if condition_root.0 {
            body.push(self.mk_node(
                ontol_hir::Kind::PushCondClause(cond_var, Clause::Root(cond_var)),
                UNIT_META,
            ));
        }

        let Some(type_def_id) = struct_binder.meta().ty.get_single_def_id() else {
            return;
        };

        if let Some(properties) = self.relations.properties_by_def_id(type_def_id) {
            if properties.identified_by.is_some() {
                body.push(self.mk_node(
                    ontol_hir::Kind::PushCondClause(
                        cond_var,
                        Clause::IsEntity(EvalCondTerm::QuoteVar(struct_binder.0.var), type_def_id),
                    ),
                    UNIT_META,
                ));
            }
        }
    }
}

pub(super) fn unifier_todo(msg: String) -> UnifierError {
    if !CLASSIC_UNIFIER_FALLBACK {
        todo!("{msg}");
    }
    UnifierError::TODO(msg)
}

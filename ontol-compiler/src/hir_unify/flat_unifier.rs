#![allow(clippy::only_used_in_recursion)]

use std::fmt::Display;

use indexmap::IndexMap;
use ontol_hir::StructFlags;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    hir_unify::CLASSIC_UNIFIER_FALLBACK,
    relation::Relations,
    typed_hir::{Meta, TypedHir, TypedHirData},
    types::{Type, TypeRef, Types},
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

enum ExprMode {
    Expr,
    Condition,
}

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

    match_struct_depth: usize,
    expr_mode: ExprMode,
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
            match_struct_depth: 0,
            expr_mode: ExprMode::Expr,
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

    pub fn push_struct_expr_flags(&mut self, flags: StructFlags, ty: TypeRef) -> UnifierResult<()> {
        if flags.contains(StructFlags::MATCH) {
            // Error is used in unifier tests
            if !matches!(ty, Type::Error) {
                let def_ty = match ty {
                    Type::Seq(_, val_ty) => val_ty,
                    other => other,
                };

                let def_id = def_ty
                    .get_single_def_id()
                    .ok_or(UnifierError::NonEntityQuery)?;
                let _ = self
                    .relations
                    .identified_by(def_id)
                    .ok_or(UnifierError::NonEntityQuery)?;
            }
            self.match_struct_depth += 1;
            self.expr_mode = ExprMode::Condition;
        }
        Ok(())
    }

    pub fn pop_struct_expr_flags(&mut self, flags: StructFlags) {
        if flags.contains(StructFlags::MATCH) {
            self.match_struct_depth -= 1;
            if self.match_struct_depth == 0 {
                self.expr_mode = ExprMode::Expr;
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

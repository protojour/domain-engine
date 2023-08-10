use std::fmt::Debug;

use bit_set::BitSet;
use ontol_hir::visitor::HirVisitor;
use ontol_runtime::DefId;

use crate::{
    hir_unify::{expr_builder::ExprBuilder, scope_builder::ScopeBuilder, unifier::Unifier},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{HirFunc, TypedHir, TypedHirNode},
    types::Type,
    Compiler, SourceSpan,
};

mod dep_tree;
mod dependent_scope_analyzer;
mod expr;
mod expr_builder;
mod regroup_match_prop;
mod scope;
mod scope_builder;
mod unifier;
mod unify_props;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(VarSet),
    NoInputBinder,
    SequenceInputNotSupported,
    MultipleVariablesInExpression(SourceSpan),
}

pub type UnifierResult<T> = Result<T, UnifierError>;

pub fn unify_to_function<'m>(
    scope: &TypedHirNode<'m>,
    expr: &TypedHirNode<'m>,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<HirFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_node(0, scope);
    var_tracker.visit_node(0, expr);

    let scope_ty = scope.ty();
    let expr_ty = expr.ty();

    let unit_type = compiler
        .types
        .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()));

    let (scope_binder, var_allocator) = {
        let mut scope_builder = ScopeBuilder::new(var_tracker.var_allocator(), unit_type);
        let scope_binder = scope_builder.build_scope_binder(scope)?;
        (scope_binder, scope_builder.var_allocator())
    };

    let (expr, var_allocator) = {
        let mut expr_builder = ExprBuilder::new(var_allocator);
        let expr = expr_builder.hir_to_expr(expr);
        (expr, expr_builder.var_allocator())
    };

    let unified =
        Unifier::new(&mut compiler.types, var_allocator).unify(scope_binder.scope, expr)?;

    match unified.typed_binder {
        Some(arg) => {
            // NB: Error is used in unification tests
            if !matches!(scope_ty, Type::Error) {
                assert_eq!(arg.ty, scope_ty);
            }
            if !matches!(expr_ty, Type::Error) {
                assert_eq!(unified.node.ty(), expr_ty);
            }

            Ok(HirFunc {
                arg,
                body: unified.node,
            })
        }
        None => Err(UnifierError::NoInputBinder),
    }
}

struct VariableTracker {
    largest: ontol_hir::Var,
}

impl<'s, 'm: 's> ontol_hir::visitor::HirVisitor<'s, 'm, TypedHir> for VariableTracker {
    fn visit_var(&mut self, var: &ontol_hir::Var) {
        self.observe(*var);
    }

    fn visit_binder(&mut self, var: &ontol_hir::Var) {
        self.observe(*var);
    }

    fn visit_label(&mut self, label: &ontol_hir::Label) {
        self.observe(ontol_hir::Var(label.0))
    }
}

impl Default for VariableTracker {
    fn default() -> Self {
        Self {
            largest: ontol_hir::Var(0),
        }
    }
}

impl VariableTracker {
    fn observe(&mut self, var: ontol_hir::Var) {
        if var.0 > self.largest.0 {
            self.largest.0 = var.0;
        }
    }

    fn var_allocator(&self) -> ontol_hir::VarAllocator {
        let idx = self.largest.0 + 1;
        ontol_hir::Var(idx).into()
    }
}

#[derive(Clone, Default)]
pub struct VarSet(pub BitSet);

impl VarSet {
    pub fn iter(&self) -> VarSetIter {
        VarSetIter(self.0.iter())
    }

    #[inline]
    pub fn insert(&mut self, var: ontol_hir::Var) -> bool {
        self.0.insert(var.0 as usize)
    }

    #[inline]
    pub fn remove(&mut self, var: ontol_hir::Var) -> bool {
        self.0.remove(var.0 as usize)
    }

    #[inline]
    pub fn union_with(&mut self, other: &Self) {
        self.0.union_with(&other.0);
    }
}

impl FromIterator<ontol_hir::Var> for VarSet {
    fn from_iter<T: IntoIterator<Item = ontol_hir::Var>>(iter: T) -> Self {
        Self(iter.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl Debug for VarSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut set = f.debug_set();
        for bit in &self.0 {
            set.entry(&ontol_hir::Var(bit as u32));
        }

        set.finish()
    }
}

impl<I> From<I> for VarSet
where
    I: IntoIterator<Item = ontol_hir::Var>,
{
    fn from(value: I) -> Self {
        Self(value.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl<'a> IntoIterator for &'a VarSet {
    type Item = ontol_hir::Var;
    type IntoIter = VarSetIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        VarSetIter(self.0.iter())
    }
}

pub struct VarSetIter<'b>(bit_set::Iter<'b, u32>);

impl<'b> Iterator for VarSetIter<'b> {
    type Item = ontol_hir::Var;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;
        Some(ontol_hir::Var(next.try_into().unwrap()))
    }
}

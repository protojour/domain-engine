use crate::types::Types;

use super::{expr, flat_scope, unifier::UnifiedNode, UnifierError};

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
        _scope: flat_scope::FlatScope<'m>,
        expr::Expr(_expr_kind, _expr_meta): expr::Expr<'m>,
    ) -> Result<UnifiedNode<'m>, UnifierError> {
        Err(UnifierError::NoInputBinder)
    }
}

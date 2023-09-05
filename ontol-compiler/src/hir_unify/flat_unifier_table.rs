use ontol_runtime::smart_format;

use crate::hir_unify::flat_unifier::unifier_todo;

use super::{expr, flat_scope, UnifierResult, VarSet};

pub(super) struct Table<'m> {
    table: Vec<ScopeMap<'m>>,
    pub const_expr: Option<expr::Expr<'m>>,
}

impl<'m> Table<'m> {
    pub fn new(flat_scope: flat_scope::FlatScope<'m>) -> Self {
        Self {
            table: flat_scope
                .scope_nodes
                .into_iter()
                .map(|scope_node| ScopeMap {
                    scope: scope_node,
                    exprs: vec![],
                })
                .collect(),
            const_expr: None,
        }
    }

    pub fn scope_map_mut(&mut self, index: usize) -> &mut ScopeMap<'m> {
        &mut self.table[index]
    }

    pub fn table_mut(&mut self) -> &mut [ScopeMap<'m>] {
        &mut self.table
    }

    pub fn assign_free_vars(&mut self, free_vars: &VarSet) -> UnifierResult<&mut ScopeMap<'m>> {
        for scope_map in self.table.iter_mut().rev() {
            if scope_map.scope.meta().pub_vars.0.is_superset(&free_vars.0) {
                return Ok(scope_map);
            }
        }

        Err(unifier_todo(smart_format!(
            "Not able to locate scope map for {free_vars:?}"
        )))
    }

    pub fn find_var_index(&self, var: ontol_hir::Var) -> Option<usize> {
        self.table
            .iter()
            .enumerate()
            .find_map(|(index, assignment)| {
                if matches!(
                    assignment.scope.kind(),
                    flat_scope::Kind::Var
                        | flat_scope::Kind::SeqPropVariant(..)
                        | flat_scope::Kind::RegexCapture(_)
                ) {
                    if assignment.scope.meta().var == var {
                        Some(index)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
    }

    pub fn find_scope_var_child(&self, var: ontol_hir::Var) -> Option<&flat_scope::ScopeNode<'m>> {
        let indexes = self.dependees(Some(var));
        if indexes.len() > 1 {
            todo!("Too many indexes here?");
        }

        indexes
            .into_iter()
            .next()
            .map(|index| &self.table[index].scope)
    }

    pub fn dependees(&self, var: Option<ontol_hir::Var>) -> Vec<usize> {
        if let Some(var) = var {
            self.table
                .iter()
                .enumerate()
                .filter_map(|(index, assignment)| {
                    if assignment.scope.meta().deps.contains(var) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            self.table
                .iter()
                .enumerate()
                .filter_map(|(index, assignment)| {
                    if assignment.scope.meta().deps.0.is_empty() {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
}

pub(super) struct ScopeMap<'m> {
    pub scope: flat_scope::ScopeNode<'m>,
    pub exprs: Vec<expr::Expr<'m>>,
}

impl<'m> ScopeMap<'m> {
    pub fn take_single_expr(&mut self) -> Option<expr::Expr<'m>> {
        let expressions = std::mem::take(&mut self.exprs);
        if expressions.len() > 1 {
            panic!("Multiple expressions");
        }
        expressions.into_iter().next()
    }
}

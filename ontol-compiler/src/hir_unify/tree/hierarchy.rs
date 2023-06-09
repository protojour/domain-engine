use std::collections::BTreeMap;

use fnv::FnvHashMap;

use crate::hir_unify::{unifier::UnifierResult, UnifierError, VarSet};

pub trait Scope: Clone {
    fn vars(&self) -> &VarSet;
}

pub trait Expression {
    fn free_vars(&self) -> &VarSet;
    fn optional(&self) -> bool;
}

#[derive(Debug)]
pub struct Hierarchy<S, E> {
    pub scope: S,
    pub expressions: Vec<E>,
    pub children: Vec<Hierarchy<S, E>>,
}

pub struct HierarchyBuilder<S> {
    scopes: Vec<S>,
    scope_index_by_var: FnvHashMap<ontol_hir::Var, usize>,
}

struct IndexedHierarchy<E> {
    pub scope_index: usize,
    pub expressions: Vec<E>,
    pub children: Vec<IndexedHierarchy<E>>,
}

impl<S: Scope> HierarchyBuilder<S> {
    pub fn new(scopes: Vec<S>) -> UnifierResult<Self> {
        let mut scope_index_by_var = FnvHashMap::default();

        for (idx, scope) in scopes.iter().enumerate() {
            for var in scope.vars() {
                if let Some(_) = scope_index_by_var.insert(var, idx) {
                    return Err(UnifierError::NonUniqueVariableDatapoints([var].into()));
                }
            }
        }

        Ok(Self {
            scopes,
            scope_index_by_var,
        })
    }

    pub fn build<E: Expression>(mut self, expressions: Vec<E>) -> Vec<Hierarchy<S, E>> {
        // retain scope order by using BTreeMap:
        let mut scope_assignments: BTreeMap<usize, Vec<E>> = Default::default();

        let mut scope_routing_table: FnvHashMap<ontol_hir::Var, usize> =
            self.scope_index_by_var.clone();

        let mut constant_exprs: Vec<E> = vec![];

        for expression in expressions {
            let mut free_var_iter = expression.free_vars().iter();
            if let Some(first_free_var) = free_var_iter.next() {
                let scope_idx = scope_routing_table.get(&first_free_var).cloned().unwrap();

                if !expression.optional() {
                    // re-route all following to the current scope arm
                    for next_free_var in free_var_iter {
                        scope_routing_table.insert(next_free_var, scope_idx);
                    }
                }

                scope_assignments
                    .entry(scope_idx)
                    .or_default()
                    .push(expression);
            } else {
                constant_exprs.push(expression);
            }
        }

        if !constant_exprs.is_empty() {
            todo!("Handle constant expressions!");
        }

        let indexed_hierarchy = scope_assignments
            .into_iter()
            .map(|(scope_index, expressions)| {
                let in_scope = self.scopes.get(scope_index).unwrap().vars().clone();

                self.build_sub_hierarchy(expressions, scope_index, in_scope)
            })
            .collect();

        self.into_hierarchy(indexed_hierarchy)
    }

    fn build_sub_hierarchy<E: Expression>(
        &mut self,
        expressions: Vec<E>,
        scope_index: usize,
        in_scope: VarSet,
    ) -> IndexedHierarchy<E> {
        // expressions in scope:
        let mut in_scope_exprs = vec![];
        // expressions not in scope (yet), needs to use a sub-group (hierarchy level) to put into scope
        let mut sub_groups: FnvHashMap<ontol_hir::Var, Vec<E>> = Default::default();

        for expression in expressions {
            let mut difference = expression.free_vars().0.difference(&in_scope.0);

            fn next_var(iter: &mut impl Iterator<Item = usize>) -> Option<ontol_hir::Var> {
                iter.next().map(|val| ontol_hir::Var(val as u32))
            }

            if let Some(first_unscoped_var) = next_var(&mut difference) {
                // Needs to assign to a subgroup. Algorithm:
                // For all unscoped vars, check if there already exists a subgroup already assigned to
                // put that variable into scope, and reuse that group.
                // If no such group exists, create a new group using the _first unscoped variable_.
                //
                // This algorithm should create fewer code branches.

                if let Some(group) = sub_groups.get_mut(&first_unscoped_var) {
                    // assign to existing sub group
                    group.push(expression);
                } else {
                    let unassigned_expression = loop {
                        if let Some(next_unscoped_var) = next_var(&mut difference) {
                            if let Some(group) = sub_groups.get_mut(&next_unscoped_var) {
                                // assign to existing sub group
                                group.push(expression);
                                break None;
                            }
                        } else {
                            // unable to assign to an existing group
                            break Some(expression);
                        }
                    };

                    if let Some(expression) = unassigned_expression {
                        // assign to new sub group
                        sub_groups
                            .entry(first_unscoped_var)
                            .or_default()
                            .push(expression);
                    }
                }
            } else {
                // all variables are in scope
                in_scope_exprs.push(expression);
            }
        }

        let children = sub_groups
            .into_iter()
            .map(|(var, sub_expressions)| {
                // introduce on more variable into scope:
                let mut sub_in_scope = in_scope.clone();
                sub_in_scope.0.insert(var.0 as usize);

                let sub_scope_index = *self.scope_index_by_var.get(&var).unwrap();

                self.build_sub_hierarchy(sub_expressions, sub_scope_index, sub_in_scope)
            })
            .collect();

        IndexedHierarchy {
            scope_index,
            expressions: in_scope_exprs,
            children,
        }
    }

    // algorithm for avoiding unnecessary scope clones
    fn into_hierarchy<E: Expression>(
        self,
        indexed_hierarchy: Vec<IndexedHierarchy<E>>,
    ) -> Vec<Hierarchy<S, E>> {
        let mut ref_counts: FnvHashMap<usize, usize> = Default::default();
        let mut scopes_by_index: FnvHashMap<usize, S> =
            self.scopes.into_iter().enumerate().collect();

        for indexed in &indexed_hierarchy {
            count_scopes(indexed, &mut ref_counts);
        }

        indexed_hierarchy
            .into_iter()
            .map(|indexed| into_hierarchy_rec(indexed, &mut ref_counts, &mut scopes_by_index))
            .collect()
    }
}

fn count_scopes<E>(indexed: &IndexedHierarchy<E>, ref_counts: &mut FnvHashMap<usize, usize>) {
    *ref_counts.entry(indexed.scope_index).or_default() += 1;
    for child in &indexed.children {
        count_scopes(child, ref_counts);
    }
}

fn into_hierarchy_rec<S: Clone, E>(
    indexed: IndexedHierarchy<E>,
    ref_counts: &mut FnvHashMap<usize, usize>,
    scopes_by_index: &mut FnvHashMap<usize, S>,
) -> Hierarchy<S, E> {
    let scope_index = indexed.scope_index;
    let count = ref_counts.get_mut(&scope_index).unwrap();
    *count -= 1;

    let scope = if *count == 0 {
        scopes_by_index.remove(&scope_index).unwrap()
    } else {
        scopes_by_index.get(&scope_index).unwrap().clone()
    };

    Hierarchy {
        scope,
        expressions: indexed.expressions,
        children: indexed
            .children
            .into_iter()
            .map(|child| into_hierarchy_rec(child, ref_counts, scopes_by_index))
            .collect(),
    }
}

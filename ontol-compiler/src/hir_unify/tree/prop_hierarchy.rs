use std::collections::BTreeMap;

use fnv::FnvHashMap;

use crate::hir_unify::{unifier::UnifierResult, UnifierError, VarSet};

use super::{expr, scope};

pub struct PropHierarchy<'m> {
    pub scope: scope::Prop<'m>,
    pub props: Vec<expr::Prop<'m>>,
    pub children: Vec<PropHierarchy<'m>>,
}

pub struct PropHierarchyBuilder<'m> {
    scope_props: Vec<scope::Prop<'m>>,
    scope_index_by_var: FnvHashMap<ontol_hir::Var, usize>,
}

struct IndexedPropHierarchy<'m> {
    pub scope_index: usize,
    pub props: Vec<expr::Prop<'m>>,
    pub children: Vec<IndexedPropHierarchy<'m>>,
}

impl<'m> PropHierarchyBuilder<'m> {
    pub fn new(scope_props: Vec<scope::Prop<'m>>) -> UnifierResult<Self> {
        let mut scope_routing_table = FnvHashMap::default();

        for (idx, scope_prop) in scope_props.iter().enumerate() {
            for var in &scope_prop.vars {
                if let Some(_) = scope_routing_table.insert(var, idx) {
                    return Err(UnifierError::NonUniqueVariableDatapoints([var].into()));
                }
            }
        }

        Ok(Self {
            scope_props,
            scope_index_by_var: scope_routing_table,
        })
    }

    pub fn build(mut self, expr_props: Vec<expr::Prop<'m>>) -> Vec<PropHierarchy<'m>> {
        // retain scope order by using BTreeMap:
        let mut scope_assignments: BTreeMap<usize, Vec<expr::Prop>> = Default::default();

        let mut scope_routing_table: FnvHashMap<ontol_hir::Var, usize> =
            self.scope_index_by_var.clone();

        let mut constant_props: Vec<expr::Prop> = vec![];

        for expr_prop in expr_props {
            let mut free_var_iter = expr_prop.free_vars.iter();
            if let Some(first_free_var) = free_var_iter.next() {
                let scope_idx = scope_routing_table.get(&first_free_var).cloned().unwrap();

                if !expr_prop.optional.0 {
                    // re-route all following to the current match arm (scope arm)
                    for next_free_var in free_var_iter {
                        scope_routing_table.insert(next_free_var, scope_idx);
                    }
                }

                scope_assignments
                    .entry(scope_idx)
                    .or_default()
                    .push(expr_prop);
            } else {
                constant_props.push(expr_prop);
            }
        }

        if !constant_props.is_empty() {
            todo!("Handle constant props!");
        }

        let indexed_hierarchy = scope_assignments
            .into_iter()
            .map(|(scope_index, expr_props)| {
                let in_scope = self.scope_props.get(scope_index).unwrap().vars.clone();

                self.build_sub_hierarchy(expr_props, scope_index, in_scope)
            })
            .collect();

        self.into_hierarchy(indexed_hierarchy)
    }

    fn build_sub_hierarchy(
        &mut self,
        expr_props: Vec<expr::Prop<'m>>,
        scope_index: usize,
        in_scope: VarSet,
    ) -> IndexedPropHierarchy<'m> {
        // props in scope:
        let mut in_scope_props = vec![];
        // props not in scope (yet), needs to use a sub-group (hierarchy level) to put into scope
        let mut sub_groups: FnvHashMap<ontol_hir::Var, Vec<expr::Prop>> = Default::default();

        for expr_prop in expr_props {
            let mut difference = expr_prop.free_vars.0.difference(&in_scope.0);

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
                    group.push(expr_prop);
                } else {
                    let unassigned_expr_prop = loop {
                        if let Some(next_unscoped_var) = next_var(&mut difference) {
                            if let Some(group) = sub_groups.get_mut(&next_unscoped_var) {
                                // assign to existing sub group
                                group.push(expr_prop);
                                break None;
                            }
                        } else {
                            // unable to assign to an existing group
                            break Some(expr_prop);
                        }
                    };

                    if let Some(expr_prop) = unassigned_expr_prop {
                        // assign to new sub group
                        sub_groups
                            .entry(first_unscoped_var)
                            .or_default()
                            .push(expr_prop);
                    }
                }
            } else {
                // all variables are in scope
                in_scope_props.push(expr_prop);
            }
        }

        let children = sub_groups
            .into_iter()
            .map(|(var, sub_props)| {
                // introduce on more variable into scope:
                let mut sub_in_scope = in_scope.clone();
                sub_in_scope.0.insert(var.0 as usize);

                let sub_scope_index = *self.scope_index_by_var.get(&var).unwrap();

                self.build_sub_hierarchy(sub_props, sub_scope_index, sub_in_scope)
            })
            .collect();

        IndexedPropHierarchy {
            scope_index,
            props: in_scope_props,
            children,
        }
    }

    // algorithm for avoiding unnecessary scope clones
    fn into_hierarchy(
        self,
        indexed_hierarchy: Vec<IndexedPropHierarchy<'m>>,
    ) -> Vec<PropHierarchy<'m>> {
        let mut ref_counts: FnvHashMap<usize, usize> = Default::default();
        let mut scopes_by_index: FnvHashMap<usize, scope::Prop> =
            self.scope_props.into_iter().enumerate().collect();

        fn count(indexed: &IndexedPropHierarchy, ref_counts: &mut FnvHashMap<usize, usize>) {
            *ref_counts.entry(indexed.scope_index).or_default() += 1;
            for child in &indexed.children {
                count(child, ref_counts);
            }
        }

        for indexed in &indexed_hierarchy {
            count(indexed, &mut ref_counts);
        }

        fn into_hierarchy_rec<'m>(
            indexed: IndexedPropHierarchy<'m>,
            ref_counts: &mut FnvHashMap<usize, usize>,
            scopes_by_index: &mut FnvHashMap<usize, scope::Prop<'m>>,
        ) -> PropHierarchy<'m> {
            let scope_index = indexed.scope_index;
            let count = ref_counts.get_mut(&scope_index).unwrap();
            *count -= 1;

            let scope = if *count == 0 {
                scopes_by_index.remove(&scope_index).unwrap()
            } else {
                scopes_by_index.get(&scope_index).unwrap().clone()
            };

            PropHierarchy {
                scope,
                props: indexed.props,
                children: indexed
                    .children
                    .into_iter()
                    .map(|child| into_hierarchy_rec(child, ref_counts, scopes_by_index))
                    .collect(),
            }
        }

        indexed_hierarchy
            .into_iter()
            .map(|indexed| into_hierarchy_rec(indexed, &mut ref_counts, &mut scopes_by_index))
            .collect()
    }
}

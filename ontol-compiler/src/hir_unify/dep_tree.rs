use std::{collections::BTreeMap, fmt::Debug};

use bit_set::BitSet;
use fnv::FnvHashMap;
use tracing::debug;

use crate::hir_unify::{UnifierError, UnifierResult, VarSet};

pub trait Scope: Clone {
    fn all_vars(&self) -> &VarSet;
    fn dependencies(&self) -> &VarSet;
    fn scan_seq_labels(&self, output: &mut VarSet);
}

pub trait Expression {
    fn free_vars(&self) -> &VarSet;
    fn is_optional(&self) -> bool;
    fn is_seq(&self) -> bool;
}

/// A dependency tree of Expressions and Scopes
#[derive(Debug)]
pub struct DepTree<E, S> {
    /// Subtrees that are dependent on some scope S
    pub trees: Vec<(S, SubTree<E, S>)>,

    /// Expressions that need to escape one level of scoping
    pub escape_exprs: Vec<(Vec<S>, E)>,

    /// Expressions that do not depend on anything
    pub constants: Vec<E>,
}

#[derive(Debug)]
pub struct SubTree<E, S> {
    pub expressions: Vec<E>,
    pub sub_trees: Vec<(S, SubTree<E, S>)>,
}

pub struct DepTreeBuilder<S> {
    scopes: Vec<S>,
    scoped_vars: BitSet,
    seq_labels: BitSet,
    scope_index_by_var: FnvHashMap<ontol_hir::Var, usize>,
}

#[derive(Debug)]
struct IndexedNode<E> {
    pub scope_index: usize,
    pub expressions: Vec<E>,
    pub children: Vec<IndexedNode<E>>,
}

impl<S: Scope + Debug> DepTreeBuilder<S> {
    pub fn new(scopes: Vec<S>) -> UnifierResult<Self> {
        let mut scope_index_by_var = FnvHashMap::default();
        let mut scoped_vars = BitSet::new();
        let mut seq_labels = VarSet::default();

        for (idx, scope) in scopes.iter().enumerate() {
            scope.scan_seq_labels(&mut seq_labels);
            scoped_vars.union_with(&scope.all_vars().0);
            debug!(
                "dep tree {idx} scope vars={:?}, deps={:?}",
                scope.all_vars(),
                scope.dependencies()
            );
            for var in scope.all_vars() {
                if scope_index_by_var.insert(var, idx).is_some() {
                    return Err(UnifierError::NonUniqueVariableDatapoints([var].into()));
                }
            }
        }

        Ok(Self {
            scopes,
            scoped_vars,
            seq_labels: seq_labels.0,
            scope_index_by_var,
        })
    }

    pub fn build<E: Expression + Debug>(mut self, expressions: Vec<E>) -> DepTree<E, S> {
        // retain scope order by using BTreeMap:
        let mut scope_assignments: BTreeMap<usize, Vec<E>> = Default::default();

        let mut scope_routing_table: FnvHashMap<ontol_hir::Var, usize> =
            self.scope_index_by_var.clone();

        let mut escape_exprs: Vec<(Vec<S>, E)> = vec![];
        let mut constants: Vec<E> = vec![];

        for (expr_idx, expression) in expressions.iter().enumerate() {
            debug!(
                "assigning expr_idx={expr_idx}, free_vars={:?} routing={scope_routing_table:?}",
                expression.free_vars()
            );
            self.register_scope_routing(expression, &mut scope_routing_table);
        }

        for expression in expressions {
            let mut var_iter = expression.free_vars().iter();

            // debug!("expr free vars: {:?}", expression.free_vars());

            match self.next_unscoped_var(&mut var_iter) {
                Some(first_free_var) => {
                    let mut seq_label_count = 0;

                    if self.seq_labels.contains(first_free_var.0 as usize) {
                        seq_label_count += 1;
                    }

                    while let Some(next_free_var) = self.next_unscoped_var(&mut var_iter) {
                        if self.seq_labels.contains(next_free_var.0 as usize) {
                            seq_label_count += 1;
                        }
                    }

                    if seq_label_count > 0 && !expression.is_seq() {
                        // Need to "line up" seq expressions with their seq scope
                        debug!("    escaping expression scope!");
                        let free_vars = expression.free_vars();
                        let cloned_scopes = self
                            .scopes
                            .iter()
                            .filter(|scope| scope.all_vars().0.is_subset(&free_vars.0))
                            .cloned()
                            .collect();

                        escape_exprs.push((cloned_scopes, expression));
                    } else {
                        let scope_idx = scope_routing_table.get(&first_free_var).cloned().unwrap();
                        scope_assignments
                            .entry(scope_idx)
                            .or_default()
                            .push(expression);
                    }
                }
                None => {
                    constants.push(expression);
                }
            }
        }

        debug!("scope routing table: {scope_routing_table:?}");

        let root_nodes = scope_assignments
            .into_iter()
            .map(|(scope_index, expressions)| {
                let scope = self.scopes.get(scope_index).unwrap();
                let in_scope = scope.all_vars().clone();

                self.build_expr_sub_tree(expressions, scope_index, in_scope.clone())
            })
            .collect();

        // If the scopes themselves are interdependent, group them into hierarchy:
        let root_nodes = self.make_scope_dependency_tree(root_nodes);

        DepTree {
            trees: self.into_sub_trees(root_nodes),
            escape_exprs,
            constants,
        }
    }

    fn register_scope_routing<E: Expression>(
        &self,
        expression: &E,
        scope_routing_table: &mut FnvHashMap<ontol_hir::Var, usize>,
    ) -> Option<usize> {
        let mut free_var_iter = expression.free_vars().iter();

        if let Some(next_free_var) = self.next_unscoped_var(&mut free_var_iter) {
            let mut scope_idx_candidate = scope_routing_table.get(&next_free_var).cloned().unwrap();

            if expression.is_optional() {
                Some(scope_idx_candidate)
            } else {
                for next_free_var in &mut free_var_iter {
                    let next_candidate = scope_routing_table.get(&next_free_var).cloned().unwrap();
                    if next_candidate < scope_idx_candidate {
                        scope_idx_candidate = next_candidate;
                    }
                }

                // re-route all following to the current scope candidate
                for free_var in expression.free_vars() {
                    Self::reroute(scope_routing_table, free_var, scope_idx_candidate);
                }

                Some(scope_idx_candidate)
            }
        } else {
            None
        }
    }

    fn reroute(
        scope_routing_table: &mut FnvHashMap<ontol_hir::Var, usize>,
        free_var: ontol_hir::Var,
        scope_idx: usize,
    ) {
        debug!("    routing all {free_var:?} to {scope_idx}");

        if let Some(existing_scope_idx) = scope_routing_table.insert(free_var, scope_idx) {
            if existing_scope_idx != scope_idx {
                debug!("        re-routing all {existing_scope_idx} to {scope_idx}");
                // if was overwritten, rewrite all other pointing to the same scope_idx.
                for (_, idx) in scope_routing_table.iter_mut() {
                    if *idx == existing_scope_idx {
                        *idx = scope_idx;
                    }
                }
            }
        }
    }

    fn build_expr_sub_tree<E: Expression>(
        &mut self,
        expressions: Vec<E>,
        scope_index: usize,
        in_scope: VarSet,
    ) -> IndexedNode<E> {
        // expressions in scope:
        let mut in_scope_exprs = vec![];
        // expressions not in scope (yet), needs to use a sub-group (tree level) to put into scope
        let mut sub_groups: FnvHashMap<ontol_hir::Var, Vec<E>> = Default::default();

        for expression in expressions {
            let mut difference = expression
                .free_vars()
                .0
                .difference(&in_scope.0)
                .map(|var| ontol_hir::Var(var as u32));

            if let Some(first_unscoped_var) = self.next_unscoped_var(&mut difference) {
                let mut next_non_seq_label: Option<ontol_hir::Var> = None;

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
                        if let Some(next_unscoped_var) = self.next_unscoped_var(&mut difference) {
                            if let Some(group) = sub_groups.get_mut(&next_unscoped_var) {
                                // assign to existing sub group
                                group.push(expression);
                                break None;
                            } else if !self.seq_labels.contains(next_unscoped_var.0 as usize)
                                && next_non_seq_label.is_none()
                            {
                                // Record that this is not a seq label
                                next_non_seq_label = Some(next_unscoped_var);
                            }
                        } else {
                            // unable to assign to an existing group
                            break Some(expression);
                        }
                    };

                    let assignee_var = if self.seq_labels.contains(first_unscoped_var.0 as usize) {
                        // prefer variable that is not a seq label
                        if let Some(var) = next_non_seq_label {
                            var
                        } else {
                            first_unscoped_var
                        }
                    } else {
                        first_unscoped_var
                    };

                    if let Some(expression) = unassigned_expression {
                        // assign to new sub group
                        sub_groups.entry(assignee_var).or_default().push(expression);
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
                // introduce one more variable into scope:
                let mut sub_in_scope = in_scope.clone();
                sub_in_scope.0.insert(var.0 as usize);

                debug!(
                    "looking up {var}, sub_expressions len: {}",
                    sub_expressions.len()
                );
                let sub_scope_index = *self.scope_index_by_var.get(&var).unwrap();

                self.build_expr_sub_tree(sub_expressions, sub_scope_index, sub_in_scope)
            })
            .collect();

        IndexedNode {
            scope_index,
            expressions: in_scope_exprs,
            children,
        }
    }

    fn next_unscoped_var(
        &self,
        iterator: &mut impl Iterator<Item = ontol_hir::Var>,
    ) -> Option<ontol_hir::Var> {
        iterator.find(|var| self.scoped_vars.contains(var.0 as usize))
    }

    fn make_scope_dependency_tree<E: Expression + Debug>(
        &mut self,
        nodes: Vec<IndexedNode<E>>,
    ) -> Vec<IndexedNode<E>> {
        let mut parents = FnvHashMap::default();
        self.compute_scope_dependency_parents(&nodes, &mut parents);

        // debug!("parents: {parents:#?}");

        let mut root_nodes = vec![];
        let mut child_nodes_by_parent: FnvHashMap<usize, Vec<IndexedNode<E>>> = Default::default();

        for node in nodes {
            if let Some(parent_idx) = parents.remove(&node.scope_index) {
                child_nodes_by_parent
                    .entry(parent_idx)
                    .or_default()
                    .push(node);
            } else {
                root_nodes.push(node);
            }
        }

        for node in root_nodes.iter_mut() {
            let scope_dep_children = Self::build_scope_dependency_tree_sub_level(
                &mut child_nodes_by_parent,
                node.scope_index,
            );

            node.children.extend(scope_dep_children);
        }

        root_nodes
    }

    fn build_scope_dependency_tree_sub_level<E>(
        child_nodes_by_parent: &mut FnvHashMap<usize, Vec<IndexedNode<E>>>,
        parent_idx: usize,
    ) -> Vec<IndexedNode<E>> {
        if let Some(mut nodes) = child_nodes_by_parent.remove(&parent_idx) {
            for node in nodes.iter_mut() {
                node.children
                    .extend(Self::build_scope_dependency_tree_sub_level(
                        child_nodes_by_parent,
                        node.scope_index,
                    ));
            }

            nodes
        } else {
            vec![]
        }
    }

    fn compute_scope_dependency_parents<E>(
        &mut self,
        nodes: &[IndexedNode<E>],
        parents: &mut FnvHashMap<usize, usize>,
    ) {
        for (node_idx, node) in nodes.iter().enumerate() {
            let scope = &self.scopes[node.scope_index];
            let dependencies = scope.dependencies();

            for var in dependencies {
                let mut visited = BitSet::new();
                let mut child_node_idx = node_idx;
                visited.insert(child_node_idx);

                let parent_node_idx = *self.scope_index_by_var.get(&var).unwrap();

                while let Some(new_parent) = parents.get(&child_node_idx) {
                    if visited.contains(*new_parent) {
                        panic!("circular dependency");
                    } else {
                        visited.insert(*new_parent);
                    }
                    child_node_idx = *new_parent;
                }

                debug!("inserting parent {child_node_idx} -> {parent_node_idx}");

                parents.insert(child_node_idx, parent_node_idx);
            }
        }
    }

    // algorithm for avoiding unnecessary scope clones
    fn into_sub_trees<E: Expression + Debug>(
        self,
        node: Vec<IndexedNode<E>>,
    ) -> Vec<(S, SubTree<E, S>)> {
        let mut ref_counts: FnvHashMap<usize, usize> = Default::default();
        let mut scopes_by_index: FnvHashMap<usize, S> =
            self.scopes.into_iter().enumerate().collect();

        for indexed in &node {
            count_scopes(indexed, &mut ref_counts);
        }

        node.into_iter()
            .map(|indexed| into_sub_tree_rec(indexed, &mut ref_counts, &mut scopes_by_index))
            .collect()
    }
}

fn count_scopes<E>(node: &IndexedNode<E>, ref_counts: &mut FnvHashMap<usize, usize>) {
    *ref_counts.entry(node.scope_index).or_default() += 1;
    for child in &node.children {
        count_scopes(child, ref_counts);
    }
}

fn into_sub_tree_rec<S: Clone, E>(
    node: IndexedNode<E>,
    ref_counts: &mut FnvHashMap<usize, usize>,
    scopes_by_index: &mut FnvHashMap<usize, S>,
) -> (S, SubTree<E, S>) {
    let scope = take_scope(node.scope_index, ref_counts, scopes_by_index);

    (
        scope,
        SubTree {
            expressions: node.expressions,
            sub_trees: node
                .children
                .into_iter()
                .map(|child| into_sub_tree_rec(child, ref_counts, scopes_by_index))
                .collect(),
        },
    )
}

fn take_scope<S: Clone>(
    scope_index: usize,
    ref_counts: &mut FnvHashMap<usize, usize>,
    scopes_by_index: &mut FnvHashMap<usize, S>,
) -> S {
    let count = ref_counts.get_mut(&scope_index).unwrap();
    *count -= 1;

    if *count == 0 {
        scopes_by_index.remove(&scope_index).unwrap()
    } else {
        scopes_by_index.get(&scope_index).unwrap().clone()
    }
}

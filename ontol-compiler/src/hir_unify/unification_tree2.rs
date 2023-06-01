use std::{collections::BTreeMap, fmt::Debug};

use bit_set::BitSet;
use fnv::FnvHashMap;

use crate::typed_hir::{Meta, TypedHirNode};

use super::{tagged_node::TaggedKind, var_path::VarPath};

pub struct TargetNode<'m> {
    pub kind: TaggedKind,
    pub meta: Meta<'m>,
    pub free_variables: BitSet,
    pub option_depth: u16,
    pub sub_nodes: Nodes<'m>,
}

#[derive(Default)]
pub struct Nodes<'m> {
    pub sub_scoping: BTreeMap<usize, Nodes<'m>>,
    pub nodes: Vec<TargetNode<'m>>,
    pub dependent_scopes: Vec<Nodes<'m>>,
}

impl<'m> TargetNode<'m> {
    pub fn union_var(mut self, var: ontol_hir::Variable) -> Self {
        self.free_variables.insert(var.0 as usize);
        self
    }

    pub fn union_label(mut self, label: ontol_hir::Label) -> Self {
        self.free_variables.insert(label.0 as usize);
        self
    }

    pub fn into_nodes(self) -> Nodes<'m> {
        Nodes {
            sub_scoping: Default::default(),
            nodes: vec![self],
            dependent_scopes: vec![],
        }
    }
}

impl<'m> Nodes<'m> {
    pub fn new(nodes: Vec<TargetNode<'m>>) -> Self {
        Self {
            sub_scoping: Default::default(),
            nodes,
            dependent_scopes: vec![],
        }
    }

    pub fn expand_scoping(
        &mut self,
        scope: &TypedHirNode<'m>,
        variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath>,
    ) {
        let unscoped = std::mem::take(&mut self.nodes);

        for target_node in unscoped {
            let mut paths_iterator = full_var_path(&target_node.free_variables, variable_paths);
            if let Some(PathSegment::Sub(scope_index)) = paths_iterator.next() {
                insert_scoped(self, (scope, scope_index), target_node, &mut paths_iterator);
            } else {
                self.nodes.push(target_node);
            }
        }
    }

    pub fn expand_scoping2(
        &mut self,
        scope: &TypedHirNode<'m>,
        variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath>,
    ) {
        let unscoped = std::mem::take(&mut self.nodes);

        for target_node in unscoped {
            let mut paths_iterator = full_var_path(&target_node.free_variables, variable_paths);
            if let Some(PathSegment::Sub(scope_index)) = paths_iterator.next() {
                insert_scoped(self, (scope, scope_index), target_node, &mut paths_iterator);
            } else {
                self.nodes.push(target_node);
            }
        }
    }
}

fn insert_scoped<'m>(
    parent_nodes: &mut Nodes<'m>,
    (scope, scope_index): (&TypedHirNode<'m>, usize),
    mut target_node: TargetNode<'m>,
    paths_iterator: &mut PathsIterator,
) {
    match target_node.kind {
        // preserve:
        TaggedKind::Struct(_) => {
            for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                insert_scoped(
                    &mut target_node.sub_nodes,
                    (scope, scope_index),
                    unscoped_sub_node,
                    paths_iterator,
                );
            }

            parent_nodes.nodes.push(target_node);
        }
        // skip:
        TaggedKind::Prop(..) => {
            for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                insert_scoped(
                    parent_nodes,
                    (scope, scope_index),
                    unscoped_sub_node,
                    paths_iterator,
                );
            }
        }
        // scope:
        _ => {
            let sub_nodes = parent_nodes.sub_scoping.entry(scope_index).or_default();
            match paths_iterator.next() {
                Some(PathSegment::Sub(sub_index)) => {
                    insert_scoped(sub_nodes, (scope, sub_index), target_node, paths_iterator)
                }
                Some(PathSegment::Root(root_index)) => {
                    parent_nodes.dependent_scopes.push(Nodes::default());
                    let mut dependent_nodes = parent_nodes.dependent_scopes.last_mut().unwrap();
                    insert_scoped(
                        &mut dependent_nodes,
                        (scope, root_index),
                        target_node,
                        paths_iterator,
                    );
                }
                None => {
                    sub_nodes.nodes.push(target_node);
                }
            }
        }
    }
}

// TODO: No cloning?
fn full_var_path(
    free_variables: &BitSet,
    variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath>,
) -> PathsIterator {
    PathsIterator::new(
        free_variables
            .iter()
            .filter_map(|var_index| variable_paths.get(&ontol_hir::Variable(var_index as u32)))
            .cloned()
            .collect(),
    )
}

struct PathsIterator {
    full_var_path: Vec<VarPath>,
    outer_index: usize,
    inner_index: usize,
}

enum PathSegment {
    Sub(usize),
    Root(usize),
}

impl PathsIterator {
    pub fn new(full_var_path: Vec<VarPath>) -> Self {
        Self {
            full_var_path,
            outer_index: 0,
            inner_index: 0,
        }
    }
}

impl Iterator for PathsIterator {
    type Item = PathSegment;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.outer_index >= self.full_var_path.len() {
                return None;
            } else {
                let var_path = &self.full_var_path[self.outer_index];
                let inner_index = self.inner_index;

                if inner_index >= var_path.0.len() {
                    self.outer_index += 1;
                    self.inner_index = 0;
                } else {
                    self.inner_index += 1;
                    let segment = var_path.0[inner_index];

                    return Some(if self.outer_index != 0 && inner_index == 0 {
                        PathSegment::Root(segment as usize)
                    } else {
                        PathSegment::Sub(segment as usize)
                    });
                }
            }
        }
    }
}

impl<'m> Debug for TargetNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TargetNode")
            .field(&self.kind)
            .field(&self.sub_nodes)
            .finish()
    }
}

impl<'m> Debug for Nodes<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Nodes");
        if !self.sub_scoping.is_empty() {
            debug.field("sub_scoping", &self.sub_scoping);
        }
        if !self.nodes.is_empty() {
            debug.field("nodes", &self.nodes);
        }
        if !self.dependent_scopes.is_empty() {
            debug.field("DEPS", &self.dependent_scopes);
        }

        debug.finish()
    }
}

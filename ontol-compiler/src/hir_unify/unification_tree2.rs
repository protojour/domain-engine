use std::{collections::BTreeMap, fmt::Debug};

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::kind::{NodeKind, Optional};
use tracing::debug;

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

    pub fn into_hir_node(self) -> TypedHirNode<'m> {
        todo!()
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

    pub fn expand_scoping<'s>(
        &mut self,
        variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
    ) {
        let unscoped = std::mem::take(&mut self.nodes);

        debug!("expand scoping, variable_paths = {variable_paths:?}");

        for target_node in unscoped {
            let mut paths_iterator = full_var_path(&target_node.free_variables, variable_paths);
            if let Some(PathSegment::Root(root_scope, scope_index)) = paths_iterator.next() {
                debug!("root_index: {scope_index}");
                ScopingCtx::new().merge_with_scope(
                    self,
                    target_node,
                    Some(NextScope {
                        parent_scope: root_scope,
                        subscope_idx: scope_index,
                    }),
                    &mut paths_iterator,
                );
            } else {
                self.nodes.push(target_node);
            }
        }
    }
}

#[derive(Debug)]
struct ScopingCtx {
    target_option_depth: u16,
    scope_option_depth: u16,
}

impl ScopingCtx {
    fn new() -> Self {
        Self {
            target_option_depth: 0,
            scope_option_depth: 0,
        }
    }

    fn merge_with_scope<'s, 'm>(
        &mut self,
        parent_nodes: &mut Nodes<'m>,
        mut target_node: TargetNode<'m>,
        next_scope: Option<NextScope<'s, 'm>>,
        paths_iterator: &mut PathsIterator<'s, 'm>,
    ) {
        debug!(
            "entered(d={}:{}) {next_scope:?} tk: {:?}",
            self.target_option_depth, self.scope_option_depth, target_node.kind,
        );

        match target_node.kind {
            _ if target_node.free_variables.is_empty() => {
                for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                    self.merge_with_scope(
                        &mut target_node.sub_nodes,
                        unscoped_sub_node,
                        next_scope,
                        paths_iterator,
                    );
                }

                parent_nodes.nodes.push(target_node);
            }
            // don't extend scope
            TaggedKind::Struct(_) => {
                for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                    self.merge_with_scope(
                        &mut target_node.sub_nodes,
                        unscoped_sub_node,
                        next_scope,
                        paths_iterator,
                    );
                }

                parent_nodes.nodes.push(target_node);
            }
            // skip:
            TaggedKind::Prop(..) => {
                for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                    self.merge_with_scope(
                        parent_nodes,
                        unscoped_sub_node,
                        next_scope,
                        paths_iterator,
                    );
                }
            }
            TaggedKind::PropVariant(Optional(true), ..) => {
                self.target_option_depth += 1;

                let cur_scope = self.expand_scope_to_match_optional_depth(
                    parent_nodes,
                    next_scope,
                    paths_iterator,
                );
                for unscoped_sub_node in std::mem::take(&mut target_node.sub_nodes.nodes) {
                    self.merge_with_scope(
                        &mut target_node.sub_nodes,
                        unscoped_sub_node,
                        cur_scope.next_scope,
                        paths_iterator,
                    );
                }
                cur_scope.parent_nodes.nodes.push(target_node);
                self.target_option_depth -= 1;
            }
            _ => {
                let current_scope =
                    self.expand_scope_once(parent_nodes, next_scope, paths_iterator);
                match current_scope.next_scope {
                    Some(next_scope) => self.merge_with_scope(
                        current_scope.parent_nodes,
                        target_node,
                        Some(next_scope),
                        paths_iterator,
                    ),
                    None => current_scope.parent_nodes.nodes.push(target_node),
                }
            }
        }
    }

    fn expand_scope_to_match_optional_depth<'s, 'm, 'n>(
        &mut self,
        parent_nodes: &'n mut Nodes<'m>,
        next_scope: Option<NextScope<'s, 'm>>,
        paths_iterator: &mut PathsIterator<'s, 'm>,
    ) -> CurrentScope<'s, 'm, 'n> {
        let mut current_scope = CurrentScope {
            parent_nodes,
            next_scope,
        };
        while self.scope_option_depth < self.target_option_depth {
            current_scope = self.expand_scope_once(
                current_scope.parent_nodes,
                current_scope.next_scope,
                paths_iterator,
            );
            if current_scope.next_scope.is_none() {
                return current_scope;
            }
        }

        current_scope
    }

    fn expand_scope_once<'s, 'm, 'n>(
        &mut self,
        parent_nodes: &'n mut Nodes<'m>,
        next_scope: Option<NextScope<'s, 'm>>,
        paths_iterator: &mut PathsIterator<'s, 'm>,
    ) -> CurrentScope<'s, 'm, 'n> {
        let NextScope {
            parent_scope,
            subscope_idx,
        } = match next_scope {
            Some(next_scope) => next_scope,
            None => {
                return CurrentScope {
                    parent_nodes,
                    next_scope: None,
                }
            }
        };

        debug!("expand scope once {subscope_idx}");

        let sub_nodes = parent_nodes.sub_scoping.entry(subscope_idx).or_default();

        match &parent_scope.kind {
            NodeKind::VariableRef(..) | NodeKind::Unit | NodeKind::Int(_) => panic!(),
            NodeKind::Let(_, definition, body) => {
                let sub_scope = if subscope_idx == 0 {
                    definition
                } else {
                    &body[subscope_idx - 1]
                };
                self.subscope(sub_nodes, sub_scope, paths_iterator)
            }
            NodeKind::Call(_, sub_scopes) => {
                self.subscope(sub_nodes, &sub_scopes[subscope_idx], paths_iterator)
            }
            NodeKind::Map(sub_scope) => self.subscope(sub_nodes, sub_scope, paths_iterator),
            NodeKind::Seq(_, attr) => self.subscope(sub_nodes, &attr[subscope_idx], paths_iterator),
            NodeKind::Struct(_, sub_scopes) => {
                debug!("struct scope");
                self.subscope(sub_nodes, &sub_scopes[subscope_idx], paths_iterator)
            }
            NodeKind::Prop(optional, struct_var, id, variant_scopes) => {
                if optional.0 {
                    self.scope_option_depth += 1;
                }
                let prop_variant = &variant_scopes[subscope_idx];
                let attr_idx = match paths_iterator.next().expect("Needs an attribute scope") {
                    PathSegment::Sub(idx) => idx,
                    PathSegment::Root(..) => panic!("Root segment at attribute scope"),
                };

                let attr_scope = &prop_variant.attr[attr_idx].as_ref();
                let attr_nodes = sub_nodes.sub_scoping.entry(attr_idx).or_default();

                debug!(
                    "prop scope {struct_var} {id} variant_idx: {subscope_idx} attr_idx: {attr_idx}"
                );

                self.subscope(attr_nodes, attr_scope, paths_iterator)
            }
            NodeKind::MatchProp(..) => {
                panic!("match-prop is not a scope")
            }
            NodeKind::Gen(_, _, sub_scopes) => {
                self.subscope(sub_nodes, &sub_scopes[subscope_idx], paths_iterator)
            }
            NodeKind::Iter(_, _, sub_scopes) => {
                self.subscope(sub_nodes, &sub_scopes[subscope_idx], paths_iterator)
            }
            NodeKind::Push(_, attr) => {
                self.subscope(sub_nodes, &attr[subscope_idx], paths_iterator)
            }
        }
    }

    fn subscope<'s, 'm, 'n>(
        &mut self,
        parent_nodes: &'n mut Nodes<'m>,
        parent_scope: &'s TypedHirNode<'m>,
        paths_iterator: &mut PathsIterator<'s, 'm>,
    ) -> CurrentScope<'s, 'm, 'n> {
        match paths_iterator.next() {
            Some(PathSegment::Sub(next_subscope_idx)) => CurrentScope {
                parent_nodes,
                next_scope: Some(NextScope {
                    parent_scope,
                    subscope_idx: next_subscope_idx,
                }),
            },
            Some(PathSegment::Root(root_scope, next_subscope_idx)) => {
                parent_nodes.dependent_scopes.push(Nodes::default());
                let dependent_nodes = parent_nodes.dependent_scopes.last_mut().unwrap();
                CurrentScope {
                    parent_nodes: dependent_nodes,
                    next_scope: Some(NextScope {
                        parent_scope: root_scope,
                        subscope_idx: next_subscope_idx,
                    }),
                }
            }
            None => CurrentScope {
                parent_nodes,
                next_scope: None,
            },
        }
    }
}

struct CurrentScope<'s, 'm, 'n> {
    parent_nodes: &'n mut Nodes<'m>,
    next_scope: Option<NextScope<'s, 'm>>,
}

#[derive(Clone, Copy)]
struct NextScope<'s, 'm> {
    parent_scope: &'s TypedHirNode<'m>,
    subscope_idx: usize,
}

impl<'s, 'm> Debug for NextScope<'s, 'm> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextScope")
            .field("subscope_idx", &self.subscope_idx)
            .finish()
    }
}

// TODO: No cloning?
fn full_var_path<'s, 'm>(
    free_variables: &BitSet,
    variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
) -> PathsIterator<'s, 'm> {
    PathsIterator::new(
        free_variables
            .iter()
            .filter_map(|var_index| variable_paths.get(&ontol_hir::Variable(var_index as u32)))
            .cloned()
            .collect(),
    )
}

struct PathsIterator<'s, 'm> {
    full_var_path: Vec<VarPath<'s, 'm>>,
    outer_index: usize,
    inner_index: usize,
}

enum PathSegment<'s, 'm> {
    Sub(usize),
    Root(&'s TypedHirNode<'m>, usize),
}

impl<'s, 'm> PathsIterator<'s, 'm> {
    pub fn new(full_var_path: Vec<VarPath<'s, 'm>>) -> Self {
        Self {
            full_var_path,
            outer_index: 0,
            inner_index: 0,
        }
    }
}

impl<'s, 'm> Iterator for PathsIterator<'s, 'm> {
    type Item = PathSegment<'s, 'm>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.outer_index >= self.full_var_path.len() {
                return None;
            } else {
                let var_path = &self.full_var_path[self.outer_index];
                let inner_index = self.inner_index;

                if inner_index >= var_path.path.len() {
                    self.outer_index += 1;
                    self.inner_index = 0;
                } else {
                    self.inner_index += 1;
                    let segment = var_path.path[inner_index];

                    return Some(if inner_index == 0 {
                        debug!("next path segment: Root({segment})");
                        PathSegment::Root(var_path.root, segment as usize)
                    } else {
                        debug!("next path segment: Sub({segment})");
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

use std::{collections::BTreeMap, fmt::Debug};

use fnv::FnvHashMap;
use ontos::Variable;
use tracing::debug;

use super::{tagged_node::TaggedNode, var_path::Path};

#[derive(Default)]
pub struct UnificationNode<'m> {
    pub sub_unifications: BTreeMap<u16, UnificationNode<'m>>,
    pub target_nodes: Vec<TaggedNode<'m>>,
    pub dependents: Vec<UnificationNode<'m>>,
}

impl<'m> Debug for UnificationNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("UNode");
        if !self.sub_unifications.is_empty() {
            debug.field("sub", &self.sub_unifications);
        }
        if !self.target_nodes.is_empty() {
            debug.field("target_nodes", &self.target_nodes);
        }
        if !self.dependents.is_empty() {
            debug.field("DEPS", &self.dependents);
        }
        debug.finish()
    }
}

pub fn build_unification_tree<'m>(
    tagged_nodes: Vec<TaggedNode<'m>>,
    variable_paths: &FnvHashMap<Variable, Path>,
) -> UnificationNode<'m> {
    let mut root = UnificationNode::default();

    for tagged_node in tagged_nodes {
        add_to_tree(tagged_node, variable_paths, &mut root);
    }

    root
}

pub fn add_to_tree<'m>(
    tagged_node: TaggedNode<'m>,
    variable_paths: &FnvHashMap<Variable, Path>,
    mut tree: &mut UnificationNode<'m>,
) {
    debug!("add_to_tree {:?}", tagged_node.free_variables);

    {
        let mut path_iterator = tagged_node
            .free_variables
            .iter()
            .filter_map(|var_index| variable_paths.get(&Variable(var_index as u32)))
            .peekable();

        while let Some(path) = path_iterator.next() {
            debug!("locating path {path:?}");

            for index in &path.0 {
                tree = tree
                    .sub_unifications
                    .entry(*index)
                    .or_insert_with(UnificationNode::default);
            }

            if path_iterator.peek().is_some() {
                tree.dependents.push(UnificationNode::default());
                tree = tree.dependents.last_mut().unwrap();
            }
        }
    }

    tree.target_nodes.push(tagged_node);
}

use std::collections::BTreeMap;

use fnv::FnvHashMap;
use ontos::kind::Variable;

use super::{tagged_node::TaggedNode, var_path::Path};

#[derive(Debug)]
pub struct UnificationNode<'m> {
    pub index: usize,
    pub unify_children: BTreeMap<u16, UnificationNode<'m>>,
    pub target_nodes: Vec<TaggedNode<'m>>,
    pub dependents: Vec<UnificationNode<'m>>,
}

impl<'m> UnificationNode<'m> {
    fn new(index: usize) -> Self {
        Self {
            index,
            unify_children: BTreeMap::new(),
            target_nodes: vec![],
            dependents: vec![],
        }
    }
}

pub fn build_unification_tree<'m>(
    tagged_nodes: Vec<TaggedNode<'m>>,
    variable_paths: &FnvHashMap<Variable, Path>,
) -> UnificationNode<'m> {
    let mut root = UnificationNode::new(0);

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
    {
        let mut path_iterator = tagged_node
            .free_variables
            .iter()
            .filter_map(|var_index| variable_paths.get(&Variable(var_index as u32)))
            .peekable();

        while let Some(path) = path_iterator.next() {
            for index in &path.0 {
                tree = tree
                    .unify_children
                    .entry(*index)
                    .or_insert_with(|| UnificationNode::new(*index as usize));
            }

            if path_iterator.peek().is_some() {
                tree.dependents.push(UnificationNode::new(0));
                tree = tree.dependents.last_mut().unwrap();
            }
        }
    }

    tree.target_nodes.push(tagged_node);
}

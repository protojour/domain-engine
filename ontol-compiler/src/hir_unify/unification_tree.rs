use std::{collections::BTreeMap, fmt::Debug};

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::{Binder, Variable};
use tracing::debug;

use crate::typed_hir::Meta;

use super::{
    tagged_node::{TaggedKind, TaggedNode},
    var_path::Path,
};

pub enum UnificationNode<'m> {
    Scoping(Scoping<'m>),
    Struct(Struct<'m>),
}

impl<'m> UnificationNode<'m> {
    pub fn build(node: TaggedNode<'m>, variable_paths: &FnvHashMap<Variable, Path>) -> Self {
        match &node.kind {
            TaggedKind::Struct(binder) => {
                let mut struct_node = Struct {
                    binder: *binder,
                    meta: node.meta,
                    sub_scoping: Scoping::default(),
                    nodes: vec![],
                };

                for child in node.children.0 {
                    if child
                        .free_variables
                        .iter()
                        .any(|var| variable_paths.contains_key(&Variable(var as u32)))
                    {
                        struct_node
                            .sub_scoping
                            .add_expr_under_scope(child, variable_paths);
                    } else {
                        struct_node.nodes.push(child);
                    }
                }

                UnificationNode::Struct(struct_node)
            }
            _ => {
                let mut root_scoping = Scoping::default();
                root_scoping.add_expr_under_scope(node, variable_paths);
                UnificationNode::Scoping(root_scoping)
            }
        }
    }

    pub fn force_into_scoping(self) -> Scoping<'m> {
        match self {
            Self::Scoping(s) => s,
            _ => panic!("Expected scoping"),
        }
    }

    fn scoping_mut(&mut self) -> &mut Scoping<'m> {
        match self {
            Self::Scoping(s) => s,
            Self::Struct(e) => &mut e.sub_scoping,
        }
    }

    fn expect_scoping_mut(&mut self) -> &mut Scoping<'m> {
        match self {
            Self::Scoping(s) => s,
            _ => panic!("Expected scoping"),
        }
    }
}

#[derive(Default)]
pub struct Scoping<'m> {
    pub sub_nodes: BTreeMap<usize, UnificationNode<'m>>,
    pub expressions: Vec<TaggedNode<'m>>,
    pub dependent_scopes: Vec<UnificationNode<'m>>,
}

impl<'m> Scoping<'m> {
    fn add_expr_under_scope(
        &mut self,
        expression: TaggedNode<'m>,
        variable_paths: &FnvHashMap<Variable, Path>,
    ) {
        match expression.kind {
            TaggedKind::Prop(..) => {
                for child in expression.children.0 {
                    self.add_expr_under_scope(child, variable_paths);
                }
            }
            _ => {
                let sub_scoping =
                    self.variables_sub_scoping_mut(&expression.free_variables, variable_paths);

                sub_scoping.expressions.push(expression);
            }
        }
    }

    fn variables_sub_scoping_mut(
        &mut self,
        free_variables: &BitSet,
        variable_paths: &FnvHashMap<Variable, Path>,
    ) -> &mut Scoping<'m> {
        let mut scoping = self;

        {
            let mut path_iterator = free_variables
                .iter()
                .filter_map(|var_index| variable_paths.get(&Variable(var_index as u32)))
                .peekable();

            while let Some(path) = path_iterator.next() {
                debug!("locating path {path:?}");

                scoping = scoping.path_sub_scoping_mut(path);

                if path_iterator.peek().is_some() {
                    scoping
                        .dependent_scopes
                        .push(UnificationNode::Scoping(Default::default()));
                    scoping = scoping
                        .dependent_scopes
                        .last_mut()
                        .unwrap()
                        .expect_scoping_mut();
                }
            }
        }

        scoping
    }

    fn path_sub_scoping_mut(&mut self, path: &Path) -> &mut Scoping<'m> {
        let mut scoping = self;

        for index in &path.0 {
            scoping = scoping
                .sub_nodes
                .entry(*index as usize)
                .or_insert_with(|| UnificationNode::Scoping(Default::default()))
                .scoping_mut()
        }

        scoping
    }
}

#[derive(Debug)]
pub struct Struct<'m> {
    pub binder: Binder,
    pub meta: Meta<'m>,
    pub sub_scoping: Scoping<'m>,
    pub nodes: Vec<TaggedNode<'m>>,
}

impl<'m> Debug for UnificationNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scoping(scoping) => {
                write!(f, "{scoping:?}")
            }
            Self::Struct(s) => write!(f, "{s:?}"),
        }
    }
}

impl<'m> Debug for Scoping<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Scoping");
        if !self.sub_nodes.is_empty() {
            debug.field("sub", &self.sub_nodes);
        }
        if !self.expressions.is_empty() {
            debug.field("expressions", &self.expressions);
        }
        if !self.dependent_scopes.is_empty() {
            debug.field("DEPS", &self.dependent_scopes);
        }
        debug.finish()
    }
}

use ontos::{
    kind::{NodeKind, Variable},
    visitor::OntosVisitor,
    Node,
};
use tracing::debug;

use crate::typed_ontos::unify::{
    unifier::{unify_tree, Unified, UnifyCtx},
    var_path::LocateCtx,
};

use self::{
    tagged_node::{tag_nodes, union_bitsets, TagCtx},
    unification_tree::build_unification_tree,
};

use super::lang::{OntosNode, TypedOntos};

mod tagged_node;
mod unification_tree;
mod unifier;
mod var_path;

struct VariableTracker {
    largest: Variable,
}

impl OntosVisitor<TypedOntos> for VariableTracker {
    fn visit_variable(&mut self, variable: &Variable) {
        self.observe(*variable);
    }

    fn visit_binder(&mut self, variable: &Variable) {
        self.observe(*variable);
    }
}

impl Default for VariableTracker {
    fn default() -> Self {
        Self {
            largest: Variable(0),
        }
    }
}

impl VariableTracker {
    fn observe(&mut self, var: Variable) {
        if var.0 > self.largest.0 {
            self.largest.0 = var.0;
        }
    }

    fn next_variable(&self) -> Variable {
        let idx = self.largest.0 + 1;
        Variable(idx)
    }
}

pub fn unify<'m>(source: OntosNode<'m>, target: OntosNode<'m>) -> OntosNode<'m> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_kind(0, source.kind());
    var_tracker.visit_kind(0, target.kind());

    let mut tag_ctx = TagCtx::default();
    let meta = target.meta;
    match target.kind {
        NodeKind::Struct(binder, children) => {
            let tagged_nodes = tag_ctx.enter_binder(binder, |ctx| tag_nodes(children, ctx));
            let free_variables =
                union_bitsets(tagged_nodes.iter().map(|node| &node.free_variables));

            let mut locate_ctx = LocateCtx::new(&free_variables);
            locate_ctx.traverse_kind(source.kind());
            let variable_paths = locate_ctx.output;

            debug!("free variable paths: {variable_paths:#?}");
            let unification_tree = build_unification_tree(tagged_nodes, &variable_paths);

            let Unified {
                nodes,
                binder: _input_binder,
            } = unify_tree(
                &source,
                unification_tree,
                &mut UnifyCtx::new(var_tracker.next_variable()),
            );

            OntosNode {
                kind: NodeKind::Struct(binder, nodes.into_iter().collect()),
                meta,
            }
        }
        _ => {
            todo!()
        }
    }
}

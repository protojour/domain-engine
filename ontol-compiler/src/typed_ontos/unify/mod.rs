use ontos::kind::NodeKind;
use tracing::debug;

use crate::typed_ontos::unify::unifier::{unify_tree, Unified, UnifyCtx};

use self::{
    tagged_node::{tag_nodes, union_bitsets, TagCtx},
    unification_tree::build_unification_tree,
    var_path::locate_variables,
};

use super::lang::OntosNode;

mod tagged_node;
mod unification_tree;
mod unifier;
mod var_path;

pub fn unify<'m>(source: OntosNode<'m>, target: OntosNode<'m>) -> OntosNode<'m> {
    let mut tag_ctx = TagCtx::default();
    let meta = target.meta;
    match target.kind {
        NodeKind::Struct(binder, children) => {
            let tagged_nodes = tag_ctx.enter_binder(binder, |ctx| tag_nodes(children, ctx));
            let free_variables =
                union_bitsets(tagged_nodes.iter().map(|node| &node.free_variables));
            let variable_paths = locate_variables(&source, &free_variables);
            debug!("free variable paths: {variable_paths:#?}");
            let unification_tree = build_unification_tree(tagged_nodes, &variable_paths);
            let Unified {
                nodes,
                binder: _input_binder,
            } = unify_tree(
                &source,
                unification_tree,
                &mut UnifyCtx::new(tag_ctx.next_variable()),
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

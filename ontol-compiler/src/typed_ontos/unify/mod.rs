use ontos::kind::NodeKind;
use tracing::debug;

use self::{
    tagged_node::{tag_nodes, union_bitsets, TagCtx},
    unification_tree::build_unification_tree,
    var_path::locate_variables,
};

use super::lang::OntosNode;

mod tagged_node;
mod unification_tree;
mod var_path;

pub fn unify<'m>(source: OntosNode<'m>, target: OntosNode<'m>) -> OntosNode<'m> {
    let mut ctx = TagCtx::default();
    let meta = target.meta;
    match target.kind {
        NodeKind::Struct(binder, children) => {
            let tagged_nodes = ctx.enter_binder(binder, |ctx| tag_nodes(children, ctx));
            let free_variables =
                union_bitsets(tagged_nodes.iter().map(|node| &node.free_variables));
            let variable_paths = locate_variables(&source, &free_variables);
            let unification_tree = build_unification_tree(tagged_nodes, &variable_paths);
            debug!("unification tree: {unification_tree:#?}");

            OntosNode {
                kind: NodeKind::Struct(binder, vec![]),
                meta,
            }
        }
        _ => {
            todo!()
        }
    }
}

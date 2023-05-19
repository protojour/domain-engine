use ontos::kind::{Binder, MatchArm, NodeKind, PatternBinding, PropPattern, Variable};
use smallvec::SmallVec;

use crate::typed_ontos::lang::{OntosKind, OntosNode};

use super::unification_tree::UnificationNode;

type UnifiedNodes<'m> = SmallVec<[OntosNode<'m>; 1]>;

pub struct Unified<'m> {
    pub nodes: UnifiedNodes<'m>,
    pub binder: Option<Binder>,
}

pub struct UnifyCtx {
    pub next_var: Variable,
}

impl UnifyCtx {
    pub fn new(next_var: Variable) -> Self {
        Self { next_var }
    }

    fn _alloc_var(&mut self) -> Variable {
        let var = self.next_var;
        self.next_var.0 += 1;
        var
    }
}

pub fn unify_tree<'m>(
    source: &OntosNode<'m>,
    u_tree: UnificationNode<'m>,
    ctx: &mut UnifyCtx,
) -> Unified<'m> {
    unify_node(source, u_tree, ctx)
}

fn unify_node<'m>(
    source: &OntosNode<'m>,
    mut u_node: UnificationNode<'m>,
    ctx: &mut UnifyCtx,
) -> Unified<'m> {
    let OntosNode { kind, meta } = source;
    let meta = *meta;
    match kind {
        NodeKind::VariableRef(var) => {
            let nodes = merge_target_nodes(u_node);
            Unified {
                nodes,
                binder: Some(Binder(*var)),
            }
        }
        NodeKind::Unit => panic!(),
        NodeKind::Int(_int) => panic!(),
        NodeKind::Call(_proc, _args) => {
            panic!()
        }
        NodeKind::Seq(_binder, _nodes) => panic!(),
        NodeKind::Struct(binder, nodes) => {
            let nodes = unify_children(nodes, u_node, ctx);
            Unified {
                nodes: [OntosNode {
                    kind: OntosKind::Destruct(binder.0, nodes),
                    meta,
                }]
                .into(),
                binder: Some(*binder),
            }
        }
        NodeKind::Prop(struct_var, prop, rel, val) => {
            let rel_binding = unify_pattern_binding(rel, u_node.unify_children.remove(&0), ctx);
            let val_binding = unify_pattern_binding(val, u_node.unify_children.remove(&1), ctx);

            let arms = match (rel_binding.nodes, val_binding.nodes) {
                (None, None) => {
                    vec![]
                }
                (Some(rel), None) => {
                    vec![MatchArm {
                        pattern: PropPattern::Present(
                            rel_binding.binding,
                            PatternBinding::Wildcard,
                        ),
                        nodes: rel.into_iter().collect(),
                    }]
                }
                (None, Some(val)) => {
                    vec![MatchArm {
                        pattern: PropPattern::Present(
                            PatternBinding::Wildcard,
                            val_binding.binding,
                        ),
                        nodes: val.into_iter().collect(),
                    }]
                }
                (Some(_rel), Some(_val)) => {
                    todo!()
                }
            };

            Unified {
                nodes: [OntosNode {
                    kind: OntosKind::MatchProp(*struct_var, prop.clone(), arms),
                    meta,
                }]
                .into(),
                binder: None,
            }
        }
        NodeKind::MapSeq(..) => {
            todo!()
        }
        NodeKind::Destruct(..) => {
            unimplemented!("BUG: Destruct is an output node")
        }
        NodeKind::MatchProp(..) => {
            unimplemented!("BUG: MatchProp is an output node")
        }
    }
}

fn merge_target_nodes<'m>(u_node: UnificationNode<'m>) -> UnifiedNodes<'m> {
    let mut merged: UnifiedNodes<'m> = Default::default();
    for tagged_node in u_node.target_nodes {
        merged.push(tagged_node.into_ontos_node());
    }

    merged
}

fn unify_children<'m>(
    children: &[OntosNode<'m>],
    u_node: UnificationNode<'m>,
    ctx: &mut UnifyCtx,
) -> Vec<OntosNode<'m>> {
    u_node
        .unify_children
        .into_iter()
        .flat_map(|(index, u_node)| {
            let child = &children[index as usize];

            unify_node(child, u_node, ctx).nodes
        })
        .collect()
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Option<UnifiedNodes<'m>>,
}

fn unify_pattern_binding<'m>(
    source: &OntosNode<'m>,
    u_node: Option<UnificationNode<'m>>,
    ctx: &mut UnifyCtx,
) -> UnifyPatternBinding<'m> {
    let u_node = match u_node {
        Some(u_node) => u_node,
        None => {
            return UnifyPatternBinding {
                binding: PatternBinding::Wildcard,
                nodes: None,
            }
        }
    };

    let Unified { nodes, binder } = unify_node(source, u_node, ctx);
    let binding = match binder {
        Some(binder) => PatternBinding::Binder(binder.0),
        None => PatternBinding::Wildcard,
    };
    UnifyPatternBinding {
        binding,
        nodes: Some(nodes),
    }
}

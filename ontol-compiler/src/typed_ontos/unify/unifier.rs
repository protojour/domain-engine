use ontos::kind::{Binder, MatchArm, NodeKind, PatternBinding, PropPattern, Variable};

use crate::typed_ontos::lang::{OntosKind, OntosNode};

use super::unification_tree::UnificationNode;

pub struct Unified<'m> {
    pub node: OntosNode<'m>,
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
        NodeKind::VariableRef(var) => Unified {
            node: OntosNode {
                kind: OntosKind::VariableRef(*var),
                meta,
            },
            binder: Some(Binder(*var)),
        },
        NodeKind::Unit => panic!(),
        NodeKind::Int(_int) => panic!(),
        NodeKind::Call(_proc, _args) => {
            panic!()
        }
        NodeKind::Seq(_binder, _nodes) => panic!(),
        NodeKind::Struct(binder, nodes) => {
            let nodes = unify_children(nodes, u_node, ctx);
            Unified {
                node: OntosNode {
                    kind: OntosKind::Destruct(binder.0, nodes),
                    meta,
                },
                binder: Some(*binder),
            }
        }
        NodeKind::Prop(struct_var, prop, rel, val) => {
            let rel_binding = unify_pattern_binding(rel, u_node.unify_children.remove(&0), ctx);
            let val_binding = unify_pattern_binding(val, u_node.unify_children.remove(&1), ctx);

            let arms = match (rel_binding.node, val_binding.node) {
                (None, None) => {
                    vec![]
                }
                (Some(rel), None) => {
                    vec![MatchArm {
                        pattern: PropPattern::Present(
                            rel_binding.binding,
                            PatternBinding::Wildcard,
                        ),
                        node: rel,
                    }]
                }
                (None, Some(val)) => {
                    vec![MatchArm {
                        pattern: PropPattern::Present(
                            PatternBinding::Wildcard,
                            val_binding.binding,
                        ),
                        node: val,
                    }]
                }
                (Some(_rel), Some(_val)) => {
                    todo!()
                }
            };

            Unified {
                node: OntosNode {
                    kind: OntosKind::MatchProp(*struct_var, prop.clone(), arms),
                    meta,
                },
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

fn unify_children<'m>(
    children: &[OntosNode<'m>],
    u_node: UnificationNode<'m>,
    ctx: &mut UnifyCtx,
) -> Vec<OntosNode<'m>> {
    u_node
        .unify_children
        .into_iter()
        .map(|(index, u_node)| {
            let child = &children[index as usize];

            unify_node(child, u_node, ctx).node
        })
        .collect()
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    node: Option<OntosNode<'m>>,
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
                node: None,
            }
        }
    };

    let Unified { node, binder } = unify_node(source, u_node, ctx);
    let binding = match binder {
        Some(binder) => PatternBinding::Binder(binder.0),
        None => PatternBinding::Wildcard,
    };
    UnifyPatternBinding {
        binding,
        node: Some(node),
    }
}

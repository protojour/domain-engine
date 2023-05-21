use ontol_runtime::vm::proc::BuiltinProc;
use ontos::kind::{Binder, MatchArm, NodeKind, PatternBinding, PropPattern, Variable};
use smallvec::SmallVec;

use crate::typed_ontos::lang::{Meta, OntosKind, OntosNode};

use super::unification_tree::UnificationNode;

type UnifiedNodes<'m> = SmallVec<[OntosNode<'m>; 1]>;

pub struct Unified<'m> {
    pub binder: Option<Binder>,
    pub nodes: UnifiedNodes<'m>,
}

struct UnifyCtx<'a, 'm> {
    root_source: &'a OntosNode<'m>,
    next_variable: Variable,
}

impl<'a, 'm> UnifyCtx<'a, 'm> {
    fn alloc_var(&mut self) -> Variable {
        let var = self.next_variable;
        self.next_variable.0 += 1;
        var
    }
}

pub fn unify_tree<'m>(
    u_tree: UnificationNode<'m>,
    source: &OntosNode<'m>,
    next_variable: Variable,
) -> Unified<'m> {
    let mut ctx = UnifyCtx {
        next_variable,
        root_source: source,
    };
    unify_node(u_tree, source, &mut ctx)
}

fn unify_node<'m>(
    mut u_node: UnificationNode<'m>,
    source: &OntosNode<'m>,
    ctx: &mut UnifyCtx<'_, 'm>,
) -> Unified<'m> {
    let OntosNode { kind, meta } = source;
    let meta = *meta;
    match kind {
        NodeKind::VariableRef(var) => {
            let nodes = merge_target_nodes(&mut u_node, ctx);
            Unified {
                binder: Some(Binder(*var)),
                nodes,
            }
        }
        NodeKind::Unit => panic!(),
        NodeKind::Int(_int) => panic!(),
        NodeKind::Call(proc, args) => match u_node.sub_unifications.len() {
            0 => {
                let args = unify_children(args, u_node, ctx);
                Unified {
                    binder: None,
                    nodes: [OntosNode {
                        kind: NodeKind::Call(*proc, args),
                        meta,
                    }]
                    .into(),
                }
            }
            1 => {
                let subst_var = ctx.alloc_var();
                let inverted_call = invert_call(proc, subst_var, u_node, args, meta, ctx);
                Unified {
                    binder: Some(Binder(subst_var)),
                    nodes: [OntosNode {
                        kind: NodeKind::Let(
                            Binder(inverted_call.pivot_variable),
                            Box::new(inverted_call.def),
                            inverted_call.body.into_iter().collect(),
                        ),
                        meta,
                    }]
                    .into(),
                }
            }
            _ => {
                panic!("Multiple variables in function call!");
            }
        },
        NodeKind::Let(..) => {
            unimplemented!("BUG: Let is an output node")
        }
        NodeKind::Seq(_binder, _nodes) => panic!(),
        NodeKind::Struct(binder, nodes) => {
            let nodes = unify_children(nodes, u_node, ctx);
            Unified {
                binder: Some(*binder),
                nodes: nodes.into(),
            }
        }
        NodeKind::Prop(struct_var, prop, variant) => {
            let rel_binding =
                unify_pattern_binding(u_node.sub_unifications.remove(&0), &variant.rel, ctx);
            let val_binding =
                unify_pattern_binding(u_node.sub_unifications.remove(&1), &variant.val, ctx);

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
                (Some(rel), Some(val)) => {
                    let mut concatenated: Vec<OntosNode> = vec![];
                    concatenated.extend(rel);
                    concatenated.extend(val);
                    vec![MatchArm {
                        pattern: PropPattern::Present(rel_binding.binding, val_binding.binding),
                        nodes: concatenated,
                    }]
                }
            };

            Unified {
                binder: None,
                nodes: [OntosNode {
                    kind: OntosKind::MatchProp(*struct_var, prop.clone(), arms),
                    meta,
                }]
                .into(),
            }
        }
        NodeKind::MapSeq(..) => {
            todo!()
        }
        NodeKind::MatchProp(..) => {
            unimplemented!("BUG: MatchProp is an output node")
        }
    }
}

struct InvertedCall<'m> {
    pivot_variable: Variable,
    def: OntosNode<'m>,
    body: UnifiedNodes<'m>,
}

fn invert_call<'m>(
    proc: &BuiltinProc,
    subst_var: Variable,
    mut u_node: UnificationNode<'m>,
    args: &[OntosNode<'m>],
    meta: Meta<'m>,
    ctx: &mut UnifyCtx<'_, 'm>,
) -> InvertedCall<'m> {
    if u_node.sub_unifications.len() > 1 {
        panic!("Too many sub unifications in function call");
    }

    // FIXME: Properly check this
    let inverted_proc = match proc {
        BuiltinProc::Add => BuiltinProc::Sub,
        BuiltinProc::Sub => BuiltinProc::Add,
        BuiltinProc::Mul => BuiltinProc::Div,
        BuiltinProc::Div => BuiltinProc::Mul,
        _ => panic!("Unsupported procedure; cannot invert {proc:?}"),
    };

    let mut new_args: Vec<OntosNode<'m>> = vec![];

    let (unification_idx, mut sub_unification) = u_node.sub_unifications.pop_first().unwrap();
    let unification_idx = unification_idx as usize;
    for (index, arg) in args.iter().enumerate() {
        if index != unification_idx {
            new_args.push(arg.clone());
        }
    }

    let pivot_arg = &args[unification_idx];
    match &pivot_arg.kind {
        NodeKind::VariableRef(var) => {
            new_args.insert(
                unification_idx,
                OntosNode {
                    kind: NodeKind::VariableRef(subst_var),
                    meta: pivot_arg.meta,
                },
            );
            let body = merge_target_nodes(&mut sub_unification, ctx);

            InvertedCall {
                pivot_variable: *var,
                def: OntosNode {
                    kind: NodeKind::Call(inverted_proc, new_args),
                    meta,
                },
                body,
            }
        }
        NodeKind::Call(child_proc, child_args) => {
            let sub_inverted_call = invert_call(
                child_proc,
                subst_var,
                sub_unification,
                child_args,
                pivot_arg.meta,
                ctx,
            );
            new_args.insert(unification_idx, sub_inverted_call.def);
            InvertedCall {
                pivot_variable: sub_inverted_call.pivot_variable,
                def: OntosNode {
                    kind: NodeKind::Call(inverted_proc, new_args),
                    meta,
                },
                body: sub_inverted_call.body,
            }
        }
        _ => unimplemented!(),
    }
}

fn merge_target_nodes<'m>(
    u_node: &mut UnificationNode<'m>,
    ctx: &mut UnifyCtx<'_, 'm>,
) -> UnifiedNodes<'m> {
    let mut merged: UnifiedNodes<'m> = Default::default();
    for tagged_node in std::mem::take(&mut u_node.target_nodes) {
        merged.push(tagged_node.into_ontos_node());
    }

    for root_u_node in std::mem::take(&mut u_node.dependents) {
        let unified = unify_node(root_u_node, ctx.root_source, ctx);
        merged.extend(unified.nodes);
    }

    merged
}

fn unify_children<'m>(
    children: &[OntosNode<'m>],
    u_node: UnificationNode<'m>,
    ctx: &mut UnifyCtx<'_, 'm>,
) -> Vec<OntosNode<'m>> {
    u_node
        .sub_unifications
        .into_iter()
        .flat_map(|(index, u_node)| {
            let child = &children[index as usize];

            unify_node(u_node, child, ctx).nodes
        })
        .collect()
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Option<UnifiedNodes<'m>>,
}

fn unify_pattern_binding<'m>(
    u_node: Option<UnificationNode<'m>>,
    source: &OntosNode<'m>,
    ctx: &mut UnifyCtx<'_, 'm>,
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

    let Unified { nodes, binder } = unify_node(u_node, source, ctx);
    let binding = match binder {
        Some(binder) => PatternBinding::Binder(binder.0),
        None => PatternBinding::Wildcard,
    };
    UnifyPatternBinding {
        binding,
        nodes: Some(nodes),
    }
}

use ontol_runtime::vm::proc::BuiltinProc;
use ontos::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern},
    visitor::OntosVisitor,
    Binder, Variable,
};
use smallvec::SmallVec;

use crate::typed_ontos::{
    lang::{Meta, OntosFunc, OntosKind, OntosNode},
    unify::{
        tagged_node::union_bitsets, unification_tree::build_unification_tree,
        var_path::locate_variables,
    },
};

use super::{
    tagged_node::{TaggedNode, Tagger},
    unification_tree::UnificationNode,
    VariableTracker,
};

type UnifiedNodes<'m> = SmallVec<[OntosNode<'m>; 1]>;

pub struct Unified<'m> {
    pub binder: Option<Binder>,
    pub nodes: UnifiedNodes<'m>,
}

struct Unifier<'a, 'm> {
    root_source: &'a OntosNode<'m>,
    next_variable: Variable,
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn alloc_var(&mut self) -> Variable {
        let var = self.next_variable;
        self.next_variable.0 += 1;
        var
    }
}

pub fn unify_to_function<'m>(
    mut source: OntosNode<'m>,
    mut target: OntosNode<'m>,
) -> OntosFunc<'m> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_node(0, &mut source);
    var_tracker.visit_node(0, &mut target);

    let meta = target.meta;

    match target.kind {
        NodeKind::Struct(binder, children) => {
            let Unified {
                nodes,
                binder: input_binder,
            } = unify_tagged_nodes(
                Tagger::default().enter_binder(binder, |ctx| ctx.tag_nodes(children)),
                &mut source,
                var_tracker.next_variable(),
            );

            OntosFunc {
                arg: input_binder.unwrap(),
                body: OntosNode {
                    kind: NodeKind::Struct(binder, nodes.into_iter().collect()),
                    meta,
                },
            }
        }
        kind => {
            todo!("unhandled: {kind}");
        }
    }
}

fn unify_tagged_nodes<'m>(
    tagged_nodes: Vec<TaggedNode<'m>>,
    root_source: &mut OntosNode<'m>,
    next_variable: Variable,
) -> Unified<'m> {
    let free_variables = union_bitsets(tagged_nodes.iter().map(|node| &node.free_variables));
    let u_tree = build_unification_tree(
        tagged_nodes,
        &locate_variables(root_source, &free_variables),
    );

    Unifier {
        root_source,
        next_variable,
    }
    .unify_node(u_tree, root_source)
}

struct InvertedCall<'m> {
    pivot_variable: Variable,
    def: OntosNode<'m>,
    body: UnifiedNodes<'m>,
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Option<UnifiedNodes<'m>>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn unify_node(
        &mut self,
        mut u_node: UnificationNode<'m>,
        source: &OntosNode<'m>,
    ) -> Unified<'m> {
        let OntosNode { kind, meta } = source;
        let meta = *meta;
        match kind {
            NodeKind::VariableRef(var) => {
                let nodes = self.merge_target_nodes(&mut u_node);
                Unified {
                    binder: Some(Binder(*var)),
                    nodes,
                }
            }
            NodeKind::Unit => panic!(),
            NodeKind::Int(_int) => panic!(),
            NodeKind::Call(proc, args) => match u_node.sub_unifications.len() {
                0 => {
                    let args = self.unify_children(args, u_node);
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
                    let subst_var = self.alloc_var();
                    let inverted_call = self.invert_call(proc, subst_var, u_node, args, meta);
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
                let nodes = self.unify_children(nodes, u_node);
                Unified {
                    binder: Some(*binder),
                    nodes: nodes.into(),
                }
            }
            NodeKind::Prop(struct_var, prop, variant) => {
                let rel_binding =
                    self.unify_pattern_binding(u_node.sub_unifications.remove(&0), &variant.rel);
                let val_binding =
                    self.unify_pattern_binding(u_node.sub_unifications.remove(&1), &variant.val);

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

    fn invert_call(
        &mut self,
        proc: &BuiltinProc,
        subst_var: Variable,
        mut u_node: UnificationNode<'m>,
        args: &[OntosNode<'m>],
        meta: Meta<'m>,
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
                let body = self.merge_target_nodes(&mut sub_unification);

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
                let sub_inverted_call = self.invert_call(
                    child_proc,
                    subst_var,
                    sub_unification,
                    child_args,
                    pivot_arg.meta,
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

    fn merge_target_nodes(&mut self, u_node: &mut UnificationNode<'m>) -> UnifiedNodes<'m> {
        let mut merged: UnifiedNodes<'m> = Default::default();
        for tagged_node in std::mem::take(&mut u_node.target_nodes) {
            merged.push(tagged_node.into_ontos_node());
        }

        for root_u_node in std::mem::take(&mut u_node.dependents) {
            let unified = self.unify_node(root_u_node, self.root_source);
            merged.extend(unified.nodes);
        }

        merged
    }

    fn unify_children(
        &mut self,
        children: &[OntosNode<'m>],
        u_node: UnificationNode<'m>,
    ) -> Vec<OntosNode<'m>> {
        u_node
            .sub_unifications
            .into_iter()
            .flat_map(|(index, u_node)| {
                let child = &children[index as usize];

                self.unify_node(u_node, child).nodes
            })
            .collect()
    }

    fn unify_pattern_binding(
        &mut self,
        u_node: Option<UnificationNode<'m>>,
        source: &OntosNode<'m>,
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

        let Unified { nodes, binder } = self.unify_node(u_node, source);
        let binding = match binder {
            Some(binder) => PatternBinding::Binder(binder.0),
            None => PatternBinding::Wildcard,
        };
        UnifyPatternBinding {
            binding,
            nodes: Some(nodes),
        }
    }
}

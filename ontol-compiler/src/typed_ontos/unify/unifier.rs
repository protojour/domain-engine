use ontol_runtime::vm::proc::BuiltinProc;
use ontos::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern, PropVariant},
    visitor::OntosVisitor,
    Binder, Variable,
};
use smallvec::SmallVec;
use tracing::debug;

use crate::typed_ontos::{
    lang::{Meta, OntosFunc, OntosKind, OntosNode, TypedOntos},
    unify::{
        tagged_node::union_free_variables, unification_tree::build_unification_tree,
        var_path::locate_variables,
    },
};

use super::{
    tagged_node::{TaggedNode, Tagger},
    unification_tree::UnificationNode,
    UnifierError, VariableTracker,
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
) -> Result<OntosFunc<'m>, UnifierError> {
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
            )?;

            Ok(OntosFunc {
                arg: input_binder.ok_or(UnifierError::NoInputBinder)?,
                body: OntosNode {
                    kind: NodeKind::Struct(binder, nodes.into_iter().collect()),
                    meta,
                },
            })
        }
        kind => {
            let tagged_node = Tagger::default().tag_node(OntosNode { kind, meta });
            let Unified {
                nodes,
                binder: input_binder,
            } = unify_tagged_nodes(vec![tagged_node], &mut source, var_tracker.next_variable())?;

            let body = match nodes.len() {
                0 => panic!("No nodes"),
                1 => nodes.into_iter().next().unwrap(),
                _ => panic!("Too many nodes"),
            };

            Ok(OntosFunc {
                arg: input_binder.ok_or(UnifierError::NoInputBinder)?,
                body,
            })
        }
    }
}

fn unify_tagged_nodes<'m>(
    tagged_nodes: Vec<TaggedNode<'m>>,
    root_source: &mut OntosNode<'m>,
    next_variable: Variable,
) -> Result<Unified<'m>, UnifierError> {
    let free_variables = union_free_variables(tagged_nodes.as_slice());
    debug!(
        "free_variables: {:?}",
        free_variables
            .iter()
            .map(|i| format!("{}", Variable(i as u32)))
            .collect::<Vec<_>>()
    );
    let var_paths = locate_variables(root_source, &free_variables)?;
    debug!("var_paths: {var_paths:?}");
    let u_tree = build_unification_tree(tagged_nodes, &var_paths);
    debug!("{u_tree:#?}");

    Ok(Unifier {
        root_source,
        next_variable,
    }
    .unify_node(u_tree, root_source))
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
                    let args = self.unify_children(u_node, args);
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
            NodeKind::Map(arg) => match u_node.sub_unifications.remove(&0) {
                None => {
                    let unified_arg = self.unify_node(u_node, arg);
                    let arg: OntosNode = unified_arg.nodes.into_iter().next().unwrap();
                    Unified {
                        binder: unified_arg.binder,
                        nodes: [OntosNode {
                            kind: NodeKind::Map(Box::new(arg)),
                            meta,
                        }]
                        .into(),
                    }
                }
                Some(sub_node) => self.unify_node(sub_node, arg),
            },
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _nodes) => panic!(),
            NodeKind::Struct(binder, nodes) => {
                let nodes = self.unify_children(u_node, nodes);
                Unified {
                    binder: Some(*binder),
                    nodes: nodes.into(),
                }
            }
            NodeKind::Prop(struct_var, id, variants) => {
                let match_arms = u_node
                    .sub_unifications
                    .into_iter()
                    .flat_map(|(index, variant_u_node)| {
                        self.unify_prop_variant_to_match_arm(variant_u_node, &variants[index])
                    })
                    .collect();

                Unified {
                    binder: None,
                    nodes: [OntosNode {
                        kind: OntosKind::MatchProp(*struct_var, *id, match_arms),
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

    fn unify_prop_variant_to_match_arm(
        &mut self,
        mut u_node: UnificationNode<'m>,
        variant: &PropVariant<'m, TypedOntos>,
    ) -> Option<MatchArm<'m, TypedOntos>> {
        if u_node.target_nodes.is_empty() {
            // treat this "transparently"
            let rel_binding =
                self.unify_pattern_binding(u_node.sub_unifications.remove(&0), &variant.rel);
            let val_binding =
                self.unify_pattern_binding(u_node.sub_unifications.remove(&1), &variant.val);

            match (rel_binding.nodes, val_binding.nodes) {
                (None, None) => {
                    debug!("No nodes in variant binding");
                    None
                }
                (Some(rel), None) => Some(MatchArm {
                    pattern: PropPattern::Present(
                        None,
                        rel_binding.binding,
                        PatternBinding::Wildcard,
                    ),
                    nodes: rel.into_iter().collect(),
                }),
                (None, Some(val)) => Some(MatchArm {
                    pattern: PropPattern::Present(
                        None,
                        PatternBinding::Wildcard,
                        val_binding.binding,
                    ),
                    nodes: val.into_iter().collect(),
                }),
                (Some(rel), Some(val)) => {
                    let mut concatenated: Vec<OntosNode> = vec![];
                    concatenated.extend(rel);
                    concatenated.extend(val);
                    Some(MatchArm {
                        pattern: PropPattern::Present(
                            None,
                            rel_binding.binding,
                            val_binding.binding,
                        ),
                        nodes: concatenated,
                    })
                }
            }
        } else {
            // probably(?) a sequence transformation
            None
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
        u_node: UnificationNode<'m>,
        children: &[OntosNode<'m>],
    ) -> Vec<OntosNode<'m>> {
        u_node
            .sub_unifications
            .into_iter()
            .flat_map(|(index, u_node)| self.unify_node(u_node, &children[index]).nodes)
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

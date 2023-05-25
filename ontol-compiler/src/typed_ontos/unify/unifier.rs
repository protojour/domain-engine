use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
use ontos::{
    kind::{
        Attribute, Dimension, MatchArm, NodeKind, PatternBinding, PropPattern, PropVariant, Seq,
    },
    visitor::OntosVisitor,
    Binder, Variable,
};
use smallvec::SmallVec;
use tracing::{debug, trace, warn};

use crate::{
    mem::Intern,
    typed_ontos::{
        lang::{Meta, OntosFunc, OntosKind, OntosNode, TypedBinder, TypedOntos},
        unify::{
            tagged_node::{union_free_variables, DebugVariables},
            unification_tree::build_unification_tree,
            var_path::{locate_slice_variables, locate_variables},
        },
    },
    types::{Type, TypeRef, Types},
    Compiler, SourceSpan,
};

use super::{
    tagged_node::{TaggedKind, TaggedNode, Tagger},
    unification_tree::UnificationNode,
    UnifierError, VariableTracker,
};

type UnifiedNodes<'m> = SmallVec<[OntosNode<'m>; 1]>;

type UnifierResult<T> = Result<T, UnifierError>;

pub struct Unified<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub nodes: UnifiedNodes<'m>,
}

pub fn unify_to_function<'m>(
    source: OntosNode<'m>,
    target: OntosNode<'m>,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<OntosFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_node(0, &source);
    var_tracker.visit_node(0, &target);

    Unifier {
        root_source: &source,
        next_variable: var_tracker.next_variable(),
        types: &mut compiler.types,
    }
    .unify_to_function(target)
}

struct Unifier<'a, 'm> {
    root_source: &'a OntosNode<'m>,
    next_variable: Variable,
    types: &'a mut Types<'m>,
}

struct InvertedCall<'m> {
    pivot_variable: Variable,
    subst_ty: TypeRef<'m>,
    def: OntosNode<'m>,
    body: UnifiedNodes<'m>,
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Option<UnifiedNodes<'m>>,
}

struct TypedMatchArm<'m> {
    arm: MatchArm<'m, TypedOntos>,
    ty: TypeRef<'m>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn alloc_var(&mut self) -> Variable {
        let var = self.next_variable;
        self.next_variable.0 += 1;
        var
    }
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn unify_to_function(&mut self, target: OntosNode<'m>) -> UnifierResult<OntosFunc<'m>> {
        let meta = target.meta;

        match target.kind {
            NodeKind::Struct(binder, children) => {
                let Unified {
                    nodes,
                    binder: input_binder,
                } = self.unify_tagged_nodes(
                    Tagger::default()
                        .enter_binder(binder, |tagger| tagger.tag_nodes(children.into_iter())),
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
                } = self.unify_tagged_nodes(vec![tagged_node])?;

                let body = match nodes.len() {
                    0 => panic!("No nodes"),
                    1 => nodes.into_iter().next().unwrap(),
                    _ => panic!("Too many nodes"),
                };

                debug!("Root input binder: {input_binder:?}");

                Ok(OntosFunc {
                    arg: input_binder.ok_or(UnifierError::NoInputBinder)?,
                    body,
                })
            }
        }
    }

    fn unify_tagged_nodes(
        &mut self,
        tagged_nodes: Vec<TaggedNode<'m>>,
    ) -> UnifierResult<Unified<'m>> {
        let free_variables = union_free_variables(tagged_nodes.as_slice());
        trace!("free_variables: {:?}", DebugVariables(&free_variables));

        let var_paths = locate_variables(self.root_source, &free_variables)?;
        trace!("var_paths: {var_paths:?}");

        let u_tree = build_unification_tree(tagged_nodes, &var_paths);
        trace!("{u_tree:#?}");

        self.unify_node(u_tree, self.root_source)
    }

    fn unify_node(
        &mut self,
        mut u_node: UnificationNode<'m>,
        source: &OntosNode<'m>,
    ) -> UnifierResult<Unified<'m>> {
        let OntosNode { kind, meta } = source;
        let meta = *meta;
        match kind {
            NodeKind::VariableRef(var) => {
                let nodes = self.merge_target_nodes(&mut u_node)?;
                Ok(Unified {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: meta.ty,
                    }),
                    nodes,
                })
            }
            NodeKind::Unit => panic!(),
            NodeKind::Int(_int) => panic!(),
            NodeKind::Call(proc, args) => match u_node.sub_unifications.len() {
                0 => {
                    let args = self.unify_children(u_node, args)?;
                    Ok(Unified {
                        binder: None,
                        nodes: [OntosNode {
                            kind: NodeKind::Call(*proc, args),
                            meta,
                        }]
                        .into(),
                    })
                }
                1 => {
                    let subst_var = self.alloc_var();
                    let inverted_call = self.invert_call(proc, subst_var, u_node, args, meta)?;
                    let return_ty = self.last_type(inverted_call.body.iter());
                    Ok(Unified {
                        binder: Some(TypedBinder {
                            variable: subst_var,
                            ty: inverted_call.subst_ty,
                        }),
                        nodes: [OntosNode {
                            kind: NodeKind::Let(
                                Binder(inverted_call.pivot_variable),
                                Box::new(inverted_call.def),
                                inverted_call.body.into_iter().collect(),
                            ),
                            meta: Meta {
                                ty: return_ty,
                                span: meta.span,
                            },
                        }]
                        .into(),
                    })
                }
                _ => {
                    panic!("Multiple variables in function call!");
                }
            },
            NodeKind::Map(arg) => match u_node.sub_unifications.remove(&0) {
                None => {
                    let unified_arg = self.unify_node(u_node, arg)?;
                    let arg: OntosNode = unified_arg.nodes.into_iter().next().unwrap();
                    Ok(Unified {
                        binder: unified_arg.binder,
                        nodes: [OntosNode {
                            kind: NodeKind::Map(Box::new(arg)),
                            meta,
                        }]
                        .into(),
                    })
                }
                Some(sub_node) => self.unify_node(sub_node, arg),
            },
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, nodes) => {
                let nodes = self.unify_children(u_node, nodes)?;
                Ok(Unified {
                    binder: Some(TypedBinder {
                        variable: binder.0,
                        ty: meta.ty,
                    }),
                    nodes: nodes.into(),
                })
            }
            NodeKind::Prop(struct_var, id, variants) => {
                let mut match_arms = vec![];
                let mut ty = &Type::Tautology;
                for (index, variant_u_node) in u_node.sub_unifications {
                    if let Some(typed_match_arm) =
                        self.unify_prop_variant_to_match_arm(variant_u_node, &variants[index])?
                    {
                        if let Type::Tautology = typed_match_arm.ty {
                            panic!("found Tautology");
                        }

                        ty = typed_match_arm.ty;
                        match_arms.push(typed_match_arm.arm);
                    }
                }

                Ok(Unified {
                    binder: None,
                    nodes: if match_arms.is_empty() {
                        SmallVec::default()
                    } else {
                        if let Type::Tautology = ty {
                            unreachable!();
                        }

                        [OntosNode {
                            kind: OntosKind::MatchProp(*struct_var, *id, match_arms),
                            meta: Meta {
                                ty,
                                span: meta.span,
                            },
                        }]
                        .into()
                    },
                })
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
        u_node: UnificationNode<'m>,
        variant: &PropVariant<'m, TypedOntos>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        if u_node.target_nodes.is_empty() {
            // treat this "transparently"
            self.make_match_arm(u_node, variant, None)
        } else if u_node.target_nodes.len() == 1 {
            if !matches!(&variant.dimension, Dimension::Seq(_)) {
                panic!("BUG: Non-sequence");
            }

            let target = u_node.target_nodes.into_iter().next().unwrap();
            let _meta = target.meta;

            match target.kind {
                TaggedKind::Seq(_) | TaggedKind::PropVariant(_, _, Dimension::Seq(_)) => {
                    let free_variables = union_free_variables(target.children.0.iter());

                    trace!("seq free_variables: {:?}", DebugVariables(&free_variables));

                    let var_paths = locate_slice_variables(
                        &[variant.attr.rel.as_ref(), variant.attr.val.as_ref()],
                        &free_variables,
                    )
                    .unwrap();
                    trace!("seq var_paths: {var_paths:?}");

                    let u_tree = build_unification_tree(target.children.0, &var_paths);

                    trace!("seq u_tree: {u_tree:#?}");

                    Ok(self
                        .make_match_arm(u_tree, variant, Some(Seq))?
                        .map(|mut typed_arm| {
                            typed_arm.ty = self.types.intern(Type::Array(typed_arm.ty));
                            typed_arm
                        }))
                }
                other => panic!("BUG: Unsupported kind: {other:?}"),
            }
        } else {
            panic!("BUG: Many target nodes for prop variant");
        }
    }

    fn make_match_arm(
        &mut self,
        mut u_node: UnificationNode<'m>,
        variant: &PropVariant<'m, TypedOntos>,
        seq: Option<Seq>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        let rel_binding =
            self.unify_pattern_binding(u_node.sub_unifications.remove(&0), &variant.attr.rel)?;
        let val_binding =
            self.unify_pattern_binding(u_node.sub_unifications.remove(&1), &variant.attr.val)?;

        Ok(match (rel_binding.nodes, val_binding.nodes) {
            (None, None) => {
                debug!("No nodes in variant binding");
                None
            }
            (Some(rel), None) => Self::last_type_opt(rel.iter()).map(|ty| TypedMatchArm {
                arm: MatchArm {
                    pattern: PropPattern::Present(
                        seq,
                        rel_binding.binding,
                        PatternBinding::Wildcard,
                    ),
                    nodes: rel.into_iter().collect(),
                },
                ty,
            }),
            (None, Some(val)) => Self::last_type_opt(val.iter()).map(|ty| TypedMatchArm {
                arm: MatchArm {
                    pattern: PropPattern::Present(
                        seq,
                        PatternBinding::Wildcard,
                        val_binding.binding,
                    ),
                    nodes: val.into_iter().collect(),
                },
                ty,
            }),
            (Some(rel), Some(val)) => {
                let mut concatenated: Vec<OntosNode> = vec![];
                concatenated.extend(rel);
                concatenated.extend(val);
                Self::last_type_opt(concatenated.iter()).map(|ty| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Present(
                            seq,
                            rel_binding.binding,
                            val_binding.binding,
                        ),
                        nodes: concatenated,
                    },
                    ty,
                })
            }
        })
    }

    fn invert_call(
        &mut self,
        proc: &BuiltinProc,
        subst_var: Variable,
        mut u_node: UnificationNode<'m>,
        args: &[OntosNode<'m>],
        meta: Meta<'m>,
    ) -> UnifierResult<InvertedCall<'m>> {
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
        debug!("pivot_arg type: {:?}", pivot_arg.meta.ty);
        match &pivot_arg.kind {
            NodeKind::VariableRef(var) => {
                new_args.insert(
                    unification_idx,
                    OntosNode {
                        kind: NodeKind::VariableRef(subst_var),
                        meta: pivot_arg.meta,
                    },
                );
                let body = self.merge_target_nodes(&mut sub_unification)?;

                Ok(InvertedCall {
                    pivot_variable: *var,
                    subst_ty: meta.ty,
                    def: OntosNode {
                        kind: NodeKind::Call(inverted_proc, new_args),
                        meta,
                    },
                    body,
                })
            }
            NodeKind::Call(child_proc, child_args) => {
                let sub_inverted_call = self.invert_call(
                    child_proc,
                    subst_var,
                    sub_unification,
                    child_args,
                    pivot_arg.meta,
                )?;
                new_args.insert(unification_idx, sub_inverted_call.def);
                Ok(InvertedCall {
                    pivot_variable: sub_inverted_call.pivot_variable,
                    subst_ty: meta.ty,
                    def: OntosNode {
                        kind: NodeKind::Call(inverted_proc, new_args),
                        meta,
                    },
                    body: sub_inverted_call.body,
                })
            }
            _ => unimplemented!(),
        }
    }

    fn merge_target_nodes(
        &mut self,
        u_node: &mut UnificationNode<'m>,
    ) -> UnifierResult<UnifiedNodes<'m>> {
        let mut merged: UnifiedNodes<'m> = Default::default();
        let mut property_groups: IndexMap<(Variable, PropertyId), Vec<(Dimension, TaggedNode)>> =
            Default::default();

        for tagged_node in std::mem::take(&mut u_node.target_nodes) {
            match &tagged_node.kind {
                TaggedKind::PropVariant(variable, property_id, dimension) => {
                    property_groups
                        .entry((*variable, *property_id))
                        .or_default()
                        .push((*dimension, tagged_node));
                }
                _ => {
                    merged.push(tagged_node.into_ontos_node());
                }
            }
        }

        for ((variable, property_id), dimension_variants) in property_groups {
            let mut variants = vec![];
            let mut ty = &Type::Tautology;
            for (dimension, variant) in dimension_variants {
                let (rel, val) = variant.children.into_ontos_pair();
                ty = variant.meta.ty;
                if let Type::Tautology = ty {
                    warn!(
                        "Prop variant is Tautology (rel, val): ({:?}, {:?}) ({}, {})",
                        rel.meta.ty, val.meta.ty, rel, val
                    );
                }
                variants.push(PropVariant {
                    dimension,
                    attr: Attribute {
                        rel: Box::new(rel),
                        val: Box::new(val),
                    },
                })
            }

            merged.push(OntosNode {
                kind: NodeKind::Prop(variable, property_id, variants),
                meta: Meta {
                    ty,
                    span: SourceSpan::none(),
                },
            })
        }

        for root_u_node in std::mem::take(&mut u_node.dependents) {
            let unified = self.unify_node(root_u_node, self.root_source)?;
            merged.extend(unified.nodes);
        }

        Ok(merged)
    }

    fn unify_children(
        &mut self,
        u_node: UnificationNode<'m>,
        children: &[OntosNode<'m>],
    ) -> UnifierResult<Vec<OntosNode<'m>>> {
        let mut output = vec![];
        for (index, sub_node) in u_node.sub_unifications {
            output.extend(self.unify_node(sub_node, &children[index])?.nodes);
        }

        Ok(output)
    }

    fn unify_pattern_binding(
        &mut self,
        u_node: Option<UnificationNode<'m>>,
        source: &OntosNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding<'m>> {
        let u_node = match u_node {
            Some(u_node) => u_node,
            None => {
                return Ok(UnifyPatternBinding {
                    binding: PatternBinding::Wildcard,
                    nodes: None,
                })
            }
        };

        let Unified { nodes, binder } = self.unify_node(u_node, source)?;
        let binding = match binder {
            Some(TypedBinder { variable, .. }) => PatternBinding::Binder(variable),
            None => PatternBinding::Wildcard,
        };
        Ok(UnifyPatternBinding {
            binding,
            nodes: Some(nodes),
        })
    }

    fn last_type<'n>(
        &mut self,
        mut iterator: impl Iterator<Item = &'n OntosNode<'m>>,
    ) -> TypeRef<'m>
    where
        'm: 'n,
    {
        if let Some(next) = iterator.next() {
            let mut ty = next.meta.ty;
            for next in iterator {
                ty = next.meta.ty;
            }
            ty
        } else {
            self.types.intern(Type::Unit(DefId::unit()))
        }
    }

    fn last_type_opt<'n>(
        mut iterator: impl Iterator<Item = &'n OntosNode<'m>>,
    ) -> Option<TypeRef<'m>>
    where
        'm: 'n,
    {
        if let Some(next) = iterator.next() {
            let mut ty = next.meta.ty;
            for next in iterator {
                ty = next.meta.ty;
            }
            Some(ty)
        } else {
            None
        }
    }
}

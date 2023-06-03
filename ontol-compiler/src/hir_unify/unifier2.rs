use indexmap::IndexMap;
use ontol_hir::{
    kind::{
        Attribute, Dimension, MatchArm, NodeKind, Optional, PatternBinding, PropPattern,
        PropVariant,
    },
    Binder, Variable,
};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use tracing::{debug, warn};

use crate::{
    hir_unify::unified_target_node::{UnifiedTargetNode, UnifiedTargetNodes},
    typed_hir::{Meta, TypedBinder, TypedHirKind, TypedHirNode},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    tagged_node::TaggedKind,
    unification_tree2::{Nodes, TargetNode},
    unified_target_node::UnifiedTaggedNode,
    unifier::{InvertedCall, TypedMatchArm, Unifier, UnifierResult},
    UnifierError,
};

pub struct Unified2<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub target_nodes: UnifiedTargetNodes<'m>,
}

pub struct UnifyPatternBinding2<'m> {
    binding: PatternBinding,
    target_nodes: Option<UnifiedTargetNodes<'m>>,
}

pub struct LetAttr2<'m> {
    pub rel_binding: PatternBinding,
    pub val_binding: PatternBinding,
    pub target_nodes: UnifiedTargetNodes<'m>,
    pub ty: TypeRef<'m>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    pub fn unify_nodes2(
        &mut self,
        debug_index: Option<usize>,
        mut nodes: Nodes<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        let unscoped_unified = self.unify_unscoped2(&mut nodes, scope_source)?;
        let mut unified = self.unify_scoping2(debug_index, nodes, scope_source)?;

        unified.target_nodes.0.extend(unscoped_unified.0);

        Ok(unified)
    }

    fn unify_scoping2(
        &mut self,
        debug_index: Option<usize>,
        mut nodes: Nodes<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;
        match kind {
            NodeKind::VariableRef(var) => {
                debug!("unify_scoping({debug_index:?}, VariableRef)");
                let unified = self.unify_unscoped2(&mut nodes, scope_source)?;
                Ok(Unified2 {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: meta.ty,
                    }),
                    target_nodes: UnifiedTargetNodes(unified.0.to_vec()),
                })
            }
            NodeKind::Unit => {
                debug!("unify_scoping({debug_index:?}, Unit)");
                let unified = self.unify_unscoped2(&mut nodes, scope_source)?;
                Ok(Unified2 {
                    binder: None,
                    target_nodes: UnifiedTargetNodes(unified.0.to_vec()),
                })
            }
            NodeKind::Int(_int) => panic!(),
            NodeKind::Call(proc, args) => match nodes.sub_scoping.len() {
                0 => {
                    debug!("unify_scoping({debug_index:?}, Call const)");
                    let args = self.unify_sub_scopes2(nodes, args)?;
                    Ok(Unified2 {
                        binder: None,
                        target_nodes: UnifiedTargetNodes(
                            [UnifiedTargetNode::Hir(TypedHirNode {
                                kind: NodeKind::Call(*proc, args.into_hir()),
                                meta,
                            })]
                            .into(),
                        ),
                    })
                }
                1 => {
                    debug!("unify_scoping({debug_index:?}, Call var)");

                    // Invert call algorithm:
                    // start with a _variable_ representing this expression.
                    // Expand this expression along the way recursing _down_ to
                    // find the actual inner variable.
                    // That inner variable is the _binder_ of the resulting `let` expression.
                    let subst_var = self.alloc_var();
                    let inner_expr = TypedHirNode {
                        kind: TypedHirKind::VariableRef(subst_var),
                        meta,
                    };
                    let inverted_call =
                        self.invert_call_recursive2(proc, args, nodes, meta, inner_expr)?;

                    let return_ty = self.last_type(inverted_call.body.iter());
                    Ok(Unified2 {
                        binder: Some(TypedBinder {
                            variable: subst_var,
                            ty: meta.ty,
                        }),
                        target_nodes: UnifiedTargetNodes(
                            [UnifiedTargetNode::Hir(TypedHirNode {
                                kind: NodeKind::Let(
                                    Binder(inverted_call.let_binder),
                                    Box::new(inverted_call.def),
                                    inverted_call.body.into_iter().collect(),
                                ),
                                meta: Meta {
                                    ty: return_ty,
                                    span: meta.span,
                                },
                            })]
                            .into(),
                        ),
                    })
                }
                _ => {
                    panic!("Multiple variables in function call!");
                }
            },
            NodeKind::Map(arg) => match nodes.sub_scoping.remove(&0) {
                None => {
                    debug!("unify_scoping({debug_index:?}, Map const)");

                    let unified_arg = self.unify_scoping2(Some(0), nodes, arg)?;
                    let mut param = unified_arg.target_nodes.into_hir_one();
                    let param_ty = param.meta.ty;
                    param.meta.ty = meta.ty;
                    Ok(Unified2 {
                        binder: unified_arg.binder,
                        target_nodes: UnifiedTargetNodes(
                            [UnifiedTargetNode::Hir(TypedHirNode {
                                kind: NodeKind::Map(Box::new(param)),
                                meta: Meta {
                                    ty: param_ty,
                                    span: meta.span,
                                },
                            })]
                            .into(),
                        ),
                    })
                }
                Some(sub_nodes) => {
                    debug!("unify_scoping({debug_index:?}, Map var)");

                    self.unify_nodes2(Some(0), sub_nodes, arg)
                }
            },
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, child_scopes) => {
                debug!("unify_scoping({debug_index:?}, Struct({}))", binder.0);
                self.unify_struct_scoping2(nodes, *binder, scope_source, child_scopes, meta)
            }
            NodeKind::Prop(optional, struct_var, id, variants) => {
                debug!("unify_scoping({debug_index:?}, Prop)");
                let mut match_arms = vec![];
                let mut ty = &Type::Tautology;
                for (index, variant_u_node) in nodes.sub_scoping {
                    debug!("unify_source_arm(Some({index}))");

                    let PropVariant { dimension, attr } = &variants[index];

                    if let Some(typed_match_arm) =
                        self.unify_prop_variant_scoping(variant_u_node, *dimension, attr)?
                    {
                        // if let Type::Tautology = typed_match_arm.ty {
                        //     panic!("found Tautology");
                        // }

                        ty = typed_match_arm.ty;
                        match_arms.push(typed_match_arm.arm);
                    }
                }

                if optional.0 {
                    match_arms.push(MatchArm {
                        pattern: PropPattern::Absent,
                        nodes: vec![],
                    });
                }

                Ok(Unified2 {
                    binder: None,
                    target_nodes: if match_arms.is_empty() {
                        UnifiedTargetNodes(vec![])
                    } else {
                        // if let Type::Tautology = ty {
                        //     panic!("Tautology");
                        // }

                        UnifiedTargetNodes(
                            [UnifiedTargetNode::Hir(TypedHirNode {
                                kind: TypedHirKind::MatchProp(*struct_var, *id, match_arms),
                                meta: Meta {
                                    ty,
                                    span: meta.span,
                                },
                            })]
                            .into(),
                        )
                    },
                })
            }
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                debug!("unify_scoping({debug_index:?}, Gen)");
                todo!()
            }
            NodeKind::Iter(..) => {
                debug!("unify_scoping({debug_index:?}, Iter)");
                todo!()
            }
            NodeKind::Push(..) => {
                debug!("unify_scoping({debug_index:?}, Push)");
                todo!()
            }
        }
    }

    fn unify_unscoped2(
        &mut self,
        nodes: &mut Nodes<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifiedTargetNodes<'m>> {
        let mut merged: UnifiedTargetNodes<'m> = Default::default();
        let mut property_groups: IndexMap<
            (Variable, PropertyId),
            Vec<(Dimension, UnifiedTaggedNode)>,
        > = Default::default();

        for target_node in std::mem::take(&mut nodes.nodes) {
            match target_node.kind {
                TaggedKind::PropVariant(_, variable, property_id, dimension) => {
                    let unified_tagged_node = self.unify_target_node(target_node, scope_source)?;

                    property_groups
                        .entry((variable, property_id))
                        .or_default()
                        .push((dimension, unified_tagged_node));
                }
                _ => {
                    let unified_tagged_node = self.unify_target_node(target_node, scope_source)?;
                    merged.0.push(unified_tagged_node.into_target());
                }
            }
        }

        for ((variable, property_id), dimension_variants) in property_groups {
            let mut variants = vec![];
            let mut ty = &Type::Tautology;
            for (dimension, variant) in dimension_variants {
                let (rel, val) = variant.children.into_hir_pair();
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

            merged.0.push(UnifiedTargetNode::Hir(TypedHirNode {
                kind: NodeKind::Prop(Optional(false), variable, property_id, variants),
                meta: Meta {
                    ty,
                    span: SourceSpan::none(),
                },
            }))
        }

        for root_u_node in std::mem::take(&mut nodes.dependent_scopes) {
            // FIXME: root_source is wrong. The root should be in dependent_scopes
            let unified = self.unify_nodes2(None, root_u_node, self.root_source)?;
            merged.0.extend(unified.target_nodes.0);
        }

        Ok(merged)
    }

    fn unify_target_node(
        &mut self,
        target_node: TargetNode<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifiedTaggedNode<'m>> {
        let unified = self.unify_nodes2(None, target_node.sub_nodes, scope_source)?;
        Ok(UnifiedTaggedNode {
            kind: target_node.kind,
            meta: target_node.meta,
            children: unified.target_nodes,
        })
    }

    fn unify_sub_scopes2(
        &mut self,
        nodes: Nodes<'m>,
        scope_sources: &[TypedHirNode<'m>],
    ) -> UnifierResult<UnifiedTargetNodes<'m>> {
        let mut output = vec![];
        for (index, sub_nodes) in nodes.sub_scoping {
            output.extend(
                self.unify_nodes2(Some(index), sub_nodes, &scope_sources[index])?
                    .target_nodes
                    .0,
            );
        }

        Ok(UnifiedTargetNodes(output))
    }

    fn unify_struct_scoping2(
        &mut self,
        mut nodes: Nodes<'m>,
        binder: Binder,
        struct_scope: &TypedHirNode<'m>,
        child_scopes: &[TypedHirNode<'m>],
        meta: Meta<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        let unscoped_nodes = self.unify_unscoped2(&mut nodes, struct_scope)?;
        let mut sub_scope_nodes = self.unify_sub_scopes2(nodes, child_scopes)?;

        sub_scope_nodes.0.extend(unscoped_nodes.0);

        Ok(Unified2 {
            binder: Some(TypedBinder {
                variable: binder.0,
                ty: meta.ty,
            }),
            target_nodes: sub_scope_nodes,
        })
    }

    fn invert_call_recursive2(
        &mut self,
        proc: &BuiltinProc,
        args: &[TypedHirNode<'m>],
        mut nodes: Nodes<'m>,
        meta: Meta<'m>,
        inner_expr: TypedHirNode<'m>,
    ) -> UnifierResult<InvertedCall<'m>> {
        if nodes.sub_scoping.len() > 1 {
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

        let mut new_args: Vec<TypedHirNode<'m>> = vec![];

        let (unification_idx, mut sub_nodes) = nodes.sub_scoping.pop_first().unwrap();

        for (index, arg) in args.iter().enumerate() {
            if index != unification_idx {
                new_args.push(arg.clone());
            }
        }

        let pivot_arg = &args[unification_idx];

        match &pivot_arg.kind {
            NodeKind::VariableRef(var) => {
                new_args.insert(unification_idx, inner_expr);
                let body = self.unify_unscoped2(&mut sub_nodes, &args[unification_idx])?;

                Ok(InvertedCall {
                    let_binder: *var,
                    def: TypedHirNode {
                        kind: NodeKind::Call(inverted_proc, new_args),
                        meta,
                    },
                    body: body.into_hir_iter().collect(),
                })
            }
            NodeKind::Call(child_proc, child_args) => {
                new_args.insert(unification_idx, inner_expr);
                let new_inner_expr = TypedHirNode {
                    kind: NodeKind::Call(inverted_proc, new_args),
                    meta,
                };
                self.invert_call_recursive2(
                    child_proc,
                    child_args,
                    sub_nodes,
                    pivot_arg.meta,
                    new_inner_expr,
                )
            }
            _ => unimplemented!(),
        }
    }

    fn unify_prop_variant_scoping(
        &mut self,
        nodes: Nodes<'m>,
        scope_dimension: Dimension,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        if nodes.nodes.is_empty() {
            // treat this "transparently"
            Ok(self
                .let_attr2(nodes, scope_attr)?
                .map(|let_attr| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Attr(let_attr.rel_binding, let_attr.val_binding),
                        nodes: let_attr.target_nodes.into_hir_iter().collect(),
                    },
                    ty: let_attr.ty,
                }))
        } else if nodes.nodes.len() == 1 {
            todo!();

            /*
            if !matches!(scope_dimension, Dimension::Seq(_)) {
                panic!("BUG: Non-sequence");
            }

            let expr = nodes.nodes.into_iter().next().unwrap();
            let _meta = expr.meta;

            match expr.kind {
                TaggedKind::Seq(_) => {
                    let mut iterator = expr.children.0.into_iter();
                    let rel_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.rel)?;
                    let val_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.val)?;

                    let input_seq_var = self.alloc_var();
                    let output_seq_var = self.alloc_var();

                    Ok(self
                        .let_attr_pair(rel_binding, val_binding)?
                        .map(|let_attr| {
                            // FIXME: Array/seq types must take two parameters
                            let seq_ty = self.types.intern(Type::Array(let_attr.val.ty));

                            let gen_node = TypedHirNode {
                                kind: NodeKind::Gen(
                                    input_seq_var,
                                    IterBinder {
                                        seq: PatternBinding::Binder(output_seq_var),
                                        rel: let_attr.rel.binding,
                                        val: let_attr.val.binding,
                                    },
                                    vec![TypedHirNode {
                                        kind: NodeKind::Push(
                                            output_seq_var,
                                            Attribute {
                                                rel: Box::new(self.single_node_or_unit(
                                                    let_attr.rel.nodes.into_iter(),
                                                )),
                                                val: Box::new(self.single_node_or_unit(
                                                    let_attr.val.nodes.into_iter(),
                                                )),
                                            },
                                        ),
                                        meta: Meta {
                                            ty: self.unit_type(),
                                            span: SourceSpan::none(),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: seq_ty,
                                    span: SourceSpan::none(),
                                },
                            };

                            TypedMatchArm {
                                arm: MatchArm {
                                    pattern: PropPattern::Seq(PatternBinding::Binder(
                                        input_seq_var,
                                    )),
                                    nodes: vec![gen_node],
                                },
                                ty: seq_ty,
                            }
                        }))
                }
                TaggedKind::PropVariant(_, struct_var, prop_id, Dimension::Seq(_)) => {
                    let mut iterator = expr.children.0.into_iter();
                    let rel_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.rel)?;
                    let val_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.val)?;

                    let input_seq_var = self.alloc_var();
                    let output_seq_var = self.alloc_var();

                    Ok(self
                        .let_attr_pair(rel_binding, val_binding)?
                        .map(|let_attr| {
                            // FIXME: Array/seq types must take two parameters
                            let seq_ty = self.types.intern(Type::Array(let_attr.val.ty));

                            let gen_node = TypedHirNode {
                                kind: NodeKind::Gen(
                                    input_seq_var,
                                    IterBinder {
                                        seq: PatternBinding::Binder(output_seq_var),
                                        rel: let_attr.rel.binding,
                                        val: let_attr.val.binding,
                                    },
                                    vec![TypedHirNode {
                                        kind: NodeKind::Push(
                                            output_seq_var,
                                            Attribute {
                                                rel: Box::new(self.single_node_or_unit(
                                                    let_attr.rel.nodes.into_iter(),
                                                )),
                                                val: Box::new(self.single_node_or_unit(
                                                    let_attr.val.nodes.into_iter(),
                                                )),
                                            },
                                        ),
                                        meta: Meta {
                                            ty: self.unit_type(),
                                            span: SourceSpan::none(),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: seq_ty,
                                    span: SourceSpan::none(),
                                },
                            };

                            let prop_node = TypedHirNode {
                                // FIXME: It feels wrong to construct the NodeKind::Prop explicitly here.
                                // this node should already be in target_nodes, so what should instead be done
                                // is to put its variables into scope.
                                kind: NodeKind::Prop(
                                    Optional(false),
                                    struct_var,
                                    prop_id,
                                    vec![PropVariant {
                                        dimension: Dimension::Singular,
                                        attr: Attribute {
                                            rel: Box::new(self.unit_node()),
                                            val: Box::new(gen_node),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: self.unit_type(),
                                    span: SourceSpan::none(),
                                },
                            };

                            TypedMatchArm {
                                arm: MatchArm {
                                    pattern: PropPattern::Seq(PatternBinding::Binder(
                                        input_seq_var,
                                    )),
                                    nodes: vec![prop_node],
                                },
                                ty: seq_ty,
                            }
                        }))
                }
                other => panic!("BUG: Unsupported kind: {other:?}"),
            }
            */
        } else {
            panic!("BUG: Many target nodes for prop variant");
        }
    }

    fn let_attr2(
        &mut self,
        mut nodes: Nodes<'m>,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<LetAttr2<'m>>> {
        let rel_binding =
            self.unify_pattern_binding2(Some(0), nodes.sub_scoping.remove(&0), &scope_attr.rel)?;
        let val_binding =
            self.unify_pattern_binding2(Some(1), nodes.sub_scoping.remove(&1), &scope_attr.val)?;

        Ok(match (rel_binding.target_nodes, val_binding.target_nodes) {
            (None, None) => {
                debug!("No nodes in variant binding");
                None
            }
            (Some(rel), None) => rel.type_iter().last().map(|ty| LetAttr2 {
                rel_binding: rel_binding.binding,
                val_binding: PatternBinding::Wildcard,
                target_nodes: rel,
                ty,
            }),
            (None, Some(val)) => val.type_iter().last().map(|ty| LetAttr2 {
                rel_binding: PatternBinding::Wildcard,
                val_binding: val_binding.binding,
                target_nodes: val,
                ty,
            }),
            (Some(rel), Some(val)) => {
                let mut concatenated = UnifiedTargetNodes::default();
                concatenated.0.extend(rel.0);
                concatenated.0.extend(val.0);
                concatenated.type_iter().last().map(|ty| LetAttr2 {
                    rel_binding: rel_binding.binding,
                    val_binding: val_binding.binding,
                    target_nodes: concatenated,
                    ty,
                })
            }
        })
    }

    fn unify_pattern_binding2(
        &mut self,
        debug_index: Option<usize>,
        nodes: Option<Nodes<'m>>,
        source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding2<'m>> {
        let nodes = match nodes {
            Some(nodes) => nodes,
            None => {
                return Ok(UnifyPatternBinding2 {
                    binding: PatternBinding::Wildcard,
                    target_nodes: None,
                })
            }
        };

        let Unified2 {
            target_nodes,
            binder,
        } = self.unify_nodes2(debug_index, nodes, source)?;
        let binding = match binder {
            Some(TypedBinder { variable, .. }) => PatternBinding::Binder(variable),
            None => PatternBinding::Wildcard,
        };
        Ok(UnifyPatternBinding2 {
            binding,
            target_nodes: Some(target_nodes),
        })
    }
}

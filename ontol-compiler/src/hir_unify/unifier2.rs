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
    hir_unify::{
        u_node::UNodeKind,
        unified_target_node::{UnifiedTargetNode, UnifiedTargetNodes},
    },
    typed_hir::{Meta, TypedBinder, TypedHirKind, TypedHirNode},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    tagged_node::TaggedKind,
    u_node::{BlockUNodeKind, ExprUNodeKind, LeafUNodeKind, UBlockBody, UNode},
    unified_target_node::UnifiedTaggedNode,
    unifier::{InvertedCall, TypedMatchArm, UnifiedNodes, Unifier, UnifierResult},
    UnifierError,
};

pub struct Unified2<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

pub enum ScopeUnification<'s, 'm> {
    Unified(Unified2<'m>),
    Lazy(UNode<'m>, ScopeSource<'s, 'm>),
}

#[derive(Clone, Copy)]
pub enum ScopeSource<'s, 'm> {
    Absent,
    Node(&'s TypedHirNode<'m>),
    Block(TypedBinder<'m>, &'s [TypedHirNode<'m>]),
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

impl<'s, 'm> Unifier<'s, 'm> {
    pub fn unify2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<Unified2<'m>> {
        match scope_source {
            ScopeSource::Absent => self.unify_u_node2(u_node, scope_source),
            ScopeSource::Node(scope_node) => match self.unify_scoping2(scope_node, u_node)? {
                ScopeUnification::Unified(unified) => Ok(unified),
                ScopeUnification::Lazy(u_node, scope_source) => {
                    self.unify_u_node2(u_node, scope_source)
                }
            },
            ScopeSource::Block(..) => self.unify_u_node2(u_node, scope_source),
        }
    }

    fn unify_u_node2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<Unified2<'m>> {
        let meta = u_node.meta;
        match (u_node.kind, scope_source) {
            (UNodeKind::Stolen, _) => panic!("Stolen: Should not happen"),
            (UNodeKind::Leaf(LeafUNodeKind::VariableRef(var)), _) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::VariableRef(var),
                    meta,
                },
            }),
            (UNodeKind::Leaf(LeafUNodeKind::Int(int)), _) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta,
                },
            }),
            (UNodeKind::Leaf(LeafUNodeKind::Unit), _) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta,
                },
            }),
            (UNodeKind::Block(kind, body), ScopeSource::Block(scope_binder, scope)) => {
                let body = self.unify_u_block_body(body, scope)?;

                match kind {
                    BlockUNodeKind::Raw => todo!("Raw"),
                    BlockUNodeKind::Let(..) => todo!("Let"),
                    BlockUNodeKind::Struct(struct_binder) => Ok(Unified2 {
                        binder: Some(scope_binder),
                        node: TypedHirNode {
                            kind: NodeKind::Struct(struct_binder, body),
                            meta,
                        },
                    }),
                }
            }
            (UNodeKind::Block(..), _) => panic!("Invalid scope source for block u_node"),
            (UNodeKind::Expr(kind, params), _) => {
                let mut hir_params = vec![];
                for u_param in params {
                    hir_params.push(self.unify2(u_param, scope_source)?.node);
                }

                let hir_kind = match kind {
                    ExprUNodeKind::Call(proc) => NodeKind::Call(proc, hir_params),
                    ExprUNodeKind::Map => {
                        NodeKind::Map(Box::new(hir_params.into_iter().next().unwrap()))
                    }
                };

                Ok(Unified2 {
                    binder: None,
                    node: TypedHirNode {
                        kind: hir_kind,
                        meta,
                    },
                })
            }
            (UNodeKind::Attr(kind, attr), _) => todo!(),
            (UNodeKind::SubScope(subscope_idx, _), _) => todo!(),
        }
    }

    fn unify_u_block_body(
        &mut self,
        body: UBlockBody<'m>,
        scope_slice: &'s [TypedHirNode<'m>],
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let unit_type = self.unit_type();
        let mut output = vec![];

        for (subscope_idx, sub_body) in body.sub_scoping {
            let subscope = &scope_slice[subscope_idx];
            let scope_unification = self
                .unify_scoping2(
                    subscope,
                    UNode {
                        kind: UNodeKind::Block(BlockUNodeKind::Raw, sub_body),
                        free_variables: Default::default(),
                        meta: Meta {
                            ty: unit_type,
                            span: SourceSpan::none(),
                        },
                    },
                )
                .unwrap();

            match scope_unification {
                ScopeUnification::Unified(unified) => {
                    output.push(unified.node);
                }
                ScopeUnification::Lazy(
                    UNode {
                        kind: UNodeKind::Block(_, sub_body),
                        ..
                    },
                    ScopeSource::Block(_, sub_scope_slice),
                ) => {
                    output.extend(self.unify_u_block_body(sub_body, sub_scope_slice)?);
                }
                _ => panic!("Bad lazy block scope source"),
            }
        }

        Ok(output)
    }

    fn unify_scoping2(
        &mut self,
        scope_source: &'s TypedHirNode<'m>,
        u_node: UNode<'m>,
    ) -> UnifierResult<ScopeUnification<'s, 'm>> {
        let debug_index = "n/a";
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;

        match kind {
            NodeKind::VariableRef(var) => {
                debug!("unify_scoping({debug_index:?}, VariableRef)");
                let unified = self.unify_u_node2(u_node, ScopeSource::Absent)?;
                Ok(ScopeUnification::Unified(Unified2 {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: meta.ty,
                    }),
                    node: unified.node,
                }))
            }
            NodeKind::Unit => {
                debug!("unify_scoping({debug_index:?}, Unit)");
                let unified = self.unify_u_node2(u_node, ScopeSource::Absent)?;
                Ok(ScopeUnification::Unified(unified))
            }
            NodeKind::Int(_int) => panic!("Int in scoping"),
            NodeKind::Call(proc, args) => {
                let unified = self.unify_call_scope((*proc, args), meta, u_node)?;
                Ok(ScopeUnification::Unified(unified))
            }
            NodeKind::Map(arg) => todo!(),
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, child_scopes) => {
                debug!("unify_scoping({debug_index:?}, Struct({}))", binder.0);
                Ok(ScopeUnification::Lazy(
                    u_node,
                    ScopeSource::Block(
                        TypedBinder {
                            variable: binder.0,
                            ty: meta.ty,
                        },
                        child_scopes,
                    ),
                ))
            }
            NodeKind::Prop(optional, struct_var, id, variants) => {
                let mut match_arms = vec![];
                let mut ty = &Type::Tautology;

                match u_node.kind {
                    UNodeKind::Block(_, u_block) => {
                        for (subscope_idx, sub_block) in u_block.sub_scoping {
                            let PropVariant { dimension, attr } = &variants[subscope_idx];

                            if let Some(typed_match_arm) =
                                self.unify_prop_variant_scoping(sub_block, *dimension, attr)?
                            {
                                // if let Type::Tautology = typed_match_arm.ty {
                                //     panic!("found Tautology");
                                // }

                                ty = typed_match_arm.ty;
                                match_arms.push(typed_match_arm.arm);
                            }
                        }
                    }
                    _ => unimplemented!(),
                }

                Ok(ScopeUnification::Unified(Unified2 {
                    binder: None,
                    node: if match_arms.is_empty() {
                        TypedHirNode {
                            kind: NodeKind::Unit,
                            meta,
                        }
                    } else {
                        if let Type::Tautology = ty {
                            panic!("Tautology");
                        }

                        TypedHirNode {
                            kind: NodeKind::MatchProp(*struct_var, *id, match_arms),
                            meta: Meta {
                                ty,
                                span: meta.span,
                            },
                        }
                    },
                }))
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

    fn unify_call_scope(
        &mut self,
        scope_call: (BuiltinProc, &'s [TypedHirNode<'m>]),
        scope_meta: Meta<'m>,
        u_node: UNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        match u_node.kind {
            UNodeKind::SubScope(subscope_idx, child_u_node) => {
                let binder_var = self.alloc_var();
                let inner_expr = TypedHirNode {
                    kind: TypedHirKind::VariableRef(binder_var),
                    meta: scope_meta,
                };
                let inverted_call = self.invert_call(
                    scope_call,
                    scope_meta,
                    subscope_idx,
                    *child_u_node,
                    inner_expr,
                )?;
                let return_ty = self.last_type(inverted_call.body.iter());

                Ok(Unified2 {
                    binder: Some(TypedBinder {
                        variable: binder_var,
                        ty: scope_meta.ty,
                    }),
                    node: TypedHirNode {
                        kind: NodeKind::Let(
                            Binder(inverted_call.let_binder),
                            Box::new(inverted_call.def),
                            inverted_call.body.into_iter().collect(),
                        ),
                        meta: Meta {
                            ty: return_ty,
                            span: scope_meta.span,
                        },
                    },
                })
            }
            _ => self.unify_u_node2(u_node, ScopeSource::Absent),
        }
    }

    fn invert_call(
        &mut self,
        (scope_proc, scope_args): (BuiltinProc, &'s [TypedHirNode<'m>]),
        scope_meta: Meta<'m>,
        subscope_idx: usize,
        u_node: UNode<'m>,
        inner_expr: TypedHirNode<'m>,
    ) -> UnifierResult<InvertedCall<'m>> {
        // FIXME: Properly check this
        let inverted_proc = match scope_proc {
            BuiltinProc::Add => BuiltinProc::Sub,
            BuiltinProc::Sub => BuiltinProc::Add,
            BuiltinProc::Mul => BuiltinProc::Div,
            BuiltinProc::Div => BuiltinProc::Mul,
            _ => panic!("Unsupported procedure; cannot invert {scope_proc:?}"),
        };

        let mut inverted_args: Vec<TypedHirNode<'m>> = vec![];

        for (index, arg) in scope_args.iter().enumerate() {
            if index != subscope_idx {
                inverted_args.push(arg.clone());
            }
        }

        let pivot_arg = &scope_args[subscope_idx];

        match &pivot_arg.kind {
            NodeKind::VariableRef(var) => {
                inverted_args.insert(subscope_idx, inner_expr);
                let body = self.unify2(u_node, ScopeSource::Node(&scope_args[subscope_idx]))?;

                Ok(InvertedCall {
                    let_binder: *var,
                    def: TypedHirNode {
                        kind: NodeKind::Call(inverted_proc, inverted_args),
                        meta: scope_meta,
                    },
                    body: [body.node].into(),
                })
            }
            NodeKind::Call(next_scope_proc, next_scope_args) => {
                inverted_args.insert(subscope_idx, inner_expr);
                let next_inner_expr = TypedHirNode {
                    kind: NodeKind::Call(inverted_proc, inverted_args),
                    meta: scope_meta,
                };
                match u_node.kind {
                    UNodeKind::SubScope(next_subscope_idx, next_u_node) => self.invert_call(
                        (*next_scope_proc, next_scope_args),
                        pivot_arg.meta,
                        next_subscope_idx,
                        *next_u_node,
                        next_inner_expr,
                    ),
                    kind => unimplemented!("re-entrant invert_call u_node: {kind:?}"),
                }
            }
            _ => unimplemented!("Unhandled pivot_arg"),
        }
    }

    fn unify_prop_variant_scoping(
        &mut self,
        u_block: UBlockBody<'m>,
        _scope_dimension: Dimension,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        if u_block.nodes.is_empty() {
            // treat this "transparently"
            Ok(self
                .let_attr2(u_block, scope_attr)?
                .map(|let_attr| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Attr(let_attr.rel_binding, let_attr.val_binding),
                        nodes: let_attr.target_nodes.into_hir_iter().collect(),
                    },
                    ty: let_attr.ty,
                }))
        } else {
            todo!()
        }
    }

    fn let_attr2(
        &mut self,
        mut u_block: UBlockBody<'m>,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<LetAttr2<'m>>> {
        let rel_binding =
            self.unify_pattern_binding2(Some(0), u_block.sub_scoping.remove(&0), &scope_attr.rel)?;
        let val_binding =
            self.unify_pattern_binding2(Some(1), u_block.sub_scoping.remove(&1), &scope_attr.val)?;

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
        u_block: Option<UBlockBody<'m>>,
        source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding2<'m>> {
        let u_block = match u_block {
            Some(u_block) => u_block,
            None => {
                return Ok(UnifyPatternBinding2 {
                    binding: PatternBinding::Wildcard,
                    target_nodes: None,
                })
            }
        };

        /*
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
        */
        todo!("u_block: {u_block:?}")
    }
}

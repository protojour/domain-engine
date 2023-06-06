use ontol_hir::kind::{NodeKind, PatternBinding, PropVariant};
use tracing::debug;

use crate::{
    hir_unify::{u_node::UNodeKind, unified_target_node::UnifiedTargetNodes},
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    u_node::{BlockUNodeKind, ExprUNodeKind, LeafUNodeKind, UBlockBody, UNode},
    unifier::{Unifier, UnifierResult},
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
    pub binding: PatternBinding,
    pub target_nodes: Option<UnifiedTargetNodes<'m>>,
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

    pub(super) fn unify_u_node2(
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
}

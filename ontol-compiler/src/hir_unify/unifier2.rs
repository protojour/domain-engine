use ontol_hir::kind::{Attribute, Dimension, NodeKind, Optional, PatternBinding, PropVariant};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use tracing::{debug, warn};

use crate::{
    hir_unify::u_node::UNodeKind,
    typed_hir::{Meta, TypedBinder, TypedHir, TypedHirNode},
    types::TypeRef,
    SourceSpan,
};

use super::{
    scope::hir_subscope,
    u_node::{AttrUNodeKind, BlockUNodeKind, ExprUNodeKind, LeafUNodeKind, UBlockBody, UNode},
    unifier::{Unifier, UnifierResult},
    UnifierError,
};

pub struct UnifiedNode<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

pub struct UnifiedBlock<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub block: Block<'m>,
}

#[derive(Default)]
pub struct Block<'m>(pub Vec<TypedHirNode<'m>>);

pub enum ScopeUnification<'s, 'm> {
    Unified(UnifiedNode<'m>),
    Lazy(UNode<'m>, ScopeSource<'s, 'm>),
}

#[derive(Clone, Copy)]
pub enum ScopeSource<'s, 'm> {
    Absent,
    Node(&'s TypedHirNode<'m>),
    Block(TypedBinder<'m>, &'s [TypedHirNode<'m>]),
}

impl<'s, 'm> ScopeSource<'s, 'm> {
    fn subscope(&self, index: usize) -> &'s TypedHirNode<'m> {
        match self {
            Self::Absent => panic!("Cannot subscope Absent scope"),
            Self::Node(node) => hir_subscope(node, index),
            Self::Block(_, slice) => &slice[index],
        }
    }
}

#[derive(Clone)]
pub enum Scoping<'s, 'm> {
    Fulfilled,
    SubScope(usize, Box<Scoping<'s, 'm>>),
    Binder(TypedBinder<'m>),
    InvertCall(BuiltinProc, &'s [TypedHirNode<'m>], Meta<'m>),
    Block(TypedBinder<'m>, &'s [TypedHirNode<'m>]),
    Prop(
        Optional,
        ontol_hir::Var,
        PropertyId,
        &'s [PropVariant<'m, TypedHir>],
        Meta<'m>,
    ),
}

pub struct UnifyPatternBinding2<'m> {
    pub binding: PatternBinding,
    pub block: Option<Block<'m>>,
}

pub struct LetAttr2<'m> {
    pub rel_binding: PatternBinding,
    pub val_binding: PatternBinding,
    pub target_nodes: Block<'m>,
    pub ty: TypeRef<'m>,
}

impl<'m> Block<'m> {
    pub fn type_iter<'a>(&'a self) -> impl Iterator<Item = TypeRef<'m>> + 'a {
        self.0.iter().map(|node| node.meta.ty)
    }
}

impl<'s, 'm> Unifier<'s, 'm> {
    pub fn unify2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        match scope_source {
            ScopeSource::Absent => self.unify_u_node2(u_node, scope_source),
            ScopeSource::Node(scope_node) => {
                let scoping = self.classify_scoping(scope_node)?;
                self.unify_scoping2(scoping, u_node)
            }
            ScopeSource::Block(..) => self.unify_u_node2(u_node, scope_source),
        }
    }

    pub(super) fn unify_u_node2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let meta = u_node.meta;
        match (u_node.kind, scope_source) {
            (UNodeKind::Stolen, _) => panic!("Stolen: Should not happen"),
            (UNodeKind::Leaf(LeafUNodeKind::VariableRef(var)), _) => Ok(UnifiedNode {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Var(var),
                    meta,
                },
            }),
            (UNodeKind::Leaf(LeafUNodeKind::Int(int)), _) => Ok(UnifiedNode {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta,
                },
            }),
            (UNodeKind::Leaf(LeafUNodeKind::Unit), _) => Ok(UnifiedNode {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta,
                },
            }),
            (UNodeKind::Block(kind, mut u_block_body), scope_source) => {
                debug!("unify_unode Block {kind:?}");
                let mut body = self.unify_u_block_subscopes(&mut u_block_body, scope_source)?;
                body.extend(
                    self.unify_u_block_nodes(&mut u_block_body, scope_source)?
                        .block
                        .0,
                );

                match kind {
                    BlockUNodeKind::Raw => {
                        if body.len() != 1 {
                            panic!(
                                "Raw block should have 1 node, had {} (TODO: (begin) blocks?)",
                                body.len()
                            );
                        }

                        Ok(UnifiedNode {
                            binder: None,
                            node: body.into_iter().next().unwrap(),
                        })
                    }
                    BlockUNodeKind::Let(..) => todo!("Let"),
                    BlockUNodeKind::Struct(struct_binder) => Ok(UnifiedNode {
                        binder: match scope_source {
                            ScopeSource::Block(binder, _) => Some(binder),
                            _ => None,
                        },
                        node: TypedHirNode {
                            kind: NodeKind::Struct(struct_binder, body),
                            meta,
                        },
                    }),
                }
            }
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

                Ok(UnifiedNode {
                    binder: None,
                    node: TypedHirNode {
                        kind: hir_kind,
                        meta,
                    },
                })
            }
            (UNodeKind::Attr(kind, attr), scope_source) => {
                let rel = self.unify_u_node2(*attr.rel, scope_source)?;
                let val = self.unify_u_node2(*attr.val, scope_source)?;

                match kind {
                    AttrUNodeKind::PropVariant(_, struct_var, property_id) => Ok(UnifiedNode {
                        binder: None,
                        node: TypedHirNode {
                            kind: NodeKind::Prop(
                                Optional(false),
                                struct_var,
                                property_id,
                                vec![PropVariant {
                                    dimension: Dimension::Singular,
                                    attr: Attribute {
                                        rel: Box::new(rel.node),
                                        val: Box::new(val.node),
                                    },
                                }],
                            ),
                            meta,
                        },
                    }),
                    AttrUNodeKind::Seq(_) => todo!(),
                }
            }
            (UNodeKind::SubScope(_, _), _) => todo!(),
        }
    }

    fn unify_scoping2(
        &mut self,
        scoping: Scoping<'s, 'm>,
        u_node: UNode<'m>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        match scoping {
            Scoping::Fulfilled => self.unify_u_node2(u_node, ScopeSource::Absent),
            Scoping::SubScope(subscope_idx, inner_scoping) => match u_node.kind {
                UNodeKind::SubScope(actual_idx, inner_u_node) => {
                    assert_eq!(subscope_idx, actual_idx);
                    self.unify_scoping2(*inner_scoping, *inner_u_node)
                }
                UNodeKind::Block(_, mut body) => {
                    let unit_ty = self.unit_type();
                    let inner_body = body.sub_scoping.remove(&subscope_idx).unwrap();
                    self.unify_scoping2(
                        *inner_scoping,
                        UNode {
                            kind: UNodeKind::Block(BlockUNodeKind::Raw, inner_body),
                            free_variables: Default::default(),
                            meta: Meta {
                                ty: unit_ty,
                                span: SourceSpan::none(),
                            },
                        },
                    )
                }
                _ => self.unify_scoping2(*inner_scoping, u_node),
                // _ => {
                //     panic!("Cannot subscope {u_node:?}")
                // }
            },
            Scoping::Binder(typed_binder) => {
                let unified = self.unify_u_node2(u_node, ScopeSource::Absent)?;
                Ok(UnifiedNode {
                    binder: Some(typed_binder),
                    node: unified.node,
                })
            }
            Scoping::InvertCall(proc, args, meta) => {
                self.unify_call_scope((proc, args), meta, u_node)
            }
            Scoping::Block(typed_binder, scope_nodes) => {
                self.unify_u_node2(u_node, ScopeSource::Block(typed_binder, scope_nodes))
            }
            Scoping::Prop(optional, struct_var, property_id, variants, scope_meta) => self
                .unify_prop_scope(
                    optional,
                    struct_var,
                    property_id,
                    variants,
                    scope_meta,
                    u_node,
                ),
        }
    }

    pub(super) fn unify_u_block(
        &mut self,
        mut block_body: UBlockBody<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<UnifiedBlock<'m>> {
        debug!("unify_u_block {block_body:?}");

        if block_body.sub_scoping.len() == 1
            && block_body.nodes.is_empty()
            && block_body.dependent_scopes.is_empty()
        {
            match &scope_source {
                ScopeSource::Absent => debug!("unify_u_block(1) Absent scope"),
                ScopeSource::Block(..) => debug!("unify_u_block(1) Block scope"),
                ScopeSource::Node(..) => debug!("unify_u_block(1) Node scope"),
            }

            // let (subscope_idx, sub_block) = body.sub_scoping.pop_first().unwrap();
            let unit_type = self.unit_type();
            let unified_node = self.unify2(
                UNode {
                    kind: UNodeKind::Block(BlockUNodeKind::Raw, block_body),
                    free_variables: Default::default(),
                    meta: Meta {
                        ty: unit_type,
                        span: SourceSpan::none(),
                    },
                },
                scope_source,
            )?;

            Ok(UnifiedBlock {
                binder: unified_node.binder,
                block: Block(vec![unified_node.node]),
            })
        } else {
            debug!("unify_u_block: second arm");
            let mut nodes = vec![];

            let binder = match scope_source {
                ScopeSource::Absent => None,
                ScopeSource::Node(scope_node) => match self.classify_scoping(scope_node)? {
                    Scoping::Binder(binder) => Some(binder),
                    Scoping::Block(binder, _) => Some(binder),
                    _ => None,
                },
                ScopeSource::Block(typed_binder, _) => Some(typed_binder),
            };

            nodes.extend(self.unify_u_block_subscopes(&mut block_body, scope_source)?);
            nodes.extend(
                self.unify_u_block_nodes(&mut block_body, scope_source)?
                    .block
                    .0,
            );

            warn!("untested(2)!");

            Ok(UnifiedBlock {
                binder,
                block: Block(nodes),
            })
        }
    }

    fn unify_u_block_subscopes(
        &mut self,
        body: &mut UBlockBody<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let unit_type = self.unit_type();
        let mut output = vec![];

        for (subscope_idx, mut sub_body) in std::mem::take(&mut body.sub_scoping) {
            let subscope = scope_source.subscope(subscope_idx);
            match self.classify_scoping(subscope)? {
                Scoping::Block(typed_binder, sub_scope_slice) => {
                    // BUG: probably not correct?
                    output.extend(self.unify_u_block_subscopes(
                        &mut sub_body,
                        ScopeSource::Block(typed_binder, sub_scope_slice),
                    )?);
                }
                scoping => {
                    let unified = self.unify_scoping2(
                        scoping,
                        UNode {
                            kind: UNodeKind::Block(BlockUNodeKind::Raw, sub_body),
                            free_variables: Default::default(),
                            meta: Meta {
                                ty: unit_type,
                                span: SourceSpan::none(),
                            },
                        },
                    )?;

                    output.push(unified.node);
                }
            }
        }

        Ok(output)
    }

    pub(super) fn unify_u_block_nodes(
        &mut self,
        body: &mut UBlockBody<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<UnifiedBlock<'m>> {
        let mut output = vec![];

        for u_node in std::mem::take(&mut body.nodes) {
            output.push(self.unify2(u_node, scope_source)?.node);
        }

        // TODO: dependent scopes

        Ok(UnifiedBlock {
            binder: None,
            block: Block(output),
        })
    }

    fn classify_scoping(
        &mut self,
        scope_source: &'s TypedHirNode<'m>,
    ) -> UnifierResult<Scoping<'s, 'm>> {
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;

        match kind {
            NodeKind::Var(var) => {
                debug!("classify_scoping(VariableRef)");
                Ok(Scoping::Binder(TypedBinder {
                    var: *var,
                    ty: meta.ty,
                }))
            }
            NodeKind::Unit => {
                debug!("classify_scoping(Unit)");
                Ok(Scoping::Fulfilled)
            }
            NodeKind::Int(_int) => panic!("Int in scoping"),
            NodeKind::Call(proc, args) => {
                debug!("classify_scoping(Call)");
                Ok(Scoping::InvertCall(*proc, args, meta))
            }
            NodeKind::Map(arg) => {
                debug!("classify_scoping(Map)...");
                let inner = self.classify_scoping(arg)?;
                Ok(Scoping::SubScope(0, Box::new(inner)))
            }
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, child_scopes) => {
                debug!("classify_scoping(Struct({}))", binder.0);
                Ok(Scoping::Block(
                    TypedBinder {
                        var: binder.0,
                        ty: meta.ty,
                    },
                    child_scopes,
                ))
            }
            NodeKind::Prop(optional, struct_var, id, variants) => {
                Ok(Scoping::Prop(*optional, *struct_var, *id, variants, meta))
            }
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                debug!("classify_scoping(Gen)");
                todo!()
            }
            NodeKind::Iter(..) => {
                debug!("classify_scoping(Iter)");
                todo!()
            }
            NodeKind::Push(..) => {
                debug!("classify_scoping(Push)");
                todo!()
            }
        }
    }
}

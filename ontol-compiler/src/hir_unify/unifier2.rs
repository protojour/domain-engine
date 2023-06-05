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
    u_node::{ExprUNodeKind, LeafUNodeKind, UNode},
    unified_target_node::UnifiedTaggedNode,
    unifier::{InvertedCall, TypedMatchArm, Unifier, UnifierResult},
    UnifierError,
};

pub struct Unified2<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
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
    pub fn unify2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: Option<&TypedHirNode<'m>>,
    ) -> UnifierResult<Unified2<'m>> {
        match scope_source {
            Some(scope_source) => self.unify_scoping2(scope_source, u_node),
            None => self.unify_u_node2(u_node, None),
        }
    }

    fn unify_scoping2(
        &mut self,
        scope_source: &TypedHirNode<'m>,
        u_node: UNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        let debug_index = "n/a";
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;

        match kind {
            NodeKind::VariableRef(var) => {
                debug!("unify_scoping({debug_index:?}, VariableRef)");
                let unified = self.unify_u_node2(u_node, None)?;
                Ok(Unified2 {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: meta.ty,
                    }),
                    node: unified.node,
                })
            }
            NodeKind::Unit => {
                debug!("unify_scoping({debug_index:?}, Unit)");
                self.unify_u_node2(u_node, None)
            }
            NodeKind::Int(_int) => panic!("Int in scoping"),
            NodeKind::Call(proc, args) => todo!(),
            NodeKind::Map(arg) => todo!(),
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, child_scopes) => {
                debug!("unify_scoping({debug_index:?}, Struct({}))", binder.0);
                // self.unify_struct_scoping2(set, *binder, scope_source, child_scopes, meta)
                todo!()
            }
            NodeKind::Prop(optional, struct_var, id, variants) => {
                todo!()
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

    fn unify_u_node2(
        &mut self,
        u_node: UNode<'m>,
        next_scope_source: Option<&TypedHirNode<'m>>,
    ) -> UnifierResult<Unified2<'m>> {
        let meta = u_node.meta;
        match u_node.kind {
            UNodeKind::Stolen => panic!("Stolen: Should not happen"),
            UNodeKind::Leaf(LeafUNodeKind::VariableRef(var)) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::VariableRef(var),
                    meta,
                },
            }),
            UNodeKind::Leaf(LeafUNodeKind::Int(int)) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta,
                },
            }),
            UNodeKind::Leaf(LeafUNodeKind::Unit) => Ok(Unified2 {
                binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta,
                },
            }),
            UNodeKind::Block(kind, body) => todo!(),
            UNodeKind::Expr(kind, params) => {
                let mut hir_params = vec![];
                for u_param in params {
                    hir_params.push(self.unify2(u_param, next_scope_source)?.node);
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
            UNodeKind::Attr(kind, attr) => todo!(),
            UNodeKind::SubScope(subscope_idx, _) => todo!(),
        }
    }

    fn unify_sub_scoping(
        &mut self,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        todo!()
    }
}

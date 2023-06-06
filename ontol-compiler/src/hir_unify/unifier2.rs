use ontol_hir::kind::{Attribute, Dimension, NodeKind, Optional, PatternBinding, PropVariant};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use tracing::debug;

use crate::{
    hir_unify::u_node::UNodeKind,
    typed_hir::{Meta, TypedBinder, TypedHir, TypedHirNode},
    types::TypeRef,
    SourceSpan,
};

use super::{
    u_node::{AttrUNodeKind, BlockUNodeKind, ExprUNodeKind, LeafUNodeKind, UBlockBody, UNode},
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

#[derive(Clone, Copy)]
pub enum Scoping<'s, 'm> {
    Complete,
    Binder(TypedBinder<'m>),
    InvertCall(BuiltinProc, &'s [TypedHirNode<'m>], Meta<'m>),
    Block(TypedBinder<'m>, &'s [TypedHirNode<'m>]),
    Prop(
        Optional,
        ontol_hir::Variable,
        PropertyId,
        &'s [PropVariant<'m, TypedHir>],
        Meta<'m>,
    ),
}

pub struct UnifyPatternBinding2<'m> {
    pub binding: PatternBinding,
    pub target_nodes: Option<Nodes<'m>>,
}

pub struct LetAttr2<'m> {
    pub rel_binding: PatternBinding,
    pub val_binding: PatternBinding,
    pub target_nodes: Nodes<'m>,
    pub ty: TypeRef<'m>,
}

#[derive(Default)]
pub struct Nodes<'m>(pub Vec<TypedHirNode<'m>>);

impl<'m> Nodes<'m> {
    pub fn type_iter<'a>(&'a self) -> impl Iterator<Item = TypeRef<'m>> + 'a {
        self.0.iter().map(|node| node.meta.ty)
    }
}

impl<'s, 'm> Unifier<'s, 'm> {
    pub fn unify2(
        &mut self,
        u_node: UNode<'m>,
        scope_source: ScopeSource<'s, 'm>,
    ) -> UnifierResult<Unified2<'m>> {
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
            (UNodeKind::Block(kind, mut u_block_body), ScopeSource::Block(scope_binder, scope)) => {
                let mut body = self.unify_u_block_subscopes(&mut u_block_body, scope)?;
                body.extend(self.unify_u_block_nodes(&mut u_block_body, ScopeSource::Absent)?);

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
            (UNodeKind::Attr(kind, attr), scope_source) => {
                let rel = self.unify_u_node2(*attr.rel, scope_source)?;
                let val = self.unify_u_node2(*attr.val, scope_source)?;

                match kind {
                    AttrUNodeKind::PropVariant(optional, struct_var, property_id) => Ok(Unified2 {
                        binder: None,
                        node: TypedHirNode {
                            kind: NodeKind::Prop(
                                optional,
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
            (UNodeKind::SubScope(subscope_idx, _), _) => todo!(),
        }
    }

    fn unify_scoping2(
        &mut self,
        scoping: Scoping<'s, 'm>,
        u_node: UNode<'m>,
    ) -> UnifierResult<Unified2<'m>> {
        match scoping {
            Scoping::Complete => self.unify_u_node2(u_node, ScopeSource::Absent),
            Scoping::Binder(typed_binder) => {
                let unified = self.unify_u_node2(u_node, ScopeSource::Absent)?;
                Ok(Unified2 {
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

    fn unify_u_block_subscopes(
        &mut self,
        body: &mut UBlockBody<'m>,
        scope_slice: &'s [TypedHirNode<'m>],
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let unit_type = self.unit_type();
        let mut output = vec![];

        for (subscope_idx, mut sub_body) in std::mem::take(&mut body.sub_scoping) {
            let subscope = &scope_slice[subscope_idx];
            match self.classify_scoping(subscope)? {
                Scoping::Block(_, sub_scope_slice) => {
                    // BUG: probably not correct?
                    output.extend(self.unify_u_block_subscopes(&mut sub_body, sub_scope_slice)?)
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
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut output = vec![];

        for u_node in std::mem::take(&mut body.nodes) {
            output.push(self.unify2(u_node, scope_source)?.node);
        }

        Ok(output)
    }

    fn classify_scoping(
        &mut self,
        scope_source: &'s TypedHirNode<'m>,
    ) -> UnifierResult<Scoping<'s, 'm>> {
        let debug_index = "n/a";
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;

        match kind {
            NodeKind::VariableRef(var) => {
                debug!("unify_scoping({debug_index:?}, VariableRef)");
                Ok(Scoping::Binder(TypedBinder {
                    variable: *var,
                    ty: meta.ty,
                }))
            }
            NodeKind::Unit => {
                debug!("unify_scoping({debug_index:?}, Unit)");
                Ok(Scoping::Complete)
            }
            NodeKind::Int(_int) => panic!("Int in scoping"),
            NodeKind::Call(proc, args) => Ok(Scoping::InvertCall(*proc, args, meta)),
            NodeKind::Map(arg) => todo!(),
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, child_scopes) => {
                debug!("unify_scoping({debug_index:?}, Struct({}))", binder.0);
                Ok(Scoping::Block(
                    TypedBinder {
                        variable: binder.0,
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

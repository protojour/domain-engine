use ontol_hir::{kind::NodeKind, Binder};
use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    typed_hir::{Meta, TypedBinder, TypedHirKind, TypedHirNode},
    SourceSpan,
};

use super::{
    u_node::{BlockUNodeKind, UBlockBody, UNode, UNodeKind},
    unifier::{InvertedCall, Unifier, UnifierResult},
    unifier2::{ScopeSource, UnifiedNode},
};

impl<'s, 'm> Unifier<'s, 'm> {
    pub(super) fn unify_call_scope(
        &mut self,
        scope_call: (BuiltinProc, &'s [TypedHirNode<'m>]),
        scope_meta: Meta<'m>,
        u_node: UNode<'m>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let call_sub_scoping = match u_node {
            node @ UNode {
                kind: UNodeKind::SubScope(..),
                ..
            } => CallSubScoping::Node(node),
            UNode {
                kind: UNodeKind::Block(_, body),
                ..
            } => CallSubScoping::Block(body),
            u_node => {
                return self.unify_u_node2(u_node, ScopeSource::Absent);
            }
        };

        let binder_var = self.alloc_var();
        let inner_expr = TypedHirNode {
            kind: TypedHirKind::VariableRef(binder_var),
            meta: scope_meta,
        };
        let inverted_call =
            self.invert_call(scope_call, scope_meta, call_sub_scoping, inner_expr)?;
        let return_ty = self.last_type(inverted_call.body.iter());

        Ok(UnifiedNode {
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

    fn invert_call(
        &mut self,
        (scope_proc, scope_args): (BuiltinProc, &'s [TypedHirNode<'m>]),
        scope_meta: Meta<'m>,
        call_sub_scoping: CallSubScoping<'m>,
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

        let (subscope_idx, next_sub_scoping) = call_sub_scoping.sub_scoping();

        for (index, arg) in scope_args.iter().enumerate() {
            if index != subscope_idx {
                inverted_args.push(arg.clone());
            }
        }

        let pivot_arg = &scope_args[subscope_idx];

        match &pivot_arg.kind {
            NodeKind::VariableRef(var) => {
                inverted_args.insert(subscope_idx, inner_expr);
                let u_node = match next_sub_scoping {
                    CallSubScoping::Node(u_node) => u_node,
                    CallSubScoping::Block(block) => UNode {
                        kind: UNodeKind::Block(BlockUNodeKind::Raw, block),
                        free_variables: Default::default(),
                        meta: Meta {
                            ty: self.unit_type(),
                            span: SourceSpan::none(),
                        },
                    },
                };
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
                self.invert_call(
                    (*next_scope_proc, next_scope_args),
                    pivot_arg.meta,
                    next_sub_scoping,
                    next_inner_expr,
                )
            }
            _ => unimplemented!("Unhandled pivot_arg"),
        }
    }
}

enum CallSubScoping<'m> {
    Node(UNode<'m>),
    Block(UBlockBody<'m>),
}

impl<'m> CallSubScoping<'m> {
    fn sub_scoping(self) -> (usize, Self) {
        match self {
            Self::Node(
                UNode {
                    kind: UNodeKind::SubScope(subscope_idx, sub_node),
                    ..
                },
                ..,
            ) => (subscope_idx, Self::Node(*sub_node)),
            Self::Block(mut block) => {
                let (idx, sub_block) = block.sub_scoping.pop_first().unwrap();
                (idx, Self::Block(sub_block))
            }
            _ => panic!(),
        }
    }
}

use ontol_hir::{kind::NodeKind, Binder};
use ontol_runtime::vm::proc::BuiltinProc;

use crate::typed_hir::{Meta, TypedBinder, TypedHirKind, TypedHirNode};

use super::{
    u_node::{UNode, UNodeKind},
    unifier::{InvertedCall, Unifier, UnifierResult},
    unifier2::{ScopeSource, Unified2},
};

impl<'s, 'm> Unifier<'s, 'm> {
    pub(super) fn unify_call_scope(
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
}

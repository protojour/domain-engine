use std::marker::PhantomData;

use ontol_hir::kind::{Attribute, Dimension, NodeKind};

use super::expr;
use crate::{hir_unify::VarSet, typed_hir::TypedHirNode};

#[derive(Default)]
pub struct ExprBuilder<'m> {
    in_scope: VarSet,
    phantom: PhantomData<&'m ()>,
}

impl<'m> ExprBuilder<'m> {
    pub fn hir_to_expr(&mut self, node: &TypedHirNode<'m>) -> expr::Expr<'m> {
        match &node.kind {
            NodeKind::Var(var) => {
                let mut free_vars = VarSet::default();
                free_vars.0.insert(var.0 as usize);

                expr::Expr {
                    kind: expr::Kind::Var(*var),
                    meta: node.meta,
                    free_vars,
                }
            }
            NodeKind::Unit => expr::Expr {
                kind: expr::Kind::Unit,
                meta: node.meta,
                free_vars: Default::default(),
            },
            NodeKind::Int(int) => expr::Expr {
                kind: expr::Kind::Int(*int),
                meta: node.meta,
                free_vars: Default::default(),
            },
            NodeKind::Let(..) => panic!(),
            NodeKind::Call(proc, params) => {
                let params: Vec<_> = params.iter().map(|param| self.hir_to_expr(param)).collect();
                let mut free_vars = VarSet::default();
                for param in &params {
                    free_vars.0.union_with(&param.free_vars.0);
                }

                expr::Expr {
                    kind: expr::Kind::Call(expr::Call(*proc, params)),
                    meta: node.meta,
                    free_vars,
                }
            }
            NodeKind::Map(arg) => {
                let arg = self.hir_to_expr(arg);
                expr::Expr {
                    free_vars: arg.free_vars.clone(),
                    kind: expr::Kind::Map(Box::new(arg)),
                    meta: node.meta,
                }
            }
            NodeKind::Seq(label, attr) => {
                todo!("seq expr")
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(*binder, |zelf| {
                let props: Vec<_> = nodes
                    .iter()
                    .flat_map(|node| zelf.node_to_props(node))
                    .collect();
                let mut free_vars = VarSet::default();
                for prop in &props {
                    free_vars.0.union_with(&prop.free_vars.0);
                }

                expr::Expr {
                    kind: expr::Kind::Struct(expr::Struct(*binder, props)),
                    meta: node.meta,
                    free_vars,
                }
            }),
            NodeKind::Prop(optional, struct_var, id, variants) => panic!("standalone prop"),
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                todo!()
            }
            NodeKind::Iter(..) => {
                todo!()
            }
            NodeKind::Push(..) => {
                todo!()
            }
        }
    }

    fn node_to_props(&mut self, node: &TypedHirNode<'m>) -> Vec<expr::Prop<'m>> {
        match &node.kind {
            NodeKind::Prop(optional, struct_var, prop_id, variants) => variants
                .iter()
                .map(|variant| match variant.dimension {
                    Dimension::Singular => {
                        let mut union = UnionBuilder::default();
                        let rel = union.plus(self.hir_to_expr(&variant.attr.rel));
                        let val = union.plus(self.hir_to_expr(&variant.attr.val));

                        expr::Prop {
                            optional: *optional,
                            prop_id: *prop_id,
                            free_vars: union.vars,
                            seq: false,
                            struct_var: *struct_var,
                            attr: Attribute { rel, val },
                        }
                    }
                    Dimension::Seq(label) => {
                        let rel = self.hir_to_expr(&variant.attr.rel);
                        let val = self.hir_to_expr(&variant.attr.val);
                        let mut free_vars = VarSet::default();
                        free_vars.0.insert(label.0 as usize);

                        expr::Prop {
                            optional: *optional,
                            prop_id: *prop_id,
                            free_vars,
                            seq: true,
                            struct_var: *struct_var,
                            attr: Attribute { rel, val },
                        }
                    }
                })
                .collect(),
            _ => panic!("Expected property"),
        }
    }

    // do we even need this, expression are so simple
    fn enter_binder<T>(
        &mut self,
        binder: ontol_hir::Binder,
        func: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.in_scope.0.insert(binder.0 .0 as usize) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.0.remove(binder.0 .0 as usize);
        value
    }
}

#[derive(Default)]
struct UnionBuilder {
    vars: VarSet,
}

impl UnionBuilder {
    fn plus<'m>(&mut self, expr: expr::Expr<'m>) -> expr::Expr<'m> {
        self.vars.0.union_with(&expr.free_vars.0);
        expr
    }
}

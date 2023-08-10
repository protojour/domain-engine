use std::marker::PhantomData;

use super::expr;
use crate::{
    hir_unify::VarSet,
    typed_hir::{TypedBinder, TypedHirNode},
};

pub struct ExprBuilder<'m> {
    in_scope: VarSet,
    phantom: PhantomData<&'m ()>,
    var_allocator: ontol_hir::VarAllocator,
}

impl<'m> ExprBuilder<'m> {
    pub fn new(var_allocator: ontol_hir::VarAllocator) -> Self {
        Self {
            in_scope: Default::default(),
            phantom: PhantomData,
            var_allocator,
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn hir_to_expr(&mut self, TypedHirNode(kind, meta): &TypedHirNode<'m>) -> expr::Expr<'m> {
        match kind {
            ontol_hir::Kind::Var(var) => {
                let mut free_vars = VarSet::default();
                free_vars.insert(*var);

                expr::Expr(
                    expr::Kind::Var(*var),
                    expr::Meta {
                        hir_meta: *meta,
                        free_vars,
                    },
                )
            }
            ontol_hir::Kind::Unit => expr::Expr(
                expr::Kind::Unit,
                expr::Meta {
                    hir_meta: *meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::I64(int) => expr::Expr(
                expr::Kind::I64(*int),
                expr::Meta {
                    hir_meta: *meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::F64(float) => expr::Expr(
                expr::Kind::F64(*float),
                expr::Meta {
                    hir_meta: *meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::String(string) => expr::Expr(
                expr::Kind::String(string.clone()),
                expr::Meta {
                    hir_meta: *meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::Let(..) => panic!(),
            ontol_hir::Kind::Call(proc, params) => {
                let params: Vec<_> = params.iter().map(|param| self.hir_to_expr(param)).collect();
                let mut free_vars = VarSet::default();
                for param in &params {
                    free_vars.union_with(&param.1.free_vars);
                }

                expr::Expr(
                    expr::Kind::Call(expr::Call(*proc, params)),
                    expr::Meta {
                        hir_meta: *meta,
                        free_vars,
                    },
                )
            }
            ontol_hir::Kind::Map(arg) => {
                let arg = self.hir_to_expr(arg);
                let expr_meta = expr::Meta {
                    free_vars: arg.1.free_vars.clone(),
                    hir_meta: *meta,
                };
                expr::Expr(expr::Kind::Map(Box::new(arg)), expr_meta)
            }
            ontol_hir::Kind::Seq(typed_label, attr) => {
                let rel = self.hir_to_expr(&attr.rel);
                let val = self.hir_to_expr(&attr.val);

                let mut free_vars = VarSet::default();
                free_vars.union_with(&rel.1.free_vars);
                free_vars.union_with(&val.1.free_vars);
                free_vars.insert(typed_label.label.into());

                expr::Expr(
                    expr::Kind::Seq(
                        typed_label.label,
                        Box::new(ontol_hir::Attribute { rel, val }),
                    ),
                    expr::Meta {
                        free_vars,
                        hir_meta: *meta,
                    },
                )
            }
            ontol_hir::Kind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                let props: Vec<_> = nodes
                    .iter()
                    .flat_map(|node| zelf.node_to_props(node))
                    .collect();
                let mut free_vars = VarSet::default();
                for prop in &props {
                    free_vars.union_with(&prop.free_vars);
                }

                expr::Expr(
                    expr::Kind::Struct(expr::Struct(*binder, props)),
                    expr::Meta {
                        hir_meta: *meta,
                        free_vars,
                    },
                )
            }),
            ontol_hir::Kind::Prop(..) => panic!("standalone prop"),
            ontol_hir::Kind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            ontol_hir::Kind::Gen(..) => {
                todo!()
            }
            ontol_hir::Kind::Iter(..) => {
                todo!()
            }
            ontol_hir::Kind::Push(..) => {
                todo!()
            }
        }
    }

    fn node_to_props(&mut self, TypedHirNode(kind, _): &TypedHirNode<'m>) -> Vec<expr::Prop<'m>> {
        match kind {
            ontol_hir::Kind::Prop(optional, struct_var, prop_id, variants) => variants
                .iter()
                .map(|variant| match variant.dimension {
                    ontol_hir::AttrDimension::Singular => {
                        let mut union = UnionBuilder::default();
                        let rel = union.plus(self.hir_to_expr(&variant.attr.rel));
                        let val = union.plus(self.hir_to_expr(&variant.attr.val));

                        expr::Prop {
                            optional: *optional,
                            prop_id: *prop_id,
                            free_vars: union.vars,
                            seq: None,
                            struct_var: *struct_var,
                            attr: ontol_hir::Attribute { rel, val },
                        }
                    }
                    ontol_hir::AttrDimension::Seq(typed_label, _has_default) => {
                        let rel = self.hir_to_expr(&variant.attr.rel);
                        let val = self.hir_to_expr(&variant.attr.val);
                        let mut free_vars = VarSet::default();
                        free_vars.insert(typed_label.label.into());

                        expr::Prop {
                            optional: *optional,
                            prop_id: *prop_id,
                            free_vars,
                            seq: Some(typed_label.label),
                            struct_var: *struct_var,
                            attr: ontol_hir::Attribute { rel, val },
                        }
                    }
                })
                .collect(),
            _ => panic!("Expected property"),
        }
    }

    // do we even need this, expression are so simple
    fn enter_binder<T>(&mut self, binder: &TypedBinder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.var) {
            panic!(
                "Malformed HIR: {:?} variable was already in scope",
                binder.var
            );
        }
        let value = func(self);
        self.in_scope.remove(binder.var);
        value
    }
}

#[derive(Default)]
struct UnionBuilder {
    vars: VarSet,
}

impl UnionBuilder {
    fn plus<'m>(&mut self, expr: expr::Expr<'m>) -> expr::Expr<'m> {
        self.vars.union_with(&expr.1.free_vars);
        expr
    }
}

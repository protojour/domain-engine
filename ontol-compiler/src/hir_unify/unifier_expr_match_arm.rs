use ontol_hir::kind::{MatchArm, NodeKind, PatternBinding, PropPattern};

use crate::{
    typed_hir::{Meta, TypedHirNode},
    SourceSpan,
};

use super::{
    expr,
    hierarchy::SubScoped,
    scope,
    unifier::{UnifiedNode, Unifier},
    UnifierResult, VarSet,
};

impl<'a, 'm> Unifier<'a, 'm> {
    pub(super) fn unify_expr_match_arm(
        &mut self,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let (match_arm, ty) = match scope_prop.kind {
            scope::PropKind::Attr(rel_binding, val_binding) => {
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let joined_scope = self.join_attr_scope(rel_binding, val_binding);
                let nodes = self.wrap_sub_scoped_expressions_in_scope(joined_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                (
                    MatchArm {
                        pattern: PropPattern::Attr(hir_rel_binding, hir_val_binding),
                        nodes,
                    },
                    ty,
                )
            }
            scope::PropKind::Seq(label, rel_binding, val_binding) => {
                let gen_scope = scope::Scope(
                    scope::Kind::Gen(scope::Gen {
                        input_seq: ontol_hir::Var(label.0),
                        output_seq: self.alloc_var(),
                        bindings: Box::new((rel_binding, val_binding)),
                    }),
                    scope::Meta {
                        hir_meta: self.unit_meta(),
                        vars: VarSet::default(),
                    },
                );
                let nodes = self.wrap_sub_scoped_expressions_in_scope(gen_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                (
                    MatchArm {
                        pattern: PropPattern::Seq(PatternBinding::Binder(ontol_hir::Var(label.0))),
                        nodes,
                    },
                    ty,
                )
            }
        };

        let mut match_arms = vec![match_arm];

        if scope_prop.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(UnifiedNode {
            typed_binder: None,
            node: TypedHirNode {
                kind: NodeKind::MatchProp(scope_prop.struct_var, scope_prop.prop_id, match_arms),
                meta: Meta {
                    ty,
                    span: SourceSpan::none(),
                },
            },
        })
    }

    fn unify_sub_scoped_expressions(
        &mut self,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

        for expr in sub_scoped.expressions {
            nodes.push(self.unify(inner_scope.clone(), expr)?.node);
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_scopes {
            nodes.push(self.unify_expr_match_arm(sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn wrap_sub_scoped_expressions_in_scope(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope_kind {
            scope_kind @ (scope::Kind::Const | scope::Kind::Var(_)) => {
                self.unify_sub_scoped_expressions(scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
            scope::Kind::Let(let_scope) => {
                let const_scope = self.const_scope();
                let block = self.unify_sub_scoped_expressions(const_scope, sub_scoped)?;
                let ty = block
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                let node = TypedHirNode {
                    kind: NodeKind::Let(let_scope.inner_binder, Box::new(let_scope.def), block),
                    meta: Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(vec![node])
            }
            scope::Kind::Struct(_struct_scope) => {
                todo!()
            }
            scope::Kind::Gen(gen) => self.unify_sub_scoped_expressions(
                scope::Scope(scope::Kind::Gen(gen), scope_meta),
                sub_scoped,
            ),
        }
    }
}

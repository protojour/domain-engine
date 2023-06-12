use ontol_hir::kind::{
    Attribute, Dimension, MatchArm, NodeKind, Optional, PatternBinding, PropPattern, PropVariant,
};

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

/// Unifier trait for types which can produce properties.
/// This is a trait because it has to work for both expr::Expr and expr::Prop.
pub(super) trait UnifyProps<'m>: Sized {
    // Required method
    fn unify_sub_scoped<'a>(
        unifier: &mut Unifier<'a, 'm>,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>>;

    // Required method
    fn unify_with_struct<'a>(
        unifier: &mut Unifier<'a, 'm>,
        struct_scope: (scope::Struct<'m>, scope::Meta<'m>),
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>>;

    // Common logic
    fn unify_match_arm<'a>(
        unifier: &mut Unifier<'a, 'm>,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let (match_arm, ty) = match scope_prop.kind {
            scope::PropKind::Attr(rel_binding, val_binding) => {
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let joined_scope = unifier.join_attr_scope(rel_binding, val_binding);
                let nodes = Self::wrap_sub_scoped_in_scope(unifier, joined_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

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
                        output_seq: unifier.alloc_var(),
                        bindings: Box::new((rel_binding, val_binding)),
                    }),
                    scope::Meta {
                        hir_meta: unifier.unit_meta(),
                        vars: VarSet::default(),
                    },
                );
                let nodes = Self::wrap_sub_scoped_in_scope(unifier, gen_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

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

    // Common logic
    fn wrap_sub_scoped_in_scope<'a>(
        unifier: &mut Unifier<'a, 'm>,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope_kind {
            scope::Kind::Const | scope::Kind::Var(_) => {
                Self::unify_sub_scoped(unifier, scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
            scope::Kind::Let(let_scope) => {
                let const_scope = unifier.const_scope();
                let block = Self::unify_sub_scoped(unifier, const_scope, sub_scoped)?;
                let ty = block
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

                let node = TypedHirNode {
                    kind: NodeKind::Let(let_scope.inner_binder, Box::new(let_scope.def), block),
                    meta: Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(vec![node])
            }
            scope::Kind::Struct(struct_scope) => {
                Self::unify_with_struct(unifier, (struct_scope, scope_meta), sub_scoped)
            }
            scope_kind @ scope::Kind::Gen(_) => {
                Self::unify_sub_scoped(unifier, scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
        }
    }
}

/// Unification for expr::Prop
impl<'m> UnifyProps<'m> for expr::Prop<'m> {
    fn unify_sub_scoped<'a>(
        unifier: &mut Unifier<'a, 'm>,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());
        for prop in sub_scoped.expressions {
            let unit_meta = unifier.unit_meta();
            nodes.push(
                unifier
                    .unify(
                        inner_scope.clone(),
                        expr::Expr(
                            expr::Kind::Prop(Box::new(prop)),
                            expr::Meta {
                                hir_meta: unit_meta,
                                free_vars: VarSet::default(),
                            },
                        ),
                    )?
                    .node,
            );
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_scopes {
            nodes.push(Self::unify_match_arm(unifier, sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn unify_with_struct<'a>(
        unifier: &mut Unifier<'a, 'm>,
        (struct_scope, scope_meta): (scope::Struct<'m>, scope::Meta<'m>),
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let scope = scope::Scope(scope::Kind::Struct(struct_scope), scope_meta);
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

        for prop in sub_scoped.expressions {
            // FIXME: avoid clone by taking scope reference?
            let rel = unifier.unify(scope.clone(), prop.attr.rel)?;
            let val = unifier.unify(scope.clone(), prop.attr.val)?;

            nodes.push(TypedHirNode {
                kind: NodeKind::Prop(
                    Optional(false),
                    prop.struct_var,
                    prop.prop_id,
                    vec![PropVariant {
                        dimension: Dimension::Singular,
                        attr: Attribute {
                            rel: Box::new(rel.node),
                            val: Box::new(val.node),
                        },
                    }],
                ),
                meta: unifier.unit_meta(),
            });
        }

        // TODO: sub_scoped
        for (_sub_prop_scope, _sub_scoped_prop) in sub_scoped.sub_scopes {
            todo!()
        }

        Ok(nodes)
    }
}

/// Unification for expr::Expr
impl<'m> UnifyProps<'m> for expr::Expr<'m> {
    fn unify_sub_scoped<'a>(
        unifier: &mut Unifier<'a, 'm>,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

        for expr in sub_scoped.expressions {
            nodes.push(unifier.unify(inner_scope.clone(), expr)?.node);
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_scopes {
            nodes.push(Self::unify_match_arm(unifier, sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn unify_with_struct<'a>(
        _unifier: &mut Unifier<'a, 'm>,
        _: (scope::Struct<'m>, scope::Meta<'m>),
        _sub_scoped: SubScoped<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        todo!()
    }
}

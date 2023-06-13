use crate::{
    typed_hir::{Meta, TypedHirNode},
    SourceSpan,
};

use super::{
    dep_tree::SubTree,
    expr,
    regroup_match_prop::regroup_match_prop,
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
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>>;

    // Required method
    fn unify_with_prop_set<'a>(
        unifier: &mut Unifier<'a, 'm>,
        prop_set_scope: (scope::PropSet<'m>, scope::Meta<'m>),
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>>;

    // Common logic
    fn unify_match_arm<'a>(
        unifier: &mut Unifier<'a, 'm>,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let (match_arm, ty) = match scope_prop.kind {
            scope::PropKind::Attr(rel_binding, val_binding) => {
                let hir_rel_binding = rel_binding.hir_binding();
                let hir_val_binding = val_binding.hir_binding();

                let joined_scope = unifier.join_attr_scope(rel_binding, val_binding);
                let nodes = Self::wrap_sub_scoped_in_scope(unifier, joined_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(TypedHirNode::ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

                (
                    ontol_hir::MatchArm {
                        pattern: ontol_hir::PropPattern::Attr(hir_rel_binding, hir_val_binding),
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
                        dependencies: VarSet::default(),
                    },
                );
                let nodes = Self::wrap_sub_scoped_in_scope(unifier, gen_scope, sub_scoped)?;
                let ty = nodes
                    .iter()
                    .map(TypedHirNode::ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

                (
                    ontol_hir::MatchArm {
                        pattern: ontol_hir::PropPattern::Seq(ontol_hir::Binding::Binder(
                            ontol_hir::Var(label.0),
                        )),
                        nodes,
                    },
                    ty,
                )
            }
        };

        let mut match_arms = vec![match_arm];

        if scope_prop.optional.0 {
            match_arms.push(ontol_hir::MatchArm {
                pattern: ontol_hir::PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(UnifiedNode {
            typed_binder: None,
            node: TypedHirNode(
                ontol_hir::Kind::MatchProp(scope_prop.struct_var, scope_prop.prop_id, match_arms),
                Meta {
                    ty,
                    span: SourceSpan::none(),
                },
            ),
        })
    }

    // Common logic
    fn wrap_sub_scoped_in_scope<'a>(
        unifier: &mut Unifier<'a, 'm>,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
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
                    .map(TypedHirNode::ty)
                    .last()
                    .unwrap_or_else(|| unifier.unit_type());

                let node = TypedHirNode(
                    ontol_hir::Kind::Let(let_scope.inner_binder, Box::new(let_scope.def), block),
                    Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                );

                Ok(vec![node])
            }
            scope::Kind::PropSet(prop_set) => {
                Self::unify_with_prop_set(unifier, (prop_set, scope_meta), sub_scoped)
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
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_trees.len());
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

        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_trees {
            nodes.push(Self::unify_match_arm(unifier, sub_prop_scope, sub_scoped)?.node);
        }

        Ok(regroup_match_prop(nodes))
    }

    fn unify_with_prop_set<'a>(
        unifier: &mut Unifier<'a, 'm>,
        (prop_set, scope_meta): (scope::PropSet<'m>, scope::Meta<'m>),
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let scope = scope::Scope(scope::Kind::PropSet(prop_set), scope_meta);
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_trees.len());

        for prop in sub_scoped.expressions {
            // FIXME: avoid clone by taking scope reference?
            let rel = unifier.unify(scope.clone(), prop.attr.rel)?;
            let val = unifier.unify(scope.clone(), prop.attr.val)?;

            nodes.push(TypedHirNode(
                ontol_hir::Kind::Prop(
                    ontol_hir::Optional(false),
                    prop.struct_var,
                    prop.prop_id,
                    vec![ontol_hir::PropVariant {
                        dimension: ontol_hir::Dimension::Singular,
                        attr: ontol_hir::Attribute {
                            rel: Box::new(rel.node),
                            val: Box::new(val.node),
                        },
                    }],
                ),
                unifier.unit_meta(),
            ));
        }

        // TODO: sub_scoped
        for (_sub_prop_scope, _sub_scoped_prop) in sub_scoped.sub_trees {
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
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_trees.len());

        for expr in sub_scoped.expressions {
            nodes.push(unifier.unify(inner_scope.clone(), expr)?.node);
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_trees {
            nodes.push(Self::unify_match_arm(unifier, sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn unify_with_prop_set<'a>(
        _unifier: &mut Unifier<'a, 'm>,
        _: (scope::PropSet<'m>, scope::Meta<'m>),
        _sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        todo!()
    }
}

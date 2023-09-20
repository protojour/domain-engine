use crate::{
    typed_hir::{IntoTypedHirValue, Meta, UNIT_META},
    NO_SPAN,
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
    ) -> UnifierResult<ontol_hir::Nodes>;

    // Required method
    fn unify_with_prop_set<'a>(
        unifier: &mut Unifier<'a, 'm>,
        prop_set_scope: (scope::PropSet<'m>, scope::Meta<'m>),
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<ontol_hir::Nodes>;

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
                    .map(|node| unifier.hir_arena[*node].ty())
                    .last()
                    .unwrap_or_else(|| unifier.types.unit_type());

                (
                    (
                        ontol_hir::PropPattern::Attr(hir_rel_binding, hir_val_binding),
                        nodes,
                    ),
                    ty,
                )
            }
            scope::PropKind::Seq(typed_label, has_default, rel_binding, val_binding) => {
                let gen_scope = scope::Scope(
                    scope::Kind::Gen(scope::Gen {
                        input_seq: ontol_hir::Var(typed_label.value().0),
                        output_seq: unifier.var_allocator.alloc(),
                        bindings: Box::new((rel_binding, val_binding)),
                    }),
                    scope::Meta {
                        hir_meta: unifier.unit_meta(),
                        vars: VarSet::default(),
                        dependencies: VarSet::default(),
                    },
                );
                let nodes = Self::wrap_sub_scoped_in_scope(unifier, gen_scope, sub_scoped)?;
                let arm_ty = nodes
                    .iter()
                    .map(|node| unifier.hir_arena[*node].ty())
                    .last()
                    .unwrap_or_else(|| unifier.types.unit_type());

                (
                    (
                        ontol_hir::PropPattern::Seq(
                            ontol_hir::Binding::Binder(
                                ontol_hir::Binder {
                                    var: ontol_hir::Var(typed_label.value().0),
                                }
                                .with_meta(Meta {
                                    ty: typed_label.ty(),
                                    span: NO_SPAN,
                                }),
                            ),
                            has_default,
                        ),
                        nodes,
                    ),
                    arm_ty,
                )
            }
        };

        let mut match_arms = vec![match_arm];

        if scope_prop.optional.0 {
            match_arms.push((ontol_hir::PropPattern::Absent, ontol_hir::Nodes::default()));
        }

        Ok(UnifiedNode {
            typed_binder: None,
            node: unifier.mk_node(
                ontol_hir::Kind::MatchProp(
                    scope_prop.struct_var,
                    scope_prop.prop_id,
                    match_arms.into(),
                ),
                Meta { ty, span: NO_SPAN },
            ),
        })
    }

    // Common logic
    fn wrap_sub_scoped_in_scope<'a>(
        unifier: &mut Unifier<'a, 'm>,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<ontol_hir::Nodes> {
        match scope_kind {
            scope::Kind::Const | scope::Kind::Var(_) | scope::Kind::Regex(..) => {
                Self::unify_sub_scoped(unifier, scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
            scope::Kind::Let(let_scope) => {
                let const_scope = unifier.const_scope();
                let block = Self::unify_sub_scoped(unifier, const_scope, sub_scoped)?;
                let ty = block
                    .iter()
                    .map(|node| unifier.hir_arena[*node].ty())
                    .last()
                    .unwrap_or_else(|| unifier.types.unit_type());

                todo!("Import let def to arena");
                let node = unifier.mk_node(
                    ontol_hir::Kind::Let(let_scope.inner_binder, let_scope.def.node(), block),
                    Meta { ty, span: NO_SPAN },
                );

                Ok([node].into_iter().collect())
            }
            scope::Kind::PropSet(prop_set) => {
                Self::unify_with_prop_set(unifier, (prop_set, scope_meta), sub_scoped)
            }
            scope_kind @ scope::Kind::Gen(_) => {
                Self::unify_sub_scoped(unifier, scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
            _scope_kind @ scope::Kind::Escape(_) => {
                todo!("Escape")
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
    ) -> UnifierResult<ontol_hir::Nodes> {
        let mut nodes = ontol_hir::Nodes::default();
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

        Ok(regroup_match_prop(nodes, &mut unifier.hir_arena))
    }

    fn unify_with_prop_set<'a>(
        unifier: &mut Unifier<'a, 'm>,
        (prop_set, scope_meta): (scope::PropSet<'m>, scope::Meta<'m>),
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let scope = scope::Scope(scope::Kind::PropSet(prop_set), scope_meta);
        let mut nodes = ontol_hir::Nodes::default();

        for prop in sub_scoped.expressions {
            let variant = match prop.variant {
                expr::PropVariant::Singleton(attr) => {
                    // FIXME: avoid clone by taking scope reference?
                    let rel = unifier.unify(scope.clone(), attr.rel)?;
                    let val = unifier.unify(scope.clone(), attr.val)?;

                    ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                        rel: rel.node,
                        val: val.node,
                    })
                }
                expr::PropVariant::Seq { .. } => todo!(),
            };

            nodes.push(unifier.mk_node(
                ontol_hir::Kind::Prop(
                    ontol_hir::Optional(false),
                    prop.struct_var,
                    prop.prop_id,
                    [variant].into(),
                ),
                UNIT_META,
            ));
        }

        for (sub_prop_scope, sub_scoped_prop) in sub_scoped.sub_trees {
            nodes.push(Self::unify_match_arm(unifier, sub_prop_scope, sub_scoped_prop)?.node);
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
    ) -> UnifierResult<ontol_hir::Nodes> {
        let mut nodes = ontol_hir::Nodes::default();

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
        sub_scoped: SubTree<Self, scope::Prop<'m>>,
    ) -> UnifierResult<ontol_hir::Nodes> {
        todo!("sub_scoped: {sub_scoped:?}");
    }
}

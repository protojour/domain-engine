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

impl<'a, 'm> Unifier<'a, 'm> {
    pub(super) fn unify_prop_match_arm(
        &mut self,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let (match_arm, ty) = match scope_prop.kind {
            scope::PropKind::Attr(rel_binding, val_binding) => {
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let joined_scope = self.join_attr_scope(rel_binding, val_binding);
                let nodes = self.wrap_sub_scoped_props_in_scope(joined_scope, sub_scoped)?;
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
                        vars: super::VarSet::default(),
                    },
                );
                let nodes = self.wrap_sub_scoped_props_in_scope(gen_scope, sub_scoped)?;
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

    pub(super) fn unify_sub_scoped_props(
        &mut self,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());
        for prop in sub_scoped.expressions {
            let unit_meta = self.unit_meta();
            nodes.push(
                self.unify(
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
            nodes.push(self.unify_prop_match_arm(sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn wrap_sub_scoped_props_in_scope(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope_kind {
            scope::Kind::Const | scope::Kind::Var(_) => {
                self.unify_sub_scoped_props(scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
            scope::Kind::Let(let_scope) => {
                let const_scope = self.const_scope();
                let block = self.unify_sub_scoped_props(const_scope, sub_scoped)?;
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
            scope_kind @ scope::Kind::Struct(_) => {
                // debug!("struct scope: {struct_scope:#?}");
                // debug!("expressions: {sub_scoped:#?}");

                let scope = scope::Scope(scope_kind, scope_meta);
                let mut nodes =
                    Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

                for prop in sub_scoped.expressions {
                    // FIXME: avoid clone by taking scope reference?
                    let rel = self.unify(scope.clone(), prop.attr.rel)?;
                    let val = self.unify(scope.clone(), prop.attr.val)?;

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
                        meta: self.unit_meta(),
                    });
                }

                // TODO: sub_scoped
                for (_sub_prop_scope, _sub_scoped_prop) in sub_scoped.sub_scopes {
                    todo!()
                }

                Ok(nodes)
            }
            scope_kind @ scope::Kind::Gen(_) => {
                self.unify_sub_scoped_props(scope::Scope(scope_kind, scope_meta), sub_scoped)
            }
        }
    }
}

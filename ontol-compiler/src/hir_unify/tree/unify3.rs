use ontol_hir::kind::{
    Attribute, Dimension, MatchArm, NodeKind, Optional, PropPattern, PropVariant,
};
use ontol_runtime::DefId;
use tracing::debug;

use crate::{
    hir_unify::{unifier::UnifierResult, UnifierError, VarSet},
    mem::Intern,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef, Types},
    SourceSpan,
};

use super::{
    expr,
    hierarchy::{Hierarchy, HierarchyBuilder},
    scope,
};

pub struct Unifier3<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
}

pub struct UnifiedNode3<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

struct UnifiedBlock3<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub block: Vec<TypedHirNode<'m>>,
}

impl<'a, 'm> Unifier3<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>) -> Self {
        Self { types }
    }

    pub fn unify3(
        &mut self,
        scope: scope::Scope<'m>,
        expr: expr::Expr<'m>,
    ) -> Result<UnifiedNode3<'m>, UnifierError> {
        // FIXME: This can be a loop instead of forced recursion,
        // for simple cases where the unified node is returned as-is from the layer below.

        match (expr.kind, scope.kind) {
            // ### need to return scope binder
            (kind, scope::Kind::Var(var)) => {
                // TODO: remove const_scope, have a way to ergonomically re-assemble the scope
                let const_scope = self.const_scope();
                let unified = self.unify3(
                    const_scope,
                    expr::Expr {
                        kind,
                        meta: expr.meta,
                        free_vars: expr.free_vars,
                    },
                )?;

                Ok(UnifiedNode3 {
                    typed_binder: Some(TypedBinder {
                        var,
                        ty: scope.meta.ty,
                    }),
                    node: unified.node,
                })
            }
            // ### Expr constants - no scope needed:
            (expr::Kind::Unit, _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta: expr.meta,
                },
            }),
            (expr::Kind::Int(int), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta: expr.meta,
                },
            }),
            // ### forced scope expansions:
            (kind, scope::Kind::Let(let_scope)) => {
                let unified = self.unify3(
                    *let_scope.sub_scope,
                    expr::Expr {
                        kind,
                        meta: expr.meta,
                        free_vars: expr.free_vars,
                    },
                )?;
                let expr_meta = unified.node.meta;

                Ok(UnifiedNode3 {
                    typed_binder: let_scope.outer_binder,
                    node: TypedHirNode {
                        kind: NodeKind::Let(
                            let_scope.inner_binder,
                            Box::new(let_scope.def),
                            vec![unified.node],
                        ),
                        meta: expr_meta,
                    },
                })
            }
            // ### Simple scopes, no swizzling needed:
            (expr::Kind::Var(var), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Var(var),
                    meta: expr.meta,
                },
            }),
            // ### Unit scopes:
            (expr::Kind::Prop(prop), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let rel = self.unify3(const_scope.clone(), prop.attr.rel)?;
                let val = self.unify3(const_scope, prop.attr.val)?;

                Ok(UnifiedNode3 {
                    typed_binder: None,
                    node: TypedHirNode {
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
                        meta: expr.meta,
                    },
                })
            }
            (expr::Kind::Call(call), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let args = call
                    .1
                    .into_iter()
                    .map(|arg| {
                        self.unify3(const_scope.clone(), arg)
                            .map(|unified| unified.node)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(UnifiedNode3 {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Call(call.0, args),
                        meta: expr.meta,
                    },
                })
            }
            (expr::Kind::Map(param), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let unified_param = self.unify3(const_scope, *param)?;

                Ok(UnifiedNode3 {
                    typed_binder: unified_param.typed_binder,
                    node: TypedHirNode {
                        kind: NodeKind::Map(Box::new(unified_param.node)),
                        meta: expr.meta,
                    },
                })
            }
            (expr::Kind::Struct(struct_expr), scope::Kind::Const) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Struct(struct_expr.0, vec![]),
                    meta: expr.meta,
                },
            }),
            // ### "zwizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let prop_hierarchy = HierarchyBuilder::new(struct_scope.1)?.build(struct_expr.1);
                let mut nodes = Vec::with_capacity(prop_hierarchy.len());
                for level in prop_hierarchy {
                    nodes.push(self.unify_merged_prop_scope(level)?);
                }

                Ok(UnifiedNode3 {
                    typed_binder: Some(TypedBinder {
                        var: struct_scope.0 .0,
                        ty: scope.meta.ty,
                    }),
                    node: TypedHirNode {
                        kind: NodeKind::Struct(struct_expr.0, nodes),
                        meta: expr.meta,
                    },
                })
            }
            (expr, scope) => panic!("unhandled expr/scope combo: {expr:#?} / {scope:#?}"),
        }
    }

    fn fully_wrap_in_scope(
        &mut self,
        scope: scope::Scope<'m>,
        nodes_func: impl FnOnce(&mut Self, scope::Scope<'m>) -> UnifierResult<Vec<TypedHirNode<'m>>>,
    ) -> UnifierResult<UnifiedBlock3<'m>> {
        match scope.kind {
            scope::Kind::Const => {
                let const_scope = self.const_scope();
                let nodes = nodes_func(self, const_scope)?;

                Ok(UnifiedBlock3 {
                    typed_binder: None,
                    block: nodes,
                })
            }
            scope::Kind::Var(var) => {
                let const_scope = self.const_scope();
                let nodes = nodes_func(self, const_scope)?;

                Ok(UnifiedBlock3 {
                    typed_binder: Some(TypedBinder {
                        var,
                        ty: scope.meta.ty,
                    }),
                    block: nodes,
                })
            }
            scope::Kind::Struct(struct_scope) => {
                let typed_binder = TypedBinder {
                    var: struct_scope.0 .0,
                    ty: scope.meta.ty,
                };
                let nodes = nodes_func(
                    self,
                    scope::Scope {
                        kind: scope::Kind::Struct(struct_scope),
                        vars: scope.vars,
                        meta: scope.meta,
                    },
                )?;

                Ok(UnifiedBlock3 {
                    typed_binder: Some(typed_binder),
                    block: nodes,
                })
            }
            scope::Kind::Let(let_scope) => {
                let inner_block = self.fully_wrap_in_scope(*let_scope.sub_scope, nodes_func)?;
                let ty = inner_block
                    .block
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                let node = TypedHirNode {
                    kind: NodeKind::Let(
                        let_scope.inner_binder,
                        Box::new(let_scope.def),
                        inner_block.block,
                    ),
                    meta: Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(UnifiedBlock3 {
                    typed_binder: let_scope.outer_binder,
                    block: vec![node],
                })
            }
        }
    }

    fn unify_merged_prop_scope(
        &mut self,
        level: Hierarchy<scope::Prop<'m>, expr::Prop<'m>>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let level_props = level.expressions;

        let sub_nodes = level
            .children
            .into_iter()
            .map(|sub_level| self.unify_merged_prop_scope(sub_level))
            .collect::<Result<Vec<_>, _>>()?;

        // FIXME: re-grouped match arms
        let match_arm: MatchArm<_> = match level.scope.kind {
            scope::PropKind::Attr(rel_scope, val_scope) => {
                let hir_rel_binding = rel_scope.hir_pattern_binding();
                let hir_val_binding = val_scope.hir_pattern_binding();

                // TODO:
                // level_props must be zwizzled against rel and val scopes (scope index 0 and 1).
                // this looks a bit like struct prop zwizzling.
                // all props that share a scope function of the same binding
                // are grouped into the same scope.
                // Also we can support scope functions that combine rel and val in some way.
                // There should be no `fn fully_wrap_in_scope`.
                // If an output property has _disjoint scope_ for rel and val, there is no point
                // wrapping it in scope.

                // FIXME: if both scopes are defined, one should be "inside" the other
                let scope = match val_scope {
                    scope::PatternBinding::Wildcard(_) => self.const_scope(),
                    scope::PatternBinding::Scope(_, scope) => scope,
                };

                let unified_block = self.fully_wrap_in_scope(scope, move |zelf, scope| {
                    let mut nodes = vec![];

                    let prop_exprs: Vec<_> = level_props
                        .into_iter()
                        .map(|prop| zelf.mk_prop_expr(prop))
                        .collect();

                    for expr in prop_exprs {
                        nodes.push(zelf.unify3(scope.clone(), expr)?.node);
                    }

                    nodes.extend(sub_nodes);

                    Ok(nodes)
                })?;

                MatchArm {
                    pattern: PropPattern::Attr(hir_rel_binding, hir_val_binding),
                    nodes: unified_block.block,
                }
            }
            scope::PropKind::Seq(binding) => {
                todo!()
            }
        };

        let mut match_arms = vec![match_arm];

        if level.scope.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(TypedHirNode {
            kind: NodeKind::MatchProp(level.scope.struct_var, level.scope.prop_id, match_arms),
            meta: Meta {
                ty: self.types.intern(Type::Unit(DefId::unit())),
                span: SourceSpan::none(),
            },
        })
    }

    fn unify_merged_prop_scope2(
        &mut self,
        level: Hierarchy<scope::Prop<'m>, expr::Prop<'m>>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let level_props = level.expressions;

        let sub_nodes = level
            .children
            .into_iter()
            .map(|sub_level| self.unify_merged_prop_scope(sub_level))
            .collect::<Result<Vec<_>, _>>()?;

        // FIXME: re-grouped match arms
        let match_arm: MatchArm<_> = match level.scope.kind {
            scope::PropKind::Attr(rel_binding, val_binding) => {
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let attr_hierarchy = HierarchyBuilder::new(vec![
                    rel_binding.into_scope(),
                    val_binding.into_scope(),
                ])?
                .build(level_props);

                debug!("attr_hierarchy: {attr_hierarchy:#?}");

                panic!()
            }
            scope::PropKind::Seq(binding) => {
                todo!()
            }
        };

        let mut match_arms = vec![match_arm];

        if level.scope.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(TypedHirNode {
            kind: NodeKind::MatchProp(level.scope.struct_var, level.scope.prop_id, match_arms),
            meta: Meta {
                ty: self.types.intern(Type::Unit(DefId::unit())),
                span: SourceSpan::none(),
            },
        })
    }

    fn mk_prop_expr(&mut self, prop: expr::Prop<'m>) -> expr::Expr<'m> {
        let free_vars = prop.free_vars.clone();
        expr::Expr {
            kind: expr::Kind::Prop(Box::new(prop)),
            meta: Meta {
                ty: self.unit_type(),
                span: SourceSpan::none(),
            },
            free_vars,
        }
    }

    fn unit(&mut self) -> TypedHirNode<'m> {
        TypedHirNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.unit_type(),
                span: SourceSpan::none(),
            },
        }
    }

    fn const_scope(&mut self) -> scope::Scope<'m> {
        scope::Scope {
            kind: scope::Kind::Const,
            meta: self.unit_meta(),
            vars: VarSet::default(),
        }
    }

    fn unit_meta(&mut self) -> Meta<'m> {
        Meta {
            ty: self.unit_type(),
            span: SourceSpan::none(),
        }
    }

    fn unit_type(&mut self) -> TypeRef<'m> {
        self.types.intern(Type::Unit(DefId::unit()))
    }
}

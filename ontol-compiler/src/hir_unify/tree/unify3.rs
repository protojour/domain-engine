use ontol_hir::kind::{
    Attribute, Dimension, MatchArm, NodeKind, Optional, PropPattern, PropVariant,
};
use ontol_runtime::DefId;

use crate::{
    hir_unify::{unifier::UnifierResult, UnifierError, VarSet},
    mem::Intern,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef, Types},
    SourceSpan,
};

use super::{
    expr,
    prop_hierarchy::{PropHierarchy, PropHierarchyBuilder},
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

struct ScopedProps<'m> {
    scope: scope::Prop<'m>,
    props: Vec<expr::Prop<'m>>,
}

impl<'a, 'm> Unifier3<'a, 'm> {
    pub(super) fn unify3(
        &mut self,
        scope: scope::Scope<'m>,
        expr: expr::Expr<'m>,
    ) -> Result<UnifiedNode3<'m>, UnifierError> {
        match (expr.kind, scope.kind) {
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
                let unit_scope = self.unit_scope();
                let rel = self.unify3(unit_scope.clone(), prop.attr.rel)?;
                let val = self.unify3(unit_scope, prop.attr.val)?;

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
                let unit_scope = self.unit_scope();
                let args = call
                    .1
                    .into_iter()
                    .map(|arg| {
                        self.unify3(unit_scope.clone(), arg)
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
            (expr::Kind::Struct(struct_expr), scope::Kind::Const) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Struct(struct_expr.0, vec![]),
                    meta: expr.meta,
                },
            }),
            // ### "zwizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let prop_hiearchy = PropHierarchyBuilder::new(struct_scope.1)?.build(struct_expr.1);
                let nodes = prop_hiearchy
                    .into_iter()
                    .map(|level| self.unify_merged_prop_scope(level))
                    .collect::<Result<_, _>>()?;

                Ok(UnifiedNode3 {
                    typed_binder: Some(TypedBinder {
                        variable: struct_scope.0 .0,
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
        nodes_func: impl FnOnce(&mut Self) -> UnifierResult<Vec<TypedHirNode<'m>>>,
    ) -> UnifierResult<UnifiedBlock3<'m>> {
        match scope.kind {
            scope::Kind::Const => {
                let nodes = nodes_func(self)?;

                Ok(UnifiedBlock3 {
                    typed_binder: None,
                    block: nodes,
                })
            }
            scope::Kind::Var(_) => todo!(),
            scope::Kind::Struct(_) => todo!(),
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
        level: PropHierarchy<'m>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let prop_exprs: Vec<_> = level
            .props
            .into_iter()
            .map(|prop| self.mk_prop_expr(prop))
            .collect();

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

                // FIXME: if both scopes are defined, one should be "inside" the other
                let scope = match val_scope {
                    scope::PatternBinding::Wildcard => self.unit_scope(),
                    scope::PatternBinding::Scope(_, scope) => scope,
                };

                let unified_block = self.fully_wrap_in_scope(scope, move |zelf| {
                    let mut nodes = vec![];
                    let unit_scope = zelf.unit_scope();
                    for expr in prop_exprs {
                        nodes.push(zelf.unify3(unit_scope.clone(), expr)?.node);
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

    fn unit_scope(&mut self) -> scope::Scope<'m> {
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

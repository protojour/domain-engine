use fnv::FnvHashMap;
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
    hierarchy::{HierarchyBuilder, SubScoped},
    scope,
};

pub struct Unifier3<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
}

pub struct UnifiedNode3<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
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
            (kind, scope_kind @ scope::Kind::Let(_)) => self.unify_scope_block(
                scope::Scope {
                    kind: scope_kind,
                    vars: scope.vars,
                    meta: scope.meta,
                },
                vec![expr::Expr {
                    kind,
                    meta: expr.meta,
                    free_vars: expr.free_vars,
                }],
            ),
            // ### Const scopes (fully expanded):
            (expr::Kind::Var(var), scope::Kind::Const) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Var(var),
                    meta: expr.meta,
                },
            }),
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
                let mut args = Vec::with_capacity(call.1.len());
                for arg in call.1 {
                    args.push(self.unify3(const_scope.clone(), arg)?.node);
                }
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
            (expr::Kind::Struct(struct_expr), kind @ scope::Kind::Const) => {
                let scope = scope::Scope {
                    kind,
                    vars: scope.vars,
                    meta: scope.meta,
                };
                let mut nodes = Vec::with_capacity(struct_expr.1.len());
                for prop in struct_expr.1 {
                    let meta = self.unit_meta();
                    nodes.push(
                        self.unify3(
                            scope.clone(),
                            expr::Expr {
                                free_vars: prop.free_vars.clone(),
                                kind: expr::Kind::Prop(Box::new(prop)),
                                meta,
                            },
                        )?
                        .node,
                    );
                }

                Ok(UnifiedNode3 {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Struct(struct_expr.0, nodes),
                        meta: expr.meta,
                    },
                })
            }
            // ### "zwizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let scoped_props: Vec<_> =
                    HierarchyBuilder::new(struct_scope.1)?.build(struct_expr.1);
                let mut nodes = Vec::with_capacity(scoped_props.len());
                for (prop_scope, sub_scoped) in scoped_props {
                    nodes.push(self.unify_prop_match_arm(prop_scope, sub_scoped)?.node);
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
            (expr_kind, scope::Kind::Struct(struct_scope)) => {
                let free_vars = expr.free_vars.clone();

                let expr = expr::Expr {
                    kind: expr_kind,
                    meta: expr.meta,
                    free_vars: expr.free_vars,
                };
                let mut sub_scoped: SubScoped<expr::Expr, scope::Prop> = SubScoped {
                    expressions: vec![expr],
                    sub_scopes: vec![],
                };

                let mut scope_idx_by_var: FnvHashMap<ontol_hir::Var, usize> = Default::default();

                for (scope_idx, prop_scope) in struct_scope.1.iter().enumerate() {
                    for var in &prop_scope.vars {
                        scope_idx_by_var.insert(var, scope_idx);
                    }
                }

                let mut scope_by_idx: FnvHashMap<usize, scope::Prop> =
                    struct_scope.1.into_iter().enumerate().collect();

                // build recursive scope structure by iterating free variables
                for var in &free_vars {
                    if let Some(scope_idx) = scope_idx_by_var.remove(&var) {
                        if let Some(scope_prop) = scope_by_idx.remove(&scope_idx) {
                            sub_scoped = SubScoped {
                                expressions: vec![],
                                sub_scopes: vec![(scope_prop, sub_scoped)],
                            };
                        }
                    }
                }

                let (scope_prop, sub_scoped) = sub_scoped.sub_scopes.into_iter().next().unwrap();

                let node = self.unify_expr_match_arm(scope_prop, sub_scoped)?.node;

                Ok(UnifiedNode3 {
                    typed_binder: Some(TypedBinder {
                        var: struct_scope.0 .0,
                        ty: scope.meta.ty,
                    }),
                    node,
                })
            }
        }
    }

    fn unify_scope_block(
        &mut self,
        scope: scope::Scope<'m>,
        expressions: Vec<expr::Expr<'m>>,
    ) -> UnifierResult<UnifiedNode3<'m>> {
        match scope.kind {
            scope::Kind::Const => {
                let block = self.unify_expressions(expressions)?;
                match block.len() {
                    0 => todo!("empty"),
                    1 => Ok(UnifiedNode3 {
                        typed_binder: None,
                        node: block.into_iter().next().unwrap(),
                    }),
                    _ => todo!("multiple nodes in const scope"),
                }
            }
            scope::Kind::Var(var) => {
                let block = self.unify_expressions(expressions)?;
                match block.len() {
                    0 => todo!("empty"),
                    1 => Ok(UnifiedNode3 {
                        typed_binder: Some(TypedBinder {
                            var,
                            ty: scope.meta.ty,
                        }),
                        node: block.into_iter().next().unwrap(),
                    }),
                    _ => todo!("multiple nodes in var scope"),
                }
            }
            scope::Kind::Let(let_scope) => {
                let block = self.unify_expressions(expressions)?;
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

                Ok(UnifiedNode3 {
                    typed_binder: let_scope.outer_binder,
                    node,
                })
            }
            // scope::Kind::Prop(scope_prop) => self.unify_scope_prop(*scope_prop, expressions),
            scope::Kind::Struct(struct_scope) => {
                debug!("struct scope: {struct_scope:#?}");
                debug!("expressions: {expressions:#?}");
                todo!("struct block scope");
            }
        }
    }

    fn unify_expressions(
        &mut self,
        expressions: Vec<expr::Expr<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let const_scope = self.const_scope();
        let mut nodes = vec![];
        for expr in expressions {
            nodes.push(self.unify3(const_scope.clone(), expr)?.node);
        }
        Ok(nodes)
    }

    fn unify_prop_match_arm(
        &mut self,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode3<'m>> {
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
            scope::PropKind::Seq(rel_binding, val_binding) => {
                todo!()
            }
        };

        let mut match_arms = vec![match_arm];

        if scope_prop.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(UnifiedNode3 {
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

    fn wrap_sub_scoped_props_in_scope(
        &mut self,
        scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope.kind {
            scope::Kind::Const | scope::Kind::Var(_) => {
                let const_scope = self.const_scope();
                self.unify_sub_scoped_props(const_scope, sub_scoped)
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
            scope::Kind::Struct(struct_scope) => {
                debug!("struct scope: {struct_scope:#?}");
                debug!("expressions: {sub_scoped:#?}");

                let scope = scope::Scope {
                    kind: scope::Kind::Struct(struct_scope),
                    meta: scope.meta,
                    vars: scope.vars,
                };

                let mut nodes =
                    Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

                for prop in sub_scoped.expressions {
                    // FIXME: avoid clone by taking scope reference?
                    let rel = self.unify3(scope.clone(), prop.attr.rel)?;
                    let val = self.unify3(scope.clone(), prop.attr.val)?;

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
        }
    }

    fn unify_sub_scoped_props(
        &mut self,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());
        for prop in sub_scoped.expressions {
            let unit_meta = self.unit_meta();
            nodes.push(
                self.unify3(
                    inner_scope.clone(),
                    expr::Expr {
                        kind: expr::Kind::Prop(Box::new(prop)),
                        meta: unit_meta,
                        free_vars: VarSet::default(),
                    },
                )?
                .node,
            );
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_scopes {
            nodes.push(self.unify_prop_match_arm(sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn unify_expr_match_arm(
        &mut self,
        scope_prop: scope::Prop<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<UnifiedNode3<'m>> {
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
            scope::PropKind::Seq(rel_binding, val_binding) => {
                todo!()
            }
        };

        let mut match_arms = vec![match_arm];

        if scope_prop.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(UnifiedNode3 {
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

    fn wrap_sub_scoped_expressions_in_scope(
        &mut self,
        scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope.kind {
            kind @ (scope::Kind::Const | scope::Kind::Var(_)) => self.unify_sub_scoped_expressions(
                scope::Scope {
                    kind,
                    meta: scope.meta,
                    vars: scope.vars,
                },
                sub_scoped,
            ),
            scope::Kind::Let(let_scope) => {
                let block = self.unify_sub_scoped_expressions(
                    scope::Scope {
                        kind: scope::Kind::Const,
                        meta: scope.meta,
                        vars: scope.vars,
                    },
                    sub_scoped,
                )?;
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
        }
    }

    fn unify_sub_scoped_expressions(
        &mut self,
        inner_scope: scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Expr<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut nodes =
            Vec::with_capacity(sub_scoped.expressions.len() + sub_scoped.sub_scopes.len());

        for expr in sub_scoped.expressions {
            nodes.push(self.unify3(inner_scope.clone(), expr)?.node);
        }
        for (sub_prop_scope, sub_scoped) in sub_scoped.sub_scopes {
            nodes.push(self.unify_expr_match_arm(sub_prop_scope, sub_scoped)?.node);
        }
        Ok(nodes)
    }

    fn join_attr_scope(
        &mut self,
        rel: scope::PatternBinding<'m>,
        val: scope::PatternBinding<'m>,
    ) -> scope::Scope<'m> {
        match (rel, val) {
            (scope::PatternBinding::Wildcard(_), scope::PatternBinding::Wildcard(_)) => {
                self.const_scope()
            }
            (scope::PatternBinding::Scope(_, rel), scope::PatternBinding::Wildcard(_)) => rel,
            (scope::PatternBinding::Wildcard(_), scope::PatternBinding::Scope(_, val)) => val,
            (scope::PatternBinding::Scope(_, rel), scope::PatternBinding::Scope(_, val)) => {
                match (rel.kind, val.kind) {
                    (
                        scope::Kind::Const | scope::Kind::Var(_),
                        scope::Kind::Const | scope::Kind::Var(_),
                    ) => {
                        // attribute scopes are special, we don't need to track the var binders, as
                        // those are handled outside this function
                        self.const_scope()
                    }
                    (scope::Kind::Let(_let_scope), _other)
                    | (_other, scope::Kind::Let(_let_scope)) => {
                        todo!("merge let scope")
                    }
                    (scope::Kind::Struct(rel_struct), scope::Kind::Struct(val_struct)) => {
                        // what binder is used is insignificant here.
                        let binder = val_struct.0;
                        let mut merged_props = rel_struct.1;
                        merged_props.extend(&mut val_struct.1.into_iter());

                        scope::Scope {
                            kind: scope::Kind::Struct(scope::Struct(binder, merged_props)),
                            vars: VarSet::default(),
                            meta: self.unit_meta(),
                        }
                    }
                    (rel_kind, val_kind) => {
                        todo!("merge attr scopes {rel_kind:?} + {val_kind:?}")
                    }
                }
            }
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

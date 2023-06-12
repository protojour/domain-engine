use fnv::FnvHashMap;
use indexmap::IndexSet;
use ontol_hir::{
    kind::{
        Attribute, Dimension, IterBinder, MatchArm, NodeKind, Optional, PatternBinding,
        PropPattern, PropVariant,
    },
    visitor::HirVisitor,
};
use ontol_runtime::DefId;
use tracing::{debug, warn};

use crate::{
    hir_unify::{
        expr_builder::ExprBuilder, scope_builder::ScopeBuilder, UnifierError, UnifierResult,
        VarSet, VariableTracker,
    },
    mem::Intern,
    typed_hir::{HirFunc, Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef, Types},
    Compiler, SourceSpan,
};

use super::{
    expr,
    hierarchy::{HierarchyBuilder, SubScoped},
    scope,
};

pub fn unify_to_function<'m>(
    scope: &TypedHirNode<'m>,
    expr: &TypedHirNode<'m>,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<HirFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_node(0, &scope);
    var_tracker.visit_node(0, &expr);

    let scope_ty = scope.meta.ty;
    let expr_ty = expr.meta.ty;

    let unit_type = compiler.types.intern(Type::Unit(DefId::unit()));

    let (scope_binder, next_var) = {
        let mut scope_builder = ScopeBuilder::new(var_tracker.next_variable(), unit_type);
        let scope_binder = scope_builder.build_scope_binder(scope)?;
        (scope_binder, scope_builder.next_var())
    };

    let (expr, next_var) = {
        let mut expr_builder = ExprBuilder::new(next_var);
        let expr = expr_builder.hir_to_expr(expr);
        (expr, expr_builder.next_var())
    };

    let unified = Unifier::new(&mut compiler.types, next_var).unify(scope_binder.scope, expr)?;

    debug!("unified node {}", unified.node);

    let hir_func = match unified.typed_binder {
        Some(arg) => {
            // NB: Error is used in unification tests
            if !matches!(scope_ty, Type::Error) {
                assert_eq!(arg.ty, scope_ty);
            }
            if !matches!(expr_ty, Type::Error) {
                assert_eq!(unified.node.meta.ty, expr_ty);
            }

            Ok(HirFunc {
                arg,
                body: unified.node,
            })
        }
        None => Err(UnifierError::NoInputBinder),
    };

    hir_func.map_err(|err| {
        warn!("unifier error: {err:?}");
        err
    })
}

pub struct Unifier<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
    next_var: ontol_hir::Var,
}

struct UnifiedNode<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>, next_var: ontol_hir::Var) -> Self {
        Self { types, next_var }
    }

    fn unify(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        expr::Expr(expr_kind, expr_meta): expr::Expr<'m>,
    ) -> Result<UnifiedNode<'m>, UnifierError> {
        // FIXME: This can be a loop instead of forced recursion,
        // for simple cases where the unified node is returned as-is from the layer below.

        // debug!(
        //     "unify expr::{} / scope::{}",
        //     expr_kind.debug_short(),
        //     scope_kind.debug_short()
        // );

        match (expr_kind, scope_kind) {
            // ### need to return scope binder
            (expr_kind, scope::Kind::Var(var)) => {
                // TODO: remove const_scope, have a way to ergonomically re-assemble the scope
                let const_scope = self.const_scope();
                let unified = self.unify(const_scope, expr::Expr(expr_kind, expr_meta))?;

                Ok(UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var,
                        ty: scope_meta.hir_meta.ty,
                    }),
                    node: unified.node,
                })
            }
            // ### Expr constants - no scope needed:
            (expr::Kind::Unit, _) => Ok(UnifiedNode {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta: expr_meta.hir_meta,
                },
            }),
            (expr::Kind::Int(int), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta: expr_meta.hir_meta,
                },
            }),
            // ### forced scope expansions:
            (expr_kind, scope_kind @ scope::Kind::Let(_)) => self.unify_scope_block(
                scope::Scope(scope_kind, scope_meta),
                vec![expr::Expr(expr_kind, expr_meta)],
            ),
            // ### Const scopes (fully expanded):
            (expr::Kind::Var(var), scope::Kind::Const) => Ok(UnifiedNode {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Var(var),
                    meta: expr_meta.hir_meta,
                },
            }),
            (expr::Kind::Prop(prop), scope::Kind::Const) => {
                if matches!(prop.seq, Some(_)) {
                    panic!("expected gen scope");
                }

                let const_scope = self.const_scope();
                let rel = self.unify(const_scope.clone(), prop.attr.rel)?;
                let val = self.unify(const_scope, prop.attr.val)?;

                Ok(UnifiedNode {
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
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            (expr::Kind::Prop(prop), scope::Kind::Gen(gen_scope)) => {
                if matches!(prop.seq, None) {
                    panic!("unexpected gen scope");
                }

                // FIXME: Array/seq types must take two parameters
                let seq_ty = self.types.intern(Type::Array(prop.attr.val.hir_meta().ty));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let push_unified = {
                    let joined_scope = self.join_attr_scope(rel_binding, val_binding);

                    let mut free_vars = VarSet::default();
                    free_vars.0.union_with(&prop.attr.rel.meta().free_vars.0);
                    free_vars.0.union_with(&prop.attr.val.meta().free_vars.0);

                    let unit_meta = self.unit_meta();

                    self.unify(
                        joined_scope,
                        expr::Expr(
                            expr::Kind::Push(gen_scope.output_seq, Box::new(prop.attr)),
                            expr::Meta {
                                hir_meta: unit_meta,
                                free_vars,
                            },
                        ),
                    )?
                };

                let gen_node = TypedHirNode {
                    kind: NodeKind::Gen(
                        gen_scope.input_seq,
                        IterBinder {
                            seq: PatternBinding::Binder(gen_scope.output_seq),
                            rel: hir_rel_binding,
                            val: hir_val_binding,
                        },
                        vec![push_unified.node],
                    ),
                    meta: Meta {
                        ty: seq_ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Prop(
                            Optional(false),
                            prop.struct_var,
                            prop.prop_id,
                            vec![PropVariant {
                                dimension: Dimension::Singular,
                                attr: Attribute {
                                    rel: Box::new(TypedHirNode {
                                        kind: NodeKind::Unit,
                                        meta: self.unit_meta(),
                                    }),
                                    val: Box::new(gen_node),
                                },
                            }],
                        ),
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            (expr::Kind::Seq(_label, attr), scope::Kind::Gen(gen_scope)) => {
                // FIXME: Array/seq types must take two parameters
                let seq_ty = self.types.intern(Type::Array(attr.val.hir_meta().ty));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_pattern_binding();
                let hir_val_binding = val_binding.hir_pattern_binding();

                let push_unified = {
                    let joined_scope = self.join_attr_scope(rel_binding, val_binding);
                    let unit_meta = self.unit_meta();

                    let mut free_vars = VarSet::default();
                    free_vars.0.union_with(&attr.rel.meta().free_vars.0);
                    free_vars.0.union_with(&attr.val.meta().free_vars.0);

                    self.unify(
                        joined_scope,
                        expr::Expr(
                            expr::Kind::Push(gen_scope.output_seq, attr),
                            expr::Meta {
                                hir_meta: unit_meta,
                                free_vars,
                            },
                        ),
                    )?
                };

                let gen_node = TypedHirNode {
                    kind: NodeKind::Gen(
                        gen_scope.input_seq,
                        IterBinder {
                            seq: PatternBinding::Binder(gen_scope.output_seq),
                            rel: hir_rel_binding,
                            val: hir_val_binding,
                        },
                        vec![push_unified.node],
                    ),
                    meta: Meta {
                        ty: seq_ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: gen_node,
                })
            }
            (expr::Kind::Call(call), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let mut args = Vec::with_capacity(call.1.len());
                for arg in call.1 {
                    args.push(self.unify(const_scope.clone(), arg)?.node);
                }
                Ok(UnifiedNode {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Call(call.0, args),
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            (expr::Kind::Map(param), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let unified_param = self.unify(const_scope, *param)?;

                Ok(UnifiedNode {
                    typed_binder: unified_param.typed_binder,
                    node: TypedHirNode {
                        kind: NodeKind::Map(Box::new(unified_param.node)),
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            (expr::Kind::Push(seq_var, attr), scope_kind) => {
                let scope = scope::Scope(scope_kind, scope_meta);
                let rel = self.unify(scope.clone(), attr.rel)?;
                let val = self.unify(scope, attr.val)?;

                Ok(UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var: seq_var,
                        ty: self.unit_type(),
                    }),
                    node: TypedHirNode {
                        kind: NodeKind::Push(
                            seq_var,
                            Attribute {
                                rel: Box::new(rel.node),
                                val: Box::new(val.node),
                            },
                        ),
                        meta: self.unit_meta(),
                    },
                })
            }
            (expr::Kind::Struct(struct_expr), scope_kind @ scope::Kind::Const) => {
                let scope = scope::Scope(scope_kind, scope_meta);
                let mut nodes = Vec::with_capacity(struct_expr.1.len());
                for prop in struct_expr.1 {
                    let hir_meta = self.unit_meta();
                    let expr_meta = expr::Meta {
                        free_vars: prop.free_vars.clone(),
                        hir_meta,
                    };
                    nodes.push(
                        self.unify(
                            scope.clone(),
                            expr::Expr(expr::Kind::Prop(Box::new(prop)), expr_meta),
                        )?
                        .node,
                    );
                }

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Struct(struct_expr.0, nodes),
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            // ### "zwizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let hierarchy = HierarchyBuilder::new(struct_scope.1)?.build(struct_expr.1);
                let mut nodes =
                    Vec::with_capacity(hierarchy.scoped.len() + hierarchy.unscoped.len());

                for (prop_scope, sub_scoped) in hierarchy.scoped {
                    nodes.push(self.unify_prop_match_arm(prop_scope, sub_scoped)?.node);
                }

                if !hierarchy.unscoped.is_empty() {
                    let const_scope = self.const_scope();
                    let unscoped_nodes = self.unify_sub_scoped_props(
                        const_scope,
                        SubScoped {
                            expressions: hierarchy.unscoped,
                            sub_scopes: vec![],
                        },
                    )?;
                    nodes.extend(unscoped_nodes);
                }

                Ok(UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var: struct_scope.0 .0,
                        ty: scope_meta.hir_meta.ty,
                    }),
                    node: TypedHirNode {
                        kind: NodeKind::Struct(struct_expr.0, nodes),
                        meta: expr_meta.hir_meta,
                    },
                })
            }
            (expr_kind, scope::Kind::Struct(struct_scope)) => {
                // When scoping an arbitrariy expression to a struct
                // it's important that the scoping is expanded in the correct
                // order. I.e. the variable representing the expression itself
                // should be the innermost scope expansion.
                let mut ordered_scope_vars: IndexSet<ontol_hir::Var> = Default::default();

                if let expr::Kind::Seq(label, _) = &expr_kind {
                    // label is the primary scope locator for a sequence
                    ordered_scope_vars.insert(ontol_hir::Var(label.0));
                }

                for free_var in &expr_meta.free_vars {
                    ordered_scope_vars.insert(free_var);
                }

                if ordered_scope_vars.is_empty() {
                    panic!("No free vars in {expr_kind:#?} with struct scope");
                }

                let expr = expr::Expr(expr_kind, expr_meta);
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
                for var in &ordered_scope_vars {
                    if let Some(scope_idx) = scope_idx_by_var.remove(var) {
                        if let Some(scope_prop) = scope_by_idx.remove(&scope_idx) {
                            sub_scoped = SubScoped {
                                expressions: vec![],
                                sub_scopes: vec![(scope_prop, sub_scoped)],
                            };
                        }
                    }
                }

                let (scope_prop, sub_scoped) = match sub_scoped.sub_scopes.into_iter().next() {
                    Some(val) => val,
                    None => {
                        panic!("No sub scoping but free_vars was {ordered_scope_vars:?}")
                    }
                };

                let node = self.unify_expr_match_arm(scope_prop, sub_scoped)?.node;

                Ok(UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var: struct_scope.0 .0,
                        ty: scope_meta.hir_meta.ty,
                    }),
                    node,
                })
            }
            (expr::Kind::Seq(_label, _attr), _) => {
                panic!("Seq without gen scope")
            }
            (expr_kind, scope::Kind::Gen(_)) => {
                todo!("{expr_kind:#?} with gen scope")
            }
        }
    }

    fn unify_scope_block(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        expressions: Vec<expr::Expr<'m>>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        match scope_kind {
            scope::Kind::Const => {
                let block = self.unify_expressions(expressions)?;
                match block.len() {
                    0 => todo!("empty"),
                    1 => Ok(UnifiedNode {
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
                    1 => Ok(UnifiedNode {
                        typed_binder: Some(TypedBinder {
                            var,
                            ty: scope_meta.hir_meta.ty,
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

                Ok(UnifiedNode {
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
            scope::Kind::Gen(_) => todo!(),
        }
    }

    fn unify_expressions(
        &mut self,
        expressions: Vec<expr::Expr<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let const_scope = self.const_scope();
        let mut nodes = vec![];
        for expr in expressions {
            nodes.push(self.unify(const_scope.clone(), expr)?.node);
        }
        Ok(nodes)
    }

    fn unify_prop_match_arm(
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
                        vars: VarSet::default(),
                        hir_meta: self.unit_meta(),
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

    fn wrap_sub_scoped_props_in_scope(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        sub_scoped: SubScoped<expr::Prop<'m>, scope::Prop<'m>>,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        match scope_kind {
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

    fn unify_expr_match_arm(
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
                let block = self.unify_sub_scoped_expressions(
                    scope::Scope(scope::Kind::Const, scope_meta),
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
            scope::Kind::Gen(gen) => self.unify_sub_scoped_expressions(
                scope::Scope(scope::Kind::Gen(gen), scope_meta),
                sub_scoped,
            ),
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
            nodes.push(self.unify(inner_scope.clone(), expr)?.node);
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
                match (rel.0, val.0) {
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

                        scope::Scope(
                            scope::Kind::Struct(scope::Struct(binder, merged_props)),
                            scope::Meta {
                                vars: VarSet::default(),
                                hir_meta: self.unit_meta(),
                            },
                        )
                    }
                    (rel_kind, val_kind) => {
                        todo!("merge attr scopes {rel_kind:?} + {val_kind:?}")
                    }
                }
            }
        }
    }

    fn const_scope(&mut self) -> scope::Scope<'m> {
        scope::Scope(
            scope::Kind::Const,
            scope::Meta {
                hir_meta: self.unit_meta(),
                vars: VarSet::default(),
            },
        )
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

    fn alloc_var(&mut self) -> ontol_hir::Var {
        let var = self.next_var;
        self.next_var.0 += 1;
        var
    }
}

use fnv::FnvHashMap;
use indexmap::IndexSet;
use ontol_runtime::DefId;
use tracing::debug;

use crate::{
    hir_unify::{UnifierError, UnifierResult, VarSet},
    mem::Intern,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef, Types},
    SourceSpan,
};

use super::{
    dep_tree::{DepTreeBuilder, SubTree},
    expr,
    regroup_match_prop::regroup_match_prop,
    scope,
    unify_props::UnifyProps,
};

pub struct Unifier<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
    next_var: ontol_hir::Var,
}

pub(super) struct UnifiedNode<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>, next_var: ontol_hir::Var) -> Self {
        Self { types, next_var }
    }

    pub(super) fn unify(
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
                node: TypedHirNode(ontol_hir::Kind::Unit, expr_meta.hir_meta),
            }),
            (expr::Kind::Int(int), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: TypedHirNode(ontol_hir::Kind::Int(int), expr_meta.hir_meta),
            }),
            // ### forced scope expansions:
            (expr_kind, scope_kind @ scope::Kind::Let(_)) => self.unify_scope_block(
                scope::Scope(scope_kind, scope_meta),
                vec![expr::Expr(expr_kind, expr_meta)],
            ),
            // ### Const scopes (fully expanded):
            (expr::Kind::Var(var), scope::Kind::Const) => Ok(UnifiedNode {
                typed_binder: None,
                node: TypedHirNode(ontol_hir::Kind::Var(var), expr_meta.hir_meta),
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
                    node: TypedHirNode(
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
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr::Kind::Prop(prop), scope::Kind::Gen(gen_scope)) => {
                if matches!(prop.seq, None) {
                    panic!("unexpected gen scope");
                }

                // FIXME: Array/seq types must take two parameters
                let seq_ty = self.types.intern(Type::Array(prop.attr.val.hir_meta().ty));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_binding();
                let hir_val_binding = val_binding.hir_binding();

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

                let gen_node = TypedHirNode(
                    ontol_hir::Kind::Gen(
                        gen_scope.input_seq,
                        ontol_hir::IterBinder {
                            seq: ontol_hir::Binding::Binder(gen_scope.output_seq),
                            rel: hir_rel_binding,
                            val: hir_val_binding,
                        },
                        vec![push_unified.node],
                    ),
                    Meta {
                        ty: seq_ty,
                        span: SourceSpan::none(),
                    },
                );

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: TypedHirNode(
                        ontol_hir::Kind::Prop(
                            ontol_hir::Optional(false),
                            prop.struct_var,
                            prop.prop_id,
                            vec![ontol_hir::PropVariant {
                                dimension: ontol_hir::Dimension::Singular,
                                attr: ontol_hir::Attribute {
                                    rel: Box::new(TypedHirNode(
                                        ontol_hir::Kind::Unit,
                                        self.unit_meta(),
                                    )),
                                    val: Box::new(gen_node),
                                },
                            }],
                        ),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr::Kind::Seq(_label, attr), scope::Kind::Gen(gen_scope)) => {
                // FIXME: Array/seq types must take two parameters
                let seq_ty = self.types.intern(Type::Array(attr.val.hir_meta().ty));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_binding();
                let hir_val_binding = val_binding.hir_binding();

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

                let gen_node = TypedHirNode(
                    ontol_hir::Kind::Gen(
                        gen_scope.input_seq,
                        ontol_hir::IterBinder {
                            seq: ontol_hir::Binding::Binder(gen_scope.output_seq),
                            rel: hir_rel_binding,
                            val: hir_val_binding,
                        },
                        vec![push_unified.node],
                    ),
                    Meta {
                        ty: seq_ty,
                        span: SourceSpan::none(),
                    },
                );

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
                    node: TypedHirNode(ontol_hir::Kind::Call(call.0, args), expr_meta.hir_meta),
                })
            }
            (expr::Kind::Map(param), scope::Kind::Const) => {
                let const_scope = self.const_scope();
                let unified_param = self.unify(const_scope, *param)?;

                Ok(UnifiedNode {
                    typed_binder: unified_param.typed_binder,
                    node: TypedHirNode(
                        ontol_hir::Kind::Map(Box::new(unified_param.node)),
                        expr_meta.hir_meta,
                    ),
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
                    node: TypedHirNode(
                        ontol_hir::Kind::Push(
                            seq_var,
                            ontol_hir::Attribute {
                                rel: Box::new(rel.node),
                                val: Box::new(val.node),
                            },
                        ),
                        self.unit_meta(),
                    ),
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
                    node: TypedHirNode(
                        ontol_hir::Kind::Struct(struct_expr.0, nodes),
                        expr_meta.hir_meta,
                    ),
                })
            }
            // ### "zwizzling" cases:
            (
                expr::Kind::Struct(expr::Struct(expr_binder, props)),
                scope::Kind::PropSet(scope::PropSet(scope_binder, scope_props)),
            ) => {
                let dep_tree = DepTreeBuilder::new(scope_props)?.build(props);
                let mut nodes = Vec::with_capacity(dep_tree.trees.len() + dep_tree.constants.len());

                for (prop_scope, sub_scoped) in dep_tree.trees {
                    nodes.push(expr::Prop::unify_match_arm(self, prop_scope, sub_scoped)?.node);
                }

                if !dep_tree.constants.is_empty() {
                    let const_scope = self.const_scope();
                    let unscoped_nodes = expr::Prop::unify_sub_scoped(
                        self,
                        const_scope,
                        SubTree {
                            expressions: dep_tree.constants,
                            sub_trees: vec![],
                        },
                    )?;
                    nodes.extend(unscoped_nodes);
                }

                let nodes = regroup_match_prop(nodes);

                Ok(UnifiedNode {
                    typed_binder: scope_binder.map(|binder| TypedBinder {
                        var: binder.0,
                        ty: scope_meta.hir_meta.ty,
                    }),
                    node: TypedHirNode(
                        ontol_hir::Kind::Struct(expr_binder, nodes),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr_kind, scope::Kind::PropSet(scope::PropSet(scope_binder, scope_props))) => {
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
                let mut sub_scoped: SubTree<expr::Expr, scope::Prop> = SubTree {
                    expressions: vec![expr],
                    sub_trees: vec![],
                };

                let mut scope_idx_by_var: FnvHashMap<ontol_hir::Var, usize> = Default::default();

                for (scope_idx, prop_scope) in scope_props.iter().enumerate() {
                    for var in &prop_scope.vars {
                        scope_idx_by_var.insert(var, scope_idx);
                    }
                }

                let mut scope_by_idx: FnvHashMap<usize, scope::Prop> =
                    scope_props.into_iter().enumerate().collect();

                // build recursive scope structure by iterating free variables
                for var in &ordered_scope_vars {
                    if let Some(scope_idx) = scope_idx_by_var.remove(var) {
                        if let Some(scope_prop) = scope_by_idx.remove(&scope_idx) {
                            sub_scoped = SubTree {
                                expressions: vec![],
                                sub_trees: vec![(scope_prop, sub_scoped)],
                            };
                        }
                    }
                }

                let (scope_prop, sub_scoped) = match sub_scoped.sub_trees.into_iter().next() {
                    Some(val) => val,
                    None => {
                        panic!("No sub scoping but free_vars was {ordered_scope_vars:?}")
                    }
                };

                let node = expr::Expr::unify_match_arm(self, scope_prop, sub_scoped)?.node;

                Ok(UnifiedNode {
                    typed_binder: scope_binder.map(|binder| TypedBinder {
                        var: binder.0,
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
                    .map(TypedHirNode::ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                let node = TypedHirNode(
                    ontol_hir::Kind::Let(let_scope.inner_binder, Box::new(let_scope.def), block),
                    Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                );

                Ok(UnifiedNode {
                    typed_binder: let_scope.outer_binder,
                    node,
                })
            }
            // scope::Kind::Prop(scope_prop) => self.unify_scope_prop(*scope_prop, expressions),
            scope::Kind::PropSet(prop_set) => {
                debug!("scope prop set: {prop_set:#?}");
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

    pub(super) fn join_attr_scope(
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
                    (scope::Kind::PropSet(rel_struct), scope::Kind::PropSet(val_struct)) => {
                        let mut merged_props = rel_struct.1;
                        merged_props.extend(&mut val_struct.1.into_iter());

                        scope::Scope(
                            scope::Kind::PropSet(scope::PropSet(None, merged_props)),
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

    pub(super) fn const_scope(&mut self) -> scope::Scope<'m> {
        scope::Scope(
            scope::Kind::Const,
            scope::Meta {
                hir_meta: self.unit_meta(),
                vars: VarSet::default(),
            },
        )
    }

    pub(super) fn unit_meta(&mut self) -> Meta<'m> {
        Meta {
            ty: self.unit_type(),
            span: SourceSpan::none(),
        }
    }

    pub(super) fn unit_type(&mut self) -> TypeRef<'m> {
        self.types.intern(Type::Unit(DefId::unit()))
    }

    pub(super) fn alloc_var(&mut self) -> ontol_hir::Var {
        let var = self.next_var;
        self.next_var.0 += 1;
        var
    }
}

use fnv::FnvHashMap;
use indexmap::IndexSet;
use ontol_runtime::var::Var;
use tracing::{debug, trace};

use crate::{
    hir_unify::{UnifierError, UnifierResult, VarSet},
    mem::Intern,
    typed_hir::{arena_import, IntoTypedHirData, Meta, TypedHir, TypedHirData, UNIT_META},
    types::{Type, Types, UNIT_TYPE},
    NO_SPAN, USE_FLAT_SEQ_HANDLING,
};

use super::{
    dep_tree::{DepTreeBuilder, SubTree},
    expr::{self, StringInterpolationComponent},
    regroup_match_prop::regroup_match_prop,
    scope,
    unify_props::UnifyProps,
};

const UNIFIER_DEBUG: bool = true;

pub struct Unifier<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
    pub(super) var_allocator: ontol_hir::VarAllocator,
    pub(super) hir_arena: ontol_hir::arena::Arena<'m, TypedHir>,
}

pub(super) struct UnifiedNode<'m> {
    pub typed_binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub node: ontol_hir::Node,
}

pub(super) struct UnifiedRootNode<'m> {
    pub typed_binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub node: ontol_hir::RootNode<'m, TypedHir>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>, var_allocator: ontol_hir::VarAllocator) -> Self {
        Self {
            types,
            var_allocator,
            hir_arena: Default::default(),
        }
    }

    pub(super) fn unify(
        &mut self,
        scope::Scope(scope_kind, scope_meta): scope::Scope<'m>,
        expr::Expr(expr_kind, expr_meta): expr::Expr<'m>,
    ) -> Result<UnifiedNode<'m>, UnifierError> {
        // FIXME: This can be a loop instead of forced recursion,
        // for simple cases where the unified node is returned as-is from the layer below.

        if UNIFIER_DEBUG {
            debug!(
                "unify expr::{} / scope::{}",
                expr_kind.debug_short(),
                scope_kind.debug_short()
            );
        }

        match (expr_kind, scope_kind) {
            // ### need to return scope binder
            (expr_kind, scope::Kind::Var(var)) => {
                // TODO: remove const_scope, have a way to ergonomically re-assemble the scope
                let unified = self.unify(scope::constant(), expr::Expr(expr_kind, expr_meta))?;

                Ok(UnifiedNode {
                    typed_binder: Some(ontol_hir::Binder { var }.with_meta(scope_meta.hir_meta)),
                    node: unified.node,
                })
            }
            (expr_kind, scope::Kind::Regex(string_var, regex_def_id, scope_capture_groups)) => {
                let unified =
                    self.unify(scope::constant(), expr::Expr(expr_kind, expr_meta.clone()))?;

                let capture_groups = scope_capture_groups
                    .iter()
                    .map(|scope_capture_group| ontol_hir::CaptureGroup {
                        index: scope_capture_group.index,
                        binder: scope_capture_group.binder,
                    })
                    .collect();

                Ok(UnifiedNode {
                    typed_binder: Some(
                        ontol_hir::Binder { var: string_var }.with_meta(scope_meta.hir_meta),
                    ),
                    node: self.mk_node(
                        ontol_hir::Kind::MatchRegex(
                            ontol_hir::Iter(false),
                            string_var,
                            regex_def_id,
                            vec![ontol_hir::CaptureMatchArm {
                                capture_groups,
                                nodes: [unified.node].into_iter().collect(),
                            }],
                        ),
                        expr_meta.hir_meta,
                    ),
                })
            }
            // ### Expr constants - no scope needed:
            (expr::Kind::Unit, _) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::Unit, expr_meta.hir_meta),
            }),
            (expr::Kind::I64(int), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::I64(int), expr_meta.hir_meta),
            }),
            (expr::Kind::F64(float), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::F64(float), expr_meta.hir_meta),
            }),
            (expr::Kind::String(string), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::Text(string), expr_meta.hir_meta),
            }),
            (expr::Kind::Const(const_def_id), _) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::Const(const_def_id), expr_meta.hir_meta),
            }),
            // ### forced scope expansions:
            (expr_kind, scope_kind @ scope::Kind::Let(_)) => self.unify_scope_block(
                scope::Scope(scope_kind, scope_meta),
                vec![expr::Expr(expr_kind, expr_meta)],
            ),
            // ### Const scopes (fully expanded):
            (expr::Kind::Var(var), scope::Kind::Const) => Ok(UnifiedNode {
                typed_binder: None,
                node: self.mk_node(ontol_hir::Kind::Var(var), expr_meta.hir_meta),
            }),
            (expr::Kind::Prop(prop), scope::Kind::Const) => match prop.variant {
                expr::PropVariant::Singleton(attr) => {
                    let rel = self.unify(scope::constant(), attr.rel)?;
                    let val = self.unify(scope::constant(), attr.val)?;

                    Ok(UnifiedNode {
                        typed_binder: None,
                        node: self.mk_node(
                            ontol_hir::Kind::Prop(
                                ontol_hir::Optional(false),
                                prop.struct_var,
                                prop.prop_id,
                                [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                    rel: rel.node,
                                    val: val.node,
                                })]
                                .into(),
                            ),
                            expr_meta.hir_meta,
                        ),
                    })
                }
                expr::PropVariant::Set { label, elements } => {
                    if elements.is_empty() {
                        panic!("No elements in seq prop");
                    }
                    let (_, first_element_attr) = elements.first().unwrap();

                    // FIXME: This is _probably_ incorrect.
                    // The type of the elements in the sequence must be the common supertype of all the elements.
                    // So the type needs to be carried over from somewhere else.
                    let seq_ty = self.types.intern(Type::Seq(
                        first_element_attr.rel.hir_meta().ty,
                        first_element_attr.val.hir_meta().ty,
                    ));

                    let mut sequence_body = Vec::with_capacity(elements.len());

                    for (iter, attr) in elements {
                        if iter.0 {
                            panic!("Iter element with const scope. Scope must be Gen");
                        }

                        let mut free_vars = VarSet::default();
                        free_vars.0.union_with(&attr.rel.meta().free_vars.0);
                        free_vars.0.union_with(&attr.val.meta().free_vars.0);

                        let push_unified = self.unify(
                            scope::constant(),
                            expr::Expr(
                                expr::Kind::Push(Var(label.0), Box::new(attr)),
                                expr::Meta {
                                    hir_meta: UNIT_META,
                                    free_vars,
                                },
                            ),
                        )?;

                        sequence_body.push(push_unified.node);
                    }

                    let sequence_node = self.mk_node(
                        ontol_hir::Kind::Sequence(
                            ontol_hir::Binder { var: Var(label.0) }.with_meta(Meta {
                                ty: seq_ty,
                                span: NO_SPAN,
                            }),
                            sequence_body.into(),
                        ),
                        Meta {
                            ty: seq_ty,
                            span: NO_SPAN,
                        },
                    );

                    let unit_node = self.mk_node(ontol_hir::Kind::Unit, UNIT_META);

                    Ok(UnifiedNode {
                        typed_binder: None,
                        node: self.mk_node(
                            ontol_hir::Kind::Prop(
                                ontol_hir::Optional(false),
                                prop.struct_var,
                                prop.prop_id,
                                [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                    rel: unit_node,
                                    val: sequence_node,
                                })]
                                .into(),
                            ),
                            expr_meta.hir_meta,
                        ),
                    })
                }
                expr::PropVariant::Predicate(_) => {
                    panic!("Should be solved in flat unifier");
                }
            },
            (expr::Kind::StringInterpolation(binder, components), scope::Kind::Const) => {
                let let_def = self.mk_node(ontol_hir::Kind::Text("".into()), expr_meta.hir_meta);
                let components: ontol_hir::Nodes = components
                    .into_iter()
                    .map(|component| {
                        let string_push_param = match component {
                            StringInterpolationComponent::Const(string) => {
                                self.mk_node(ontol_hir::Kind::Text(string), UNIT_META)
                            }
                            StringInterpolationComponent::Var(var, span) => self.mk_node(
                                ontol_hir::Kind::Var(var),
                                Meta {
                                    ty: UNIT_META.ty,
                                    span,
                                },
                            ),
                        };
                        self.mk_node(
                            ontol_hir::Kind::StringPush(binder.hir().var, string_push_param),
                            UNIT_META,
                        )
                    })
                    .collect();
                Ok(UnifiedNode {
                    typed_binder: None,
                    node: self.mk_node(
                        ontol_hir::Kind::Let(binder, let_def, components),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr::Kind::Prop(prop), scope::Kind::Escape(scope_kind)) => {
                let expr::PropVariant::Singleton(attr) = prop.variant else {
                    panic!("expected gen scope");
                };

                let rel = self.unify(
                    scope::Scope((*scope_kind).clone(), scope_meta.clone()),
                    attr.rel,
                )?;
                let val = self.unify(scope::Scope(*scope_kind, scope_meta), attr.val)?;

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: self.mk_node(
                        ontol_hir::Kind::Prop(
                            ontol_hir::Optional(false),
                            prop.struct_var,
                            prop.prop_id,
                            [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel: rel.node,
                                val: val.node,
                            })]
                            .into(),
                        ),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr::Kind::Prop(prop), scope::Kind::Gen(gen_scope)) => {
                let expr::PropVariant::Set { elements, .. } = prop.variant else {
                    panic!(
                        "unexpected gen scope: prop={:?} scope_input_seq={:?}",
                        prop.prop_id, gen_scope.input_seq
                    );
                };

                if elements.is_empty() {
                    panic!("No elements in seq prop");
                }
                let (_, first_element_attr) = elements.first().unwrap();

                // FIXME: This is _probably_ incorrect.
                // The type of the elements in the sequence must be the common supertype of all the elements.
                // So the type needs to be carried over from somewhere else.
                let seq_ty = self.types.intern(Type::Seq(
                    first_element_attr.rel.hir_meta().ty,
                    first_element_attr.val.hir_meta().ty,
                ));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_binding();
                let hir_val_binding = val_binding.hir_binding();

                let joined_scope = self.join_attr_scope(rel_binding, val_binding);

                let mut sequence_body: ontol_hir::Nodes = Default::default();

                for (iter, attr) in elements {
                    let mut free_vars = VarSet::default();
                    free_vars.0.union_with(&attr.rel.meta().free_vars.0);
                    free_vars.0.union_with(&attr.val.meta().free_vars.0);

                    let push_unified = self.unify(
                        joined_scope.clone(),
                        expr::Expr(
                            expr::Kind::Push(gen_scope.output_seq, Box::new(attr)),
                            expr::Meta {
                                hir_meta: UNIT_META,
                                free_vars,
                            },
                        ),
                    )?;

                    if iter.0 {
                        sequence_body.push(self.mk_node(
                            ontol_hir::Kind::ForEach(
                                gen_scope.input_seq,
                                (hir_rel_binding, hir_val_binding),
                                [push_unified.node].into_iter().collect(),
                            ),
                            UNIT_META,
                        ));
                    } else {
                        sequence_body.push(push_unified.node);
                    };
                }

                let sequence_node = self.mk_node(
                    ontol_hir::Kind::Sequence(
                        ontol_hir::Binder {
                            var: gen_scope.output_seq,
                        }
                        .with_meta(Meta {
                            ty: seq_ty,
                            span: scope_meta.hir_meta.span,
                        }),
                        sequence_body,
                    ),
                    Meta {
                        ty: seq_ty,
                        span: scope_meta.hir_meta.span,
                    },
                );

                let unit = self.mk_node(ontol_hir::Kind::Unit, UNIT_META);

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: self.mk_node(
                        ontol_hir::Kind::Prop(
                            ontol_hir::Optional(false),
                            prop.struct_var,
                            prop.prop_id,
                            [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel: unit,
                                val: sequence_node,
                            })]
                            .into(),
                        ),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr::Kind::DeclSet(_label, attr), scope::Kind::Gen(gen_scope)) => {
                let seq_ty = self
                    .types
                    .intern(Type::Seq(attr.rel.hir_meta().ty, attr.val.hir_meta().ty));

                let (rel_binding, val_binding) = *gen_scope.bindings;
                let hir_rel_binding = rel_binding.hir_binding();
                let hir_val_binding = val_binding.hir_binding();

                let push_unified = {
                    let joined_scope = self.join_attr_scope(rel_binding, val_binding);
                    let mut free_vars = VarSet::default();
                    free_vars.0.union_with(&attr.rel.meta().free_vars.0);
                    free_vars.0.union_with(&attr.val.meta().free_vars.0);

                    self.unify(
                        joined_scope,
                        expr::Expr(
                            expr::Kind::Push(gen_scope.output_seq, attr),
                            expr::Meta {
                                hir_meta: UNIT_META,
                                free_vars,
                            },
                        ),
                    )?
                };

                let for_each = self.mk_node(
                    ontol_hir::Kind::ForEach(
                        gen_scope.input_seq,
                        (hir_rel_binding, hir_val_binding),
                        [push_unified.node].into_iter().collect(),
                    ),
                    UNIT_META,
                );
                let sequence = self.mk_node(
                    ontol_hir::Kind::Sequence(
                        ontol_hir::Binder {
                            var: gen_scope.output_seq,
                        }
                        .with_meta(Meta {
                            ty: seq_ty,
                            span: scope_meta.hir_meta.span,
                        }),
                        [for_each].into_iter().collect(),
                    ),
                    Meta {
                        ty: seq_ty,
                        span: NO_SPAN,
                    },
                );

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: sequence,
                })
            }
            (expr::Kind::Call(call), scope::Kind::Const) => {
                let mut args = ontol_hir::Nodes::default();
                for arg in call.1 {
                    args.push(self.unify(scope::constant(), arg)?.node);
                }
                Ok(UnifiedNode {
                    typed_binder: None,
                    node: self.mk_node(ontol_hir::Kind::Call(call.0, args), expr_meta.hir_meta),
                })
            }
            (expr::Kind::Map(param), scope::Kind::Const) => {
                debug!(
                    "expr::Kind::Map outer_ty={:?} inner_ty={:?}",
                    expr_meta.hir_meta.ty,
                    param.meta().hir_meta.ty
                );
                let unified_param = self.unify(scope::constant(), *param)?;

                Ok(UnifiedNode {
                    typed_binder: unified_param.typed_binder,
                    node: self
                        .mk_node(ontol_hir::Kind::Map(unified_param.node), expr_meta.hir_meta),
                })
            }
            (expr::Kind::Push(seq_var, attr), scope_kind) => {
                let scope_hir_meta = scope_meta.hir_meta;
                let scope = scope::Scope(scope_kind, scope_meta);
                let rel = self.unify(scope.clone(), attr.rel)?;
                let val = self.unify(scope, attr.val)?;

                Ok(UnifiedNode {
                    typed_binder: Some(ontol_hir::Binder { var: seq_var }.with_meta(Meta {
                        ty: &UNIT_TYPE,
                        span: scope_hir_meta.span,
                    })),
                    node: self.mk_node(
                        ontol_hir::Kind::Insert(
                            seq_var,
                            ontol_hir::Attribute {
                                rel: rel.node,
                                val: val.node,
                            },
                        ),
                        UNIT_META,
                    ),
                })
            }
            // ### "zwizzling" cases:
            (
                expr::Kind::Struct {
                    binder: expr_binder,
                    flags,
                    props,
                    opt_output_type: _,
                },
                scope::Kind::PropSet(scope::PropSet(scope_binder, scope_props)),
            ) => {
                let dep_tree = DepTreeBuilder::new(scope_props)?.build(props);
                if UNIFIER_DEBUG {
                    trace!("dep_tree: {dep_tree:#?}");
                }
                for tree in &dep_tree.trees {
                    let subtree = &tree.1;
                    if !subtree.sub_trees.is_empty() {
                        // panic!("Using subtree");
                    }
                }
                let mut nodes = ontol_hir::Nodes::default();

                for (prop_scope, sub_tree) in dep_tree.trees {
                    nodes.push(expr::Prop::unify_match_arm(self, prop_scope, sub_tree)?.node);
                }

                for (scope_props, escaped_expr_prop) in dep_tree.escape_exprs {
                    // If there's only one scope_prop, unpack/unify it directly
                    if scope_props.len() == 1 {
                        let first_prop = scope_props.into_iter().next().unwrap();
                        nodes.push(
                            expr::Prop::unify_match_arm(
                                self,
                                first_prop,
                                SubTree {
                                    expressions: vec![escaped_expr_prop],
                                    sub_trees: vec![],
                                },
                            )?
                            .node,
                        );
                    } else {
                        let scope_kind =
                            scope::Kind::PropSet(scope::PropSet(scope_binder, scope_props));
                        nodes.push(
                            self.unify(
                                scope::Scope(
                                    scope::Kind::Escape(Box::new(scope_kind)),
                                    scope_meta.clone(),
                                ),
                                expr::Expr(
                                    expr::Kind::Prop(Box::new(escaped_expr_prop)),
                                    expr_meta.clone(),
                                ),
                            )?
                            .node,
                        );
                    };
                }

                if !dep_tree.constants.is_empty() {
                    let unscoped_nodes = expr::Prop::unify_sub_scoped(
                        self,
                        scope::constant(),
                        SubTree {
                            expressions: dep_tree.constants,
                            sub_trees: vec![],
                        },
                    )?;
                    nodes.extend(unscoped_nodes);
                }

                let nodes = regroup_match_prop(nodes, &mut self.hir_arena);

                Ok(UnifiedNode {
                    typed_binder: scope_binder.map(|binder| {
                        ontol_hir::Binder {
                            var: binder.hir().var,
                        }
                        .with_meta(scope_meta.hir_meta)
                    }),
                    node: self.mk_node(
                        ontol_hir::Kind::Struct(expr_binder, flags, nodes),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (
                expr::Kind::Struct {
                    binder,
                    flags,
                    props,
                    opt_output_type: _,
                },
                scope_kind,
            ) => {
                let scope = scope::Scope(scope_kind, scope_meta);
                let mut nodes = ontol_hir::Nodes::default();
                for prop in props {
                    let expr_meta = expr::Meta {
                        free_vars: prop.free_vars.clone(),
                        hir_meta: UNIT_META,
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
                    node: self.mk_node(
                        ontol_hir::Kind::Struct(binder, flags, nodes),
                        expr_meta.hir_meta,
                    ),
                })
            }
            (expr_kind, scope::Kind::PropSet(scope::PropSet(scope_binder, scope_props))) => {
                // When scoping an arbitrariy expression to a struct
                // it's important that the scoping is expanded in the correct
                // order. I.e. the variable representing the expression itself
                // should be the innermost scope expansion.
                let mut ordered_scope_vars: IndexSet<Var> = Default::default();

                if let expr::Kind::DeclSet(label, _) = &expr_kind {
                    // label is the primary scope locator for a sequence
                    ordered_scope_vars.insert(Var(label.0));
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

                let mut scope_idx_by_var: FnvHashMap<Var, usize> = Default::default();

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

                if let Some((scope_prop, sub_scoped)) = sub_scoped.sub_trees.into_iter().next() {
                    let node = expr::Expr::unify_match_arm(self, scope_prop, sub_scoped)?.node;

                    Ok(UnifiedNode {
                        typed_binder: scope_binder.map(|binder| {
                            ontol_hir::Binder {
                                var: binder.hir().var,
                            }
                            .with_meta(scope_meta.hir_meta)
                        }),
                        node,
                    })
                } else {
                    // Should be an expression that doesn't use any of the scope parameters
                    let expr = sub_scoped.expressions.into_iter().next().unwrap();
                    self.unify(scope::constant(), expr)
                }
            }
            (expr::Kind::DeclSet(_label, attr), _scope) => {
                if USE_FLAT_SEQ_HANDLING {
                    panic!("Should be solved in flat unifier");
                }
                // FIXME: This needs to be better
                let seq_ty = self
                    .types
                    .intern(Type::Seq(attr.rel.hir_meta().ty, attr.val.hir_meta().ty));

                let val = self.unify(scope::constant(), attr.val)?;

                self.hir_arena[val.node].meta_mut().ty = seq_ty;

                Ok(UnifiedNode {
                    typed_binder: None,
                    node: val.node,
                })
            }
            (expr::Kind::DestructuredSeq(..), _) => {
                panic!("Should not be used here")
            }
            (expr::Kind::SetElement(..), _) => {
                panic!("Only used in flat unifier")
            }
            (expr::Kind::HirNode(_), _) => {
                unreachable!()
            }
            (expr_kind, scope::Kind::Gen(_)) => {
                todo!("{expr_kind:#?} with gen scope")
            }
            (_expr_kind, scope::Kind::Escape(_)) => {
                panic!("non-prop Escape")
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
                        typed_binder: Some(
                            ontol_hir::Binder { var }.with_meta(scope_meta.hir_meta),
                        ),
                        node: block.into_iter().next().unwrap(),
                    }),
                    _ => todo!("multiple nodes in var scope"),
                }
            }
            scope::Kind::Let(let_scope) => {
                let block = self.unify_expressions(expressions)?;
                let ty = block
                    .iter()
                    .cloned()
                    .map(|node| self.hir_arena[node].meta().ty)
                    .last()
                    .unwrap_or(&UNIT_TYPE);

                let def = arena_import(&mut self.hir_arena, let_scope.def.as_ref());
                let node = self.mk_node(
                    ontol_hir::Kind::Let(let_scope.inner_binder, def, block.into()),
                    Meta { ty, span: NO_SPAN },
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
            scope::Kind::Gen(_) => todo!("Gen"),
            scope::Kind::Regex(..) => todo!("Regex"),
            scope::Kind::Escape(_) => todo!("Escape"),
        }
    }

    fn unify_expressions(
        &mut self,
        expressions: Vec<expr::Expr<'m>>,
    ) -> UnifierResult<Vec<ontol_hir::Node>> {
        let mut nodes = vec![];
        for expr in expressions {
            nodes.push(self.unify(scope::constant(), expr)?.node);
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
                scope::constant()
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
                        scope::constant()
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
                                dependencies: VarSet::default(),
                                hir_meta: UNIT_META,
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

    #[inline]
    pub(super) fn mk_node(
        &mut self,
        kind: ontol_hir::Kind<'m, TypedHir>,
        meta: Meta<'m>,
    ) -> ontol_hir::Node {
        self.hir_arena.add(TypedHirData(kind, meta))
    }
}

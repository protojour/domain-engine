use ontol_hir::{EvalCondTerm, StructFlags};
use ontol_runtime::{
    condition::Clause,
    smart_format,
    var::{Var, VarSet},
};
use smallvec::SmallVec;
use tracing::debug;

use crate::{
    hir_unify::{
        flat_scope, flat_unifier::StructuralOrigin, flat_unifier_impl::unify_scope_structural,
        flat_unifier_table::ExprSelector, seq_type_infer::SeqTypeInfer,
    },
    typed_hir::{self, IntoTypedHirData, Meta, TypedHir, TypedHirData, UNIT_META},
    NO_SPAN,
};

use super::{
    expr::{self, StringInterpolationComponent},
    flat_scope::{OutputVar, ScopeVar},
    flat_unifier::{unifier_todo, ExprMode, FlatUnifier, Level, MainScope},
    flat_unifier_table::Table,
    UnifierError, UnifierResult,
};

pub(super) struct ExprToHir<'t, 'u, 'a, 'm> {
    pub table: &'t mut Table<'m>,
    pub unifier: &'u mut FlatUnifier<'a, 'm>,
    pub scope_var: Option<ScopeVar>,
    pub level: Level,
}

impl<'t, 'u, 'a, 'm> ExprToHir<'t, 'u, 'a, 'm> {
    /// Convert an expression that has all its exposed free variables
    /// in scope, to a HIR node.
    pub fn expr_to_hir(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        in_scope: &VarSet,
        main_scope: MainScope,
    ) -> UnifierResult<ontol_hir::Node> {
        match kind {
            expr::Kind::Var(var) => Ok(self.mk_node(ontol_hir::Kind::Var(var), meta.hir_meta)),
            expr::Kind::Unit => Ok(self.mk_node(ontol_hir::Kind::Unit, meta.hir_meta)),
            expr::Kind::I64(int) => Ok(self.mk_node(ontol_hir::Kind::I64(int), meta.hir_meta)),
            expr::Kind::F64(float) => Ok(self.mk_node(ontol_hir::Kind::F64(float), meta.hir_meta)),
            expr::Kind::Const(def_id) => {
                Ok(self.mk_node(ontol_hir::Kind::Const(def_id), meta.hir_meta))
            }
            expr::Kind::Prop(prop) => self.prop_to_hir(*prop, in_scope, main_scope),
            expr::Kind::Call(expr::Call(proc, args)) => {
                let mut hir_args = ontol_hir::Nodes::default();
                for arg in args {
                    hir_args.push(self.expr_to_hir(arg, in_scope, main_scope.next())?);
                }
                Ok(self.mk_node(ontol_hir::Kind::Call(proc, hir_args), meta.hir_meta))
            }
            expr::Kind::Map(arg) => {
                let hir_arg = self.expr_to_hir(*arg, in_scope, main_scope.next())?;
                Ok(self.mk_node(ontol_hir::Kind::Map(hir_arg), meta.hir_meta))
            }
            expr::Kind::Struct {
                binder,
                flags,
                props,
                opt_output_type,
            } => {
                let level = self.level;

                debug!(
                    "{level}Make struct {} scope_var={:?} in_scope={:?} main_scope={main_scope:?}",
                    binder.hir().var,
                    self.scope_var,
                    in_scope
                );

                let flags = self.unifier.filter_struct_flags(flags);

                let body =
                    self.struct_body(&binder, flags, meta.hir_meta, props, in_scope, main_scope)?;
                let struct_node =
                    self.mk_node(ontol_hir::Kind::Struct(binder, flags, body), meta.hir_meta);
                Ok(self.unifier.maybe_map_node(struct_node, opt_output_type))
            }
            expr::Kind::SetElement(label, _index, _iter, attr) => {
                let (scope_var, output_var) = match main_scope {
                    MainScope::Sequence(scope_var, output_var) => (scope_var, output_var),
                    MainScope::MultiSequence(table) => {
                        let scope_var = ScopeVar(Var(label.0));
                        (scope_var, table.get(&label).unwrap().output_seq_var)
                    }
                    _ => (ScopeVar(Var(0)), OutputVar(Var(0))), // _ => panic!("Unsupported context for seq-item: {main_scope:?}"),
                };
                let next_main_scope = MainScope::Value(scope_var);

                let rel = self.expr_to_hir(attr.rel, in_scope, next_main_scope)?;
                let val = self.expr_to_hir(attr.val, in_scope, next_main_scope)?;

                Ok(self.mk_node(
                    ontol_hir::Kind::Insert(output_var.0, ontol_hir::Attribute { rel, val }),
                    UNIT_META,
                ))
            }
            expr::Kind::StringInterpolation(binder, components) => {
                let let_def = self.mk_node(ontol_hir::Kind::Text("".into()), meta.hir_meta);
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

                Ok(self.mk_node(
                    ontol_hir::Kind::Let(binder, let_def, components),
                    meta.hir_meta,
                ))
            }
            expr::Kind::HirNode(node) => Ok(node),
            other => Err(unifier_todo(smart_format!("leaf expr to hir: {other:?}"))),
        }
    }

    fn struct_body(
        &mut self,
        binder: &TypedHirData<'m, ontol_hir::Binder>,
        flags: StructFlags,
        hir_meta: typed_hir::Meta<'m>,
        props: Vec<expr::Prop<'m>>,
        in_scope: &VarSet,
        main_scope: MainScope,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let next_in_scope = in_scope.union_one(binder.hir().var);
        let mut body = ontol_hir::Nodes::default();

        self.unifier
            .push_struct_expr_flags(binder.0.var, flags, hir_meta.ty)?;

        if let ExprMode::Condition(cond_var, condition_root) = self.unifier.expr_mode() {
            self.unifier
                .push_struct_cond_clauses(cond_var, condition_root, *binder, &mut body);
        }

        if self.scope_var.is_some() {
            body.extend(unify_scope_structural(
                (
                    main_scope.next(),
                    match main_scope {
                        MainScope::Const => unreachable!(),
                        MainScope::Value(scope_var) => {
                            ExprSelector::Struct(binder.hir().var, scope_var)
                        }
                        MainScope::Sequence(scope_var, _) => {
                            ExprSelector::Struct(binder.hir().var, scope_var)
                        }
                        MainScope::MultiSequence(_) => panic!(),
                    },
                    self.level.next(),
                ),
                StructuralOrigin::Start,
                next_in_scope.clone(),
                self.table,
                self.unifier,
            )?);
        }

        for prop in props {
            body.push(self.prop_to_hir(prop, in_scope, main_scope)?);
        }

        self.unifier.pop_struct_expr_flags(flags);

        Ok(body)
    }

    fn prop_to_hir(
        &mut self,
        prop: expr::Prop<'m>,
        in_scope: &VarSet,
        main_scope: MainScope,
    ) -> UnifierResult<ontol_hir::Node> {
        match self.unifier.expr_mode() {
            ExprMode::Expr => {
                let variants: SmallVec<_> = match prop.variant {
                    expr::PropVariant::Singleton(attr) => {
                        [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                            rel: self.expr_to_hir(attr.rel, in_scope, main_scope.next())?,
                            val: self.expr_to_hir(attr.val, in_scope, main_scope.next())?,
                        })]
                        .into()
                    }
                    expr::PropVariant::Set { label, elements } => {
                        assert!(elements.is_empty());
                        let sequence_node = find_and_unify_sequence_prop(
                            prop.struct_var,
                            label,
                            in_scope,
                            self.table,
                            self.unifier,
                            self.level.next(),
                        )?;

                        [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                            rel: self.mk_node(ontol_hir::Kind::Unit, UNIT_META),
                            val: sequence_node,
                        })]
                        .into()
                    }
                    expr::PropVariant::Predicate { .. } => {
                        return Err(UnifierError::ImpossibleMapping);
                    }
                };

                Ok(self.mk_node(
                    ontol_hir::Kind::Prop(
                        ontol_hir::Optional(false),
                        prop.struct_var,
                        prop.prop_id,
                        variants,
                    ),
                    UNIT_META,
                ))
            }
            ExprMode::Condition(cond_var, _) => match prop.variant {
                expr::PropVariant::Singleton(attr) => {
                    let mut body = vec![];

                    let (rel, rel_nodes) =
                        self.expr_to_cond_term(attr.rel, in_scope, main_scope.next())?;
                    let (val, val_nodes) =
                        self.expr_to_cond_term(attr.val, in_scope, main_scope.next())?;

                    body.push(self.mk_node(
                        ontol_hir::Kind::PushCondClause(
                            cond_var,
                            Clause::Attr(prop.struct_var, prop.prop_id, (rel, val)),
                        ),
                        UNIT_META,
                    ));
                    body.extend(rel_nodes);
                    body.extend(val_nodes);

                    Ok(if body.len() == 1 {
                        body.into_iter().next().unwrap()
                    } else {
                        self.mk_node(ontol_hir::Kind::Begin(body.into()), UNIT_META)
                    })
                }
                expr::PropVariant::Set { .. } => Err(super::UnifierError::Unimplemented(
                    smart_format!("seq prop in condition: {}", prop.prop_id),
                )),
                expr::PropVariant::Predicate(_) => {
                    todo!()
                }
            },
        }
    }

    pub fn expr_to_cond_term(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        in_scope: &VarSet,
        main_scope: MainScope,
    ) -> UnifierResult<(EvalCondTerm, ontol_hir::Nodes)> {
        match kind {
            expr::Kind::Unit => Ok((EvalCondTerm::Wildcard, ontol_hir::Nodes::default())),
            expr::Kind::Struct {
                binder,
                flags,
                props,
                opt_output_type: _,
            } => {
                let var = binder.0.var;
                let body =
                    self.struct_body(&binder, flags, meta.hir_meta, props, in_scope, main_scope)?;
                Ok((EvalCondTerm::QuoteVar(var), body))
            }
            _ => {
                let node = self.expr_to_hir(expr::Expr(kind, meta), in_scope, main_scope)?;
                Ok((EvalCondTerm::Eval(node), ontol_hir::Nodes::default()))
            }
        }
    }

    #[inline]
    fn mk_node(&mut self, kind: ontol_hir::Kind<'m, TypedHir>, meta: Meta<'m>) -> ontol_hir::Node {
        self.unifier.mk_node(kind, meta)
    }
}

fn find_and_unify_sequence_prop<'m>(
    struct_var: Var,
    label: ontol_hir::Label,
    in_scope: &VarSet,
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
    level: Level,
) -> UnifierResult<ontol_hir::Node> {
    let output_seq_var = table
        .find_scope_map_by_scope_var(ScopeVar(Var(label.0)))
        .and_then(|(_, scope_map)| match scope_map.scope.kind() {
            flat_scope::Kind::SeqPropVariant(_, output_var, _, _, _, _) => Some(*output_var),
            _ => None,
        })
        .unwrap_or_else(|| {
            // This happens when there is no iteration at all, i.e. only !iter elements.
            OutputVar(unifier.var_allocator.alloc())
        });

    let next_in_scope = in_scope.union_one(output_seq_var.0);

    let scope_var = ScopeVar(Var(label.0));

    debug!("{level}going to unify scope for sequence prop");

    let sequence_body = unify_scope_structural(
        (
            MainScope::Sequence(scope_var, output_seq_var),
            ExprSelector::Struct(struct_var, ScopeVar(Var(label.0))),
            level.next(),
        ),
        // Start traversing from the beginning, to catch eventual non-iterated elements
        StructuralOrigin::Start,
        next_in_scope,
        table,
        unifier,
    )?;

    let mut seq_type_infer = SeqTypeInfer::new(output_seq_var);

    for node in &sequence_body {
        seq_type_infer.traverse(unifier.hir_arena.node_ref(*node));
    }

    let seq_ty = seq_type_infer.infer(unifier.types);

    debug!("seq_ty: {seq_ty:?}");

    let sequence_node = unifier.mk_node(
        ontol_hir::Kind::MakeSeq(
            ontol_hir::Binder {
                var: output_seq_var.0,
            }
            .with_meta(typed_hir::Meta {
                ty: seq_ty,
                span: NO_SPAN,
            }),
            sequence_body.into(),
        ),
        typed_hir::Meta {
            ty: seq_ty,
            span: NO_SPAN,
        },
    );

    Ok(sequence_node)
}

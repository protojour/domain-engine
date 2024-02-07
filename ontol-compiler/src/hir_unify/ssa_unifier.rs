use fnv::FnvHashMap;
use ontol_hir::{
    arena::NodeRef, find_value_node, Attribute, EvalCondTerm, PropFlags, PropVariant, SetEntry,
    StructFlags,
};
use ontol_runtime::{
    condition::Clause,
    smart_format,
    value::PropertyId,
    var::{Var, VarSet},
    MapFlags,
};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{
    def::Defs,
    hir_unify::{
        regex_interpolation::RegexStringInterpolator,
        ssa_util::{scan_immediate_free_vars, NodesExt},
    },
    mem::Intern,
    primitive::Primitives,
    relation::Relations,
    repr::repr_model::ReprKind,
    type_check::seal::SealCtx,
    typed_hir::{arena_import, Meta, TypedHir, TypedHirData, TypedNodeRef, UNIT_META},
    types::{Type, Types, UNIT_TYPE},
    Compiler, NO_SPAN,
};

use super::{
    regex_interpolation::StringInterpolationComponent,
    ssa_util::{
        scan_all_vars_and_labels, ExprMode, ExtendedScope, ScopeTracker, Scoped, TypeMapping,
    },
    unifier::UnifiedNode,
    UnifierError, UnifierResult,
};

/// Unifier that strives to produce Static Single-Assignment form flat hir blocks
pub struct SsaUnifier<'c, 'm> {
    #[allow(unused)]
    pub(super) types: &'c mut Types<'m>,
    #[allow(unused)]
    pub(super) relations: &'c Relations,
    pub(super) seal_ctx: &'c SealCtx,
    pub(super) defs: &'c Defs<'m>,
    pub(super) primitives: &'c Primitives,
    pub(super) var_allocator: ontol_hir::VarAllocator,
    pub(super) scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) out_arena: ontol_hir::arena::Arena<'m, TypedHir>,
    #[allow(unused)]
    pub(super) map_flags: MapFlags,

    #[allow(unused)]
    condition_root: Option<Var>,
    #[allow(unused)]
    match_struct_depth: usize,

    pub(super) all_scope_vars_and_labels: VarSet,
    pub(super) iter_extended_scope_table:
        FnvHashMap<ontol_hir::Label, Attribute<ExtendedScope<'m>>>,
    pub(super) scope_tracker: ScopeTracker<'m>,
}

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub fn new(
        scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        var_allocator: ontol_hir::VarAllocator,
        map_flags: MapFlags,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            types: &mut compiler.types,
            relations: &compiler.relations,
            seal_ctx: &compiler.seal_ctx,
            defs: &compiler.defs,
            primitives: &compiler.primitives,
            var_allocator,
            scope_arena,
            expr_arena,
            out_arena: Default::default(),
            map_flags,
            condition_root: None,
            match_struct_depth: 0,
            all_scope_vars_and_labels: Default::default(),
            iter_extended_scope_table: Default::default(),
            scope_tracker: Default::default(),
        }
    }

    pub(super) fn unify(
        &mut self,
        scope_node: ontol_hir::Node,
        expr_node: ontol_hir::Node,
    ) -> UnifierResult<UnifiedNode<'m>> {
        self.all_scope_vars_and_labels = scan_all_vars_and_labels(self.scope_arena, [scope_node]);

        self.block_unify(scope_node, expr_node, Scoped::Yes)
    }

    fn block_unify(
        &mut self,
        scope_node: ontol_hir::Node,
        expr_node: ontol_hir::Node,
        scoped: Scoped,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let expr = self.expr_arena.node_ref(expr_node);

        let block = self.prealloc_node();
        let mut block_body: ontol_hir::Nodes = Default::default();

        let scope_binding = self.define_scope(scope_node, scoped, &mut block_body)?;

        {
            // Need this to generate `MoveRestAttrs`. TODO: Refactor this.
            let scope_node = find_value_node(self.scope_arena.node_ref(scope_node));

            block_body.extend(self.write_expr(
                expr_node,
                scope_node,
                ExprMode::Expr(self.map_flags),
            )?);
        }

        self.write_node(block, ontol_hir::Kind::Block(block_body), *expr.meta());

        Ok(UnifiedNode {
            typed_binder: match scope_binding {
                ontol_hir::Binding::Wildcard => None,
                ontol_hir::Binding::Binder(binder_data) => Some(binder_data),
            },
            node: block,
        })
    }

    /// Wraps the expr in (block) if there are many expressions
    fn write_one_expr(
        &mut self,
        expr_node: ontol_hir::Node,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Node> {
        let nodes = self.write_expr(expr_node, None, mode)?;
        if nodes.len() == 1 {
            Ok(nodes.into_iter().next().unwrap())
        } else {
            let last_meta = *self
                .out_arena
                .node_ref(*nodes.iter().last().unwrap())
                .meta();
            Ok(self.mk_node(ontol_hir::Kind::Block(nodes), last_meta))
        }
    }

    fn write_expr(
        &mut self,
        expr_node: ontol_hir::Node,
        top_scope_node: Option<NodeRef<'c, 'm, TypedHir>>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let node_ref = self.expr_arena.node_ref(expr_node);
        match node_ref.kind() {
            ontol_hir::Kind::Set(entries) => {
                if entries.len() == 1 {
                    let ontol_hir::SetEntry(_, Attribute { rel, val }) = entries.first().unwrap();
                    match (self.expr_arena.kind_of(*rel), self.expr_arena.kind_of(*val)) {
                        (
                            ontol_hir::Kind::Unit,
                            ontol_hir::Kind::Struct(binder, flags, struct_body),
                        ) if flags.contains(StructFlags::MATCH) => {
                            let inner_set_ty =
                                self.types.intern(Type::Seq(&UNIT_TYPE, binder.meta().ty));
                            self.write_match_struct_expr(
                                *binder,
                                struct_body,
                                self.expr_arena.node_ref(*val),
                                TypeMapping {
                                    to: node_ref.ty(),
                                    from: inner_set_ty,
                                },
                                mode.match_struct(binder.hir().var),
                            )
                        }
                        _ => Ok(smallvec![self.write_make_seq(
                            entries.iter().map(|SetEntry(label, attr)| {
                                (label.map(|label| *label.hir()), *attr)
                            }),
                            *node_ref.meta(),
                            mode,
                        )?]),
                    }
                } else {
                    Err(UnifierError::TODO(smart_format!("Multi-entry set")))
                }
            }
            ontol_hir::Kind::Struct(binder, flags, struct_body) => {
                match (
                    flags.contains(StructFlags::MATCH),
                    mode.match_struct(binder.hir().var),
                ) {
                    (true, mode @ ExprMode::Match { .. }) => self.write_match_struct_expr(
                        *binder,
                        struct_body,
                        node_ref,
                        TypeMapping {
                            to: node_ref.ty(),
                            from: binder.ty(),
                        },
                        mode,
                    ),
                    _ => {
                        let next_mode = mode.any_struct();
                        let mut expr_body: ontol_hir::Nodes =
                            SmallVec::with_capacity(struct_body.len());
                        for node in struct_body {
                            expr_body.extend(self.write_expr(*node, None, next_mode)?);
                        }

                        // A hack for moving remaining properties from the source struct into the other struct:
                        match (mode, top_scope_node) {
                            (ExprMode::Expr(flags), Some(scope_node))
                                if !flags.contains(MapFlags::PURE_PARTIAL) =>
                            {
                                if let ontol_hir::Kind::Struct(scope_binder, ..) = scope_node.hir()
                                {
                                    expr_body.push(self.mk_node(
                                        ontol_hir::Kind::MoveRestAttrs(
                                            binder.hir().var,
                                            scope_binder.hir().var,
                                        ),
                                        Meta::new(&UNIT_TYPE, node_ref.meta().span),
                                    ));
                                }
                            }
                            _ => {}
                        }

                        Ok(smallvec![self.mk_node(
                            ontol_hir::Kind::Struct(*binder, StructFlags::empty(), expr_body),
                            *node_ref.meta(),
                        )])
                    }
                }
            }
            ontol_hir::Kind::Prop(optional, var, prop_id, variants) => {
                self.write_prop_expr((*optional, *var, *prop_id), variants, node_ref.meta(), mode)
            }
            ontol_hir::Kind::Regex(_seq_label, regex_def_id, capture_group_alternation) => {
                let regex_meta = self
                    .defs
                    .literal_regex_meta_table
                    .get(regex_def_id)
                    .unwrap();

                let first_alternation = capture_group_alternation.first().unwrap();

                let mut components = vec![];
                let mut interpolator = RegexStringInterpolator {
                    capture_groups: first_alternation,
                    current_constant: "".into(),
                    components: &mut components,
                };
                interpolator.traverse_hir(&regex_meta.hir);
                interpolator.commit_constant();

                let initial_string =
                    self.mk_node(ontol_hir::Kind::Text("".into()), *node_ref.meta());
                let with_node = self.prealloc_node();

                let string_binder = self.var_allocator.alloc();
                let mut body = ontol_hir::Nodes::default();

                for component in components {
                    let string_push_param = match component {
                        StringInterpolationComponent::Const(string) => {
                            self.mk_node(ontol_hir::Kind::Text(string), UNIT_META)
                        }
                        StringInterpolationComponent::Var(var, span) => self.mk_node(
                            ontol_hir::Kind::Var(var),
                            Meta {
                                ty: &UNIT_TYPE,
                                span,
                            },
                        ),
                    };
                    body.push(self.mk_node(
                        ontol_hir::Kind::StringPush(string_binder, string_push_param),
                        UNIT_META,
                    ));
                }

                Ok(smallvec![self.write_node(
                    with_node,
                    ontol_hir::Kind::With(
                        TypedHirData(ontol_hir::Binder { var: string_binder }, *node_ref.meta()),
                        initial_string,
                        body,
                    ),
                    *node_ref.meta()
                )])
            }
            _other => Ok(smallvec![arena_import(
                &mut self.out_arena,
                self.expr_arena.node_ref(expr_node),
            )]),
        }
    }

    fn write_prop_expr(
        &mut self,
        (flags, struct_var, prop_id): (PropFlags, Var, PropertyId),
        variants: &[ontol_hir::PropVariant<'m, TypedHir>],
        meta: &Meta<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        if variants.len() != 1 {
            panic!("Multi prop variants");
        }

        match (mode, variants.first().unwrap()) {
            (ExprMode::Expr(_), PropVariant::Singleton(Attribute { rel, val })) => {
                let free_vars = scan_immediate_free_vars(self.expr_arena, [*rel, *val]);
                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    let rel = zelf.write_one_expr(*rel, mode)?;
                    let val = zelf.write_one_expr(*val, mode)?;

                    Ok(smallvec![zelf.mk_node(
                        ontol_hir::Kind::Prop(
                            flags,
                            struct_var,
                            prop_id,
                            [ontol_hir::PropVariant::Singleton(Attribute { rel, val })].into(),
                        ),
                        *meta,
                    )])
                })
            }
            (ExprMode::Expr(_), PropVariant::Set(set_variant)) => {
                let free_vars = VarSet::from_iter([(*set_variant.label.hir()).into()]);
                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    zelf.write_set_prop((flags, struct_var, prop_id), set_variant, *meta, mode)
                })
            }
            (ExprMode::Expr(_), PropVariant::Predicate(_)) => Err(UnifierError::Unimplemented(
                smart_format!("predicate in non-matching expression"),
            )),
            (ExprMode::Match { cond_var, .. }, PropVariant::Singleton(Attribute { rel, val })) => {
                let (rel_term, rel_meta, rel_nodes) = self.write_cond_term(*rel, mode)?;
                let (val_term, val_meta, val_nodes) = self.write_cond_term(*val, mode)?;

                if flags.rel_optional() {
                    let catch_label = ontol_hir::Label(self.var_allocator.alloc().0);
                    let catch_block = self.prealloc_node();
                    let mut catch_body = smallvec![];

                    let rel = self.try_let_redef_cond_term(
                        rel_term,
                        rel_meta,
                        catch_label,
                        &mut catch_body,
                    );
                    let val = self.try_let_redef_cond_term(
                        val_term,
                        val_meta,
                        catch_label,
                        &mut catch_body,
                    );

                    catch_body.push(self.mk_node(
                        ontol_hir::Kind::PushCondClause(
                            cond_var,
                            Clause::Attr(struct_var, prop_id, (rel, val)),
                        ),
                        UNIT_META,
                    ));

                    catch_body.extend(rel_nodes);
                    catch_body.extend(val_nodes);

                    Ok(smallvec![self.write_node(
                        catch_block,
                        ontol_hir::Kind::Catch(catch_label, catch_body),
                        UNIT_META
                    )])
                } else {
                    let mut body = smallvec![self.mk_node(
                        ontol_hir::Kind::PushCondClause(
                            cond_var,
                            Clause::Attr(struct_var, prop_id, (rel_term, val_term)),
                        ),
                        UNIT_META,
                    )];
                    body.extend(rel_nodes);
                    body.extend(val_nodes);

                    Ok(body)
                }
            }
            (ExprMode::Match { .. }, PropVariant::Set(_)) => {
                Err(UnifierError::TODO(smart_format!("set-prop in condition")))
            }
            (ExprMode::Match { .. }, PropVariant::Predicate(_)) => {
                todo!("match predicate prop variant")
            }
        }
    }

    fn write_set_prop(
        &mut self,
        (flags, struct_var, prop_id): (PropFlags, Var, PropertyId),
        set_variant: &ontol_hir::SetPropertyVariant<'m, TypedHir>,
        meta: Meta<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let seq_meta = {
            let seq_ty = set_variant.label.meta().ty;

            match seq_ty {
                Type::Error | Type::Seq(..) => {}
                other => {
                    panic!("seq type was {:?}", other);
                }
            }
            Meta::new(seq_ty, meta.span)
        };

        let make_seq = self.write_make_seq(
            set_variant.elements.iter().map(|(iter, attribute)| {
                (
                    if iter.0 {
                        Some(*set_variant.label.hir())
                    } else {
                        None
                    },
                    *attribute,
                )
            }),
            seq_meta,
            mode,
        )?;

        let variant = PropVariant::Singleton(Attribute {
            rel: self.mk_node(ontol_hir::Kind::Unit, UNIT_META),
            val: make_seq,
        });

        Ok(smallvec![self.mk_node(
            ontol_hir::Kind::Prop(flags, struct_var, prop_id, [variant].into()),
            seq_meta,
        )])
    }

    fn write_make_seq(
        &mut self,
        entries: impl Iterator<Item = (Option<ontol_hir::Label>, Attribute<ontol_hir::Node>)>,
        seq_meta: Meta<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Node> {
        let make_seq = self.prealloc_node();
        let out_seq_var = self.var_allocator.alloc();

        let mut seq_body: ontol_hir::Nodes = smallvec![];

        for (label, Attribute { rel, val }) in entries {
            debug!("in_scope: {:?}", self.scope_tracker.in_scope);

            if let Some(label) = label {
                let Some(scope_attr) = self.iter_extended_scope_table.get(&label).cloned() else {
                    // panic!("set prop: no iteration source");
                    return Ok(self.mk_node(ontol_hir::Kind::Unit, seq_meta));
                };
                let free_vars = scan_immediate_free_vars(self.expr_arena, [rel, val]);

                let for_each = self.prealloc_node();
                let ((rel_binding, val_binding), for_each_body) = self.new_loop_scope(
                    // free_vars.clone(),
                    seq_meta.span,
                    move |zelf, scope_body, catcher| {
                        let rel_binding = zelf.define_scope_extended(
                            scope_attr.rel,
                            Scoped::Yes,
                            &free_vars,
                            scope_body,
                            catcher,
                        )?;
                        let val_binding = zelf.define_scope_extended(
                            scope_attr.val,
                            Scoped::Yes,
                            &free_vars,
                            scope_body,
                            catcher,
                        )?;

                        debug!("in scope in loop: {:?}", zelf.scope_tracker.in_scope);

                        let rel = zelf.write_one_expr(rel, mode)?;
                        let val = zelf.write_one_expr(val, mode)?;

                        let body = smallvec![zelf.mk_node(
                            ontol_hir::Kind::Insert(out_seq_var, Attribute { rel, val }),
                            UNIT_META,
                        )];

                        Ok(((rel_binding, val_binding), body))
                    },
                )?;
                seq_body.push_node(self.write_node(
                    for_each,
                    ontol_hir::Kind::ForEach(
                        label.into(),
                        (rel_binding, val_binding),
                        for_each_body,
                    ),
                    UNIT_META,
                ));
            } else {
                let rel = self.write_one_expr(rel, mode)?;
                let val = self.write_one_expr(val, mode)?;
                seq_body.push_node(self.mk_node(
                    ontol_hir::Kind::Insert(out_seq_var, Attribute { rel, val }),
                    UNIT_META,
                ));
            }
        }

        Ok(self.write_node(
            make_seq,
            ontol_hir::Kind::MakeSeq(TypedHirData(out_seq_var.into(), seq_meta), seq_body),
            seq_meta,
        ))
    }

    fn write_match_struct_expr(
        &mut self,
        binder: TypedHirData<'m, ontol_hir::Binder>,
        input_body: &ontol_hir::Nodes,
        node_ref: TypedNodeRef<'_, 'm>,
        type_mapping: TypeMapping<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let struct_node = self.prealloc_node();
        let match_body = self.write_match_struct_body(binder, input_body, type_mapping, mode)?;

        self.write_node(
            struct_node,
            ontol_hir::Kind::Struct(binder, StructFlags::MATCH, match_body),
            Meta::new(type_mapping.from, node_ref.meta().span),
        );

        self.write_type_map_if_necessary(struct_node, type_mapping)
    }

    fn write_match_struct_body(
        &mut self,
        binder: TypedHirData<'m, ontol_hir::Binder>,
        input_body: &ontol_hir::Nodes,
        type_mapping: TypeMapping<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let mut output_body: ontol_hir::Nodes = Default::default();

        let ExprMode::Match {
            cond_var,
            struct_level,
        } = mode
        else {
            panic!("ExprMode::Match expected");
        };

        let def_ty = match type_mapping.from {
            Type::Seq(_, val_ty) => val_ty,
            other => other,
        };

        if struct_level == 0 {
            if !matches!(type_mapping.from, Type::Error) && !matches!(def_ty, Type::Error) {
                let def_id = def_ty
                    .get_single_def_id()
                    .ok_or(UnifierError::NonEntityQuery)?;
                let _ = self
                    .relations
                    .identified_by(def_id)
                    .ok_or(UnifierError::NonEntityQuery)?;
            }

            output_body.push(self.mk_node(
                ontol_hir::Kind::PushCondClause(cond_var, Clause::Root(cond_var)),
                UNIT_META,
            ));
        }

        if let Some(type_def_id) = def_ty.get_single_def_id() {
            if let Some(properties) = self.relations.properties_by_def_id(type_def_id) {
                if properties.identified_by.is_some() {
                    output_body.push(self.mk_node(
                        ontol_hir::Kind::PushCondClause(
                            cond_var,
                            Clause::IsEntity(EvalCondTerm::QuoteVar(binder.0.var), type_def_id),
                        ),
                        UNIT_META,
                    ));
                }
            }
        }

        for node in input_body {
            output_body.extend(self.write_expr(*node, None, mode)?);
        }

        Ok(output_body)
    }

    fn write_cond_term(
        &mut self,
        expr_node: ontol_hir::Node,
        mode: ExprMode,
    ) -> UnifierResult<(EvalCondTerm, Meta<'m>, ontol_hir::Nodes)> {
        let node_ref = self.expr_arena.node_ref(expr_node);

        use ontol_hir::Kind;
        match node_ref.kind() {
            Kind::Unit => Ok((EvalCondTerm::Wildcard, UNIT_META, smallvec![])),
            Kind::Struct(binder, _, input_body) => {
                let body = self.write_match_struct_body(
                    *binder,
                    input_body,
                    TypeMapping {
                        to: node_ref.ty(),
                        from: binder.ty(),
                    },
                    mode.match_struct(binder.hir().var),
                )?;

                Ok((
                    EvalCondTerm::QuoteVar(binder.hir().var),
                    *binder.meta(),
                    body,
                ))
            }
            _ => {
                let node = self.write_one_expr(expr_node, mode)?;
                Ok((
                    EvalCondTerm::Eval(node),
                    *self.out_arena.node_ref(node).meta(),
                    smallvec![],
                ))
            }
        }
    }

    fn try_let_redef_cond_term(
        &mut self,
        input: EvalCondTerm,
        meta: Meta<'m>,
        catch_label: ontol_hir::Label,
        catch_body: &mut ontol_hir::Nodes,
    ) -> EvalCondTerm {
        match input {
            EvalCondTerm::Wildcard => EvalCondTerm::Wildcard,
            EvalCondTerm::QuoteVar(var) => {
                let redef_var = self.var_allocator.alloc();
                let source_node = self.mk_node(ontol_hir::Kind::Var(var), meta);
                catch_body.push(self.mk_node(
                    ontol_hir::Kind::TryLet(
                        catch_label,
                        TypedHirData(redef_var.into(), meta),
                        source_node,
                    ),
                    UNIT_META,
                ));

                EvalCondTerm::QuoteVar(redef_var)
            }
            EvalCondTerm::Eval(node) => {
                let redef_var = self.var_allocator.alloc();
                catch_body.push(self.mk_node(
                    ontol_hir::Kind::TryLet(
                        catch_label,
                        TypedHirData(redef_var.into(), meta),
                        node,
                    ),
                    UNIT_META,
                ));
                EvalCondTerm::Eval(self.mk_node(ontol_hir::Kind::Var(redef_var), meta))
            }
        }
    }

    fn write_type_map_if_necessary(
        &mut self,
        input: ontol_hir::Node,
        type_mapping: TypeMapping<'m>,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let span = self.out_arena.node_ref(input).span();
        match (type_mapping.from, type_mapping.to) {
            (Type::Seq(rel0, val0), Type::Seq(rel1, val1)) if rel0 == rel1 && val0 == val1 => {
                Ok(smallvec![input])
            }
            (Type::Seq(rel_from, val_from), Type::Seq(rel_to, val_to)) => {
                let inner_set_var = self.var_allocator.alloc();
                let mut nodes = smallvec![self.mk_node(
                    ontol_hir::Kind::Let(
                        TypedHirData(inner_set_var.into(), *self.out_arena.node_ref(input).meta()),
                        input
                    ),
                    UNIT_META
                )];

                let seq_map = {
                    let rel_var = self.var_allocator.alloc();
                    let val_var = self.var_allocator.alloc();
                    let mapped_rel = {
                        let input =
                            self.mk_node(ontol_hir::Kind::Var(rel_var), Meta::new(rel_from, span));
                        self.write_type_map_if_necessary(
                            input,
                            TypeMapping {
                                from: rel_from,
                                to: rel_to,
                            },
                        )?
                    };
                    let mapped_val = {
                        let input =
                            self.mk_node(ontol_hir::Kind::Var(val_var), Meta::new(val_from, span));
                        self.write_type_map_if_necessary(
                            input,
                            TypeMapping {
                                from: val_from,
                                to: val_to,
                            },
                        )?
                    };

                    let target_seq_var = self.var_allocator.alloc();
                    let push = {
                        self.mk_node(
                            ontol_hir::Kind::Insert(
                                target_seq_var,
                                ontol_hir::Attribute {
                                    rel: *mapped_rel.last().unwrap(),
                                    val: *mapped_val.last().unwrap(),
                                },
                            ),
                            Meta::new(&UNIT_TYPE, span),
                        )
                    };

                    let for_each = self.mk_node(
                        ontol_hir::Kind::ForEach(
                            inner_set_var,
                            (
                                ontol_hir::Binding::Binder(TypedHirData(
                                    rel_var.into(),
                                    Meta::new(rel_from, span),
                                )),
                                ontol_hir::Binding::Binder(TypedHirData(
                                    val_var.into(),
                                    Meta::new(rel_from, span),
                                )),
                            ),
                            [push].into_iter().collect(),
                        ),
                        Meta::new(&UNIT_TYPE, span),
                    );
                    let copy_sub_seq = self.mk_node(
                        ontol_hir::Kind::CopySubSeq(target_seq_var, inner_set_var),
                        Meta::new(&UNIT_TYPE, span),
                    );

                    self.mk_node(
                        ontol_hir::Kind::MakeSeq(
                            TypedHirData(target_seq_var.into(), Meta::new(type_mapping.from, span)),
                            smallvec![for_each, copy_sub_seq],
                        ),
                        Meta::new(type_mapping.to, span),
                    )
                };

                nodes.push(seq_map);

                Ok(nodes)
            }
            (inner, outer)
                if inner != outer
                    && !matches!(inner, Type::Error)
                    && !matches!(outer, Type::Error) =>
            {
                Ok(smallvec![self.mk_node(
                    ontol_hir::Kind::Map(input),
                    Meta::new(outer, NO_SPAN)
                )])
            }
            _ => Ok(smallvec![input]),
        }
    }

    pub(super) fn write_default_node(&mut self, meta: Meta<'m>) -> ontol_hir::Node {
        let Some(def_id) = meta.ty.get_single_def_id() else {
            return self.mk_node(ontol_hir::Kind::Unit, UNIT_META);
        };

        match self.seal_ctx.get_repr_kind(&def_id) {
            Some(ReprKind::Struct | ReprKind::StructIntersection(_)) => {
                let var = self.var_allocator.alloc();
                self.mk_node(
                    ontol_hir::Kind::Struct(
                        TypedHirData(ontol_hir::Binder { var }, meta),
                        StructFlags::default(),
                        smallvec![],
                    ),
                    meta,
                )
            }
            Some(ReprKind::Unit) => {
                self.mk_node(ontol_hir::Kind::Unit, Meta::new(&UNIT_TYPE, meta.span))
            }
            other => todo!("{other:?}"),
        }
    }

    #[inline]
    pub(super) fn mk_node(
        &mut self,
        kind: ontol_hir::Kind<'m, TypedHir>,
        meta: Meta<'m>,
    ) -> ontol_hir::Node {
        self.out_arena.add(TypedHirData(kind, meta))
    }

    #[inline]
    pub(super) fn prealloc_node(&mut self) -> ontol_hir::Node {
        self.out_arena
            .add(TypedHirData(ontol_hir::Kind::NoOp, UNIT_META))
    }

    #[inline]
    pub(super) fn write_node(
        &mut self,
        slot: ontol_hir::Node,
        kind: ontol_hir::Kind<'m, TypedHir>,
        meta: Meta<'m>,
    ) -> ontol_hir::Node {
        *self.out_arena.data_mut(slot) = TypedHirData(kind, meta);
        slot
    }
}

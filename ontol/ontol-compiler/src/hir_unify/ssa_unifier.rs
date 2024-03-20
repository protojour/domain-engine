use fnv::FnvHashMap;
use ontol_hir::{
    arena::NodeRef, find_value_node, Binder, Binding, EvalCondTerm, Kind, Node, Nodes, PropFlags,
    PropVariant, SetEntry, StructFlags,
};
use ontol_runtime::{
    property::PropertyId,
    query::condition::{Clause, ClausePair, SetOperator},
    smart_format,
    value::Attribute,
    var::{Var, VarAllocator, VarSet},
    MapFlags,
};
use smallvec::{smallvec, SmallVec};
use thin_vec::thin_vec;
use tracing::debug;

use crate::{
    def::{DefKind, Defs},
    hir_unify::{
        regex_interpolation::RegexStringInterpolator,
        ssa_util::{scan_immediate_free_vars, NodesExt},
    },
    mem::Intern,
    primitive::Primitives,
    relation::Relations,
    repr::{
        repr_ctx::ReprCtx,
        repr_model::{ReprKind, ReprScalarKind},
    },
    typed_hir::{arena_import, Meta, TypedHir, TypedHirData, TypedNodeRef},
    types::{Type, Types, UNIT_TYPE},
    CompileError, CompileErrors, Compiler, SourceSpan, NO_SPAN,
};

use super::{
    regex_interpolation::StringInterpolationComponent,
    ssa_util::{
        scan_all_vars_and_labels, ExprMode, ExtendedScope, ScopeTracker, Scoped, TypeMapping,
    },
    UnifiedNode, UnifierError, UnifierResult,
};

/// Unifier that strives to produce Static Single-Assignment form flat hir blocks
pub struct SsaUnifier<'c, 'm> {
    #[allow(unused)]
    pub(super) types: &'c mut Types<'m>,
    #[allow(unused)]
    pub(super) relations: &'c Relations,
    pub(super) repr_ctx: &'c ReprCtx,
    pub(super) defs: &'c Defs<'m>,
    pub(super) errors: &'c mut CompileErrors,
    pub(super) primitives: &'c Primitives,
    pub(super) var_allocator: VarAllocator,
    pub(super) scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) out_arena: ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) map_flags: MapFlags,

    pub(super) all_scope_vars_and_labels: VarSet,
    pub(super) iter_extended_scope_table:
        FnvHashMap<ontol_hir::Label, Attribute<ExtendedScope<'m>>>,
    pub(super) scope_tracker: ScopeTracker<'m>,
}

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub fn new(
        scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        var_allocator: VarAllocator,
        map_flags: MapFlags,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            types: &mut compiler.types,
            relations: &compiler.relations,
            repr_ctx: &compiler.repr_ctx,
            defs: &compiler.defs,
            primitives: &compiler.primitives,
            errors: &mut compiler.errors,
            var_allocator,
            scope_arena,
            expr_arena,
            out_arena: Default::default(),
            map_flags,
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
        let initial_error_count = self.errors.errors.len();
        self.all_scope_vars_and_labels = scan_all_vars_and_labels(self.scope_arena, [scope_node]);

        let result = self.block_unify(scope_node, expr_node, Scoped::Yes)?;

        if self.errors.errors.len() == initial_error_count {
            // This unifier did not report any errors
            Ok(result)
        } else {
            Err(UnifierError::Reported)
        }
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
                ExprMode::Expr {
                    flags: self.map_flags,
                    struct_level: None,
                },
            )?);
        }

        self.write_node(block, Kind::Block(block_body), *expr.meta());

        Ok(UnifiedNode {
            typed_binder: match scope_binding {
                Binding::Wildcard => None,
                Binding::Binder(binder_data) => Some(binder_data),
            },
            node: block,
        })
    }

    /// Wraps the expr in (block) if there are many expressions
    fn write_one_expr(&mut self, expr_node: Node, mode: ExprMode) -> UnifierResult<Node> {
        let nodes = self.write_expr(expr_node, None, mode)?;
        if nodes.len() == 1 {
            Ok(nodes.into_iter().next().unwrap())
        } else {
            let last_meta = *self
                .out_arena
                .node_ref(*nodes.iter().last().unwrap())
                .meta();
            Ok(self.mk_node(Kind::Block(nodes), last_meta))
        }
    }

    fn write_expr(
        &mut self,
        expr_node: Node,
        top_scope_node: Option<NodeRef<'c, 'm, TypedHir>>,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let node_ref = self.expr_arena.node_ref(expr_node);

        // debug!("write expr {node_ref}");

        match node_ref.kind() {
            Kind::Map(argument) => {
                let map_node = self.prealloc_node();
                let arg = self.write_one_expr(*argument, mode)?;

                Ok(smallvec![self.write_node(
                    map_node,
                    ontol_hir::Kind::Map(arg),
                    *node_ref.meta()
                )])
            }
            Kind::Set(entries) => {
                if entries.len() == 1 {
                    let ontol_hir::SetEntry(_, Attribute { rel, val }) = entries.first().unwrap();

                    let rel_kind = self.expr_arena.kind_of(*rel);
                    let val_kind = self.expr_arena.kind_of(*val);

                    // Handle {..struct match()} at the top level, which is a special case:
                    if let (Kind::Unit, Kind::Struct(binder, flags, struct_body)) =
                        (rel_kind, val_kind)
                    {
                        if let ExprMode::MatchStruct { match_level: 0, .. } =
                            mode.match_struct(Var(0))
                        {
                            if flags.contains(StructFlags::MATCH) {
                                let match_var = self.alloc_var();
                                let inner_set_ty =
                                    self.types.intern(Type::Seq(&UNIT_TYPE, binder.meta().ty));

                                debug!("HERE: root set query");
                                return self.write_match_struct_expr(
                                    *binder,
                                    struct_body,
                                    self.expr_arena.node_ref(*val),
                                    TypeMapping {
                                        to: node_ref.ty(),
                                        from: inner_set_ty,
                                    },
                                    mode.match_struct(match_var),
                                );
                            }
                        }
                    }
                }

                Ok(match mode {
                    ExprMode::Expr { .. } => {
                        let make_seq = self.prealloc_node();
                        let out_seq_var = self.alloc_var();
                        let seq_body =
                            self.write_seq_body(entries, *node_ref.meta(), out_seq_var, mode)?;
                        smallvec![self.write_node(
                            make_seq,
                            Kind::MakeSeq(
                                TypedHirData(out_seq_var.into(), *node_ref.meta()),
                                seq_body
                            ),
                            *node_ref.meta(),
                        )]
                    }
                    ExprMode::MatchStruct { .. } => {
                        debug!("TODO");
                        smallvec![self.mk_node(Kind::Unit, Meta::unit(NO_SPAN))]
                    }
                    ExprMode::MatchSet {
                        struct_level: _,
                        match_level: _,
                        match_var,
                        set_cond_var,
                    } => self.write_match_seq_body(
                        entries,
                        *node_ref.meta(),
                        match_var,
                        set_cond_var,
                        mode,
                    )?,
                })
            }
            Kind::Struct(binder, flags, struct_body) => {
                match (
                    flags.contains(StructFlags::MATCH),
                    mode.match_struct(Var(0)),
                ) {
                    (true, ExprMode::MatchStruct { .. }) => {
                        let match_var = self.alloc_var();
                        self.write_match_struct_expr(
                            *binder,
                            struct_body,
                            node_ref,
                            TypeMapping {
                                to: node_ref.ty(),
                                from: binder.ty(),
                            },
                            mode.match_struct(match_var),
                        )
                    }
                    _ => {
                        let next_mode = mode.any_struct();
                        let mut expr_body: ontol_hir::Nodes =
                            SmallVec::with_capacity(struct_body.len());
                        for node in struct_body {
                            expr_body.extend(self.write_expr(*node, None, next_mode)?);
                        }

                        // A hack for moving remaining properties from the source struct into the other struct:
                        match (mode, top_scope_node) {
                            (ExprMode::Expr { flags, .. }, Some(scope_node))
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
            Kind::Prop(optional, var, prop_id, variant) => {
                self.write_prop_expr((*optional, *var, *prop_id), variant, node_ref.meta(), mode)
            }
            Kind::Regex(_seq_label, regex_def_id, capture_group_alternation) => {
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

                let string_binder = self.alloc_var();
                let mut body = ontol_hir::Nodes::default();

                for component in components {
                    let string_push_param = match component {
                        StringInterpolationComponent::Const(string) => self.mk_node(
                            ontol_hir::Kind::Text(string),
                            Meta::unit(node_ref.meta().span),
                        ),
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
                        Meta::unit(node_ref.meta().span),
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
        variant: &PropVariant,
        meta: &Meta<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let relationship_id = prop_id.relationship_id;
        let builtin_rels = &self.primitives.relations;

        let (applied_mode, struct_var) = match mode {
            ExprMode::MatchStruct {
                match_var,
                match_level,
                ..
            } => {
                if relationship_id.0 == builtin_rels.order
                    || relationship_id.0 == builtin_rels.direction
                {
                    if match_level != 0 {
                        self.errors.error(
                            CompileError::TODO("order/direction at incorrect location".into()),
                            &meta.span,
                        );

                        return Err(UnifierError::Reported);
                    }

                    (
                        ExprMode::Expr {
                            flags: Default::default(),
                            struct_level: None,
                        },
                        match_var,
                    )
                } else {
                    (mode, struct_var)
                }
            }
            other => (other, struct_var),
        };

        match (applied_mode, variant) {
            (ExprMode::Expr { .. }, PropVariant::Value(Attribute { rel, val })) => {
                let free_vars = scan_immediate_free_vars(self.expr_arena, [*rel, *val]);
                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    let rel = zelf.write_one_expr(*rel, applied_mode)?;
                    let val = zelf.write_one_expr(*val, applied_mode)?;

                    Ok(smallvec![zelf.mk_node(
                        Kind::Prop(
                            flags,
                            struct_var,
                            prop_id,
                            PropVariant::Value(Attribute { rel, val }),
                        ),
                        *meta,
                    )])
                })
            }
            (ExprMode::Expr { .. }, PropVariant::Predicate(..)) => Err(
                UnifierError::Unimplemented(smart_format!("predicate in non-matching expression")),
            ),
            (
                ExprMode::MatchStruct { match_var, .. },
                PropVariant::Value(Attribute { rel, val }),
            ) => {
                let unit_meta = Meta::new(&UNIT_TYPE, meta.span);
                let mut body = Nodes::default();
                let (rel_term, _rel_meta, rel_nodes) =
                    self.write_cond_term(*rel, match_var, applied_mode, &mut body)?;
                let (val_term, _val_meta, val_nodes) =
                    self.write_cond_term(*val, match_var, applied_mode, &mut body)?;

                let set_var = self.alloc_var();
                let let_cond_var = self.mk_let_cond_var(set_var, match_var, meta.span);

                if flags.rel_optional() {
                    let mut free_vars = scan_immediate_free_vars(self.expr_arena, [*rel, *val]);
                    free_vars.insert(struct_var);
                    free_vars.insert(set_var);

                    body.extend(self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                        let mut body = Nodes::default();

                        body.extend([
                            let_cond_var,
                            zelf.mk_node(
                                Kind::PushCondClauses(
                                    match_var,
                                    thin_vec![
                                        ClausePair(
                                            struct_var,
                                            Clause::MatchProp(
                                                prop_id,
                                                SetOperator::ElementIn,
                                                set_var,
                                            )
                                        ),
                                        ClausePair(set_var, Clause::Member(rel_term, val_term))
                                    ],
                                ),
                                unit_meta,
                            ),
                        ]);

                        body.extend(rel_nodes.clone());
                        body.extend(val_nodes.clone());

                        Ok(body)
                    })?);
                } else {
                    body.extend([
                        let_cond_var,
                        self.mk_node(
                            Kind::PushCondClauses(
                                match_var,
                                thin_vec![
                                    ClausePair(
                                        struct_var,
                                        Clause::MatchProp(prop_id, SetOperator::ElementIn, set_var,)
                                    ),
                                    ClausePair(set_var, Clause::Member(rel_term, val_term))
                                ],
                            ),
                            unit_meta,
                        ),
                    ]);

                    body.extend(rel_nodes);
                    body.extend(val_nodes);
                }

                Ok(body)
            }
            (ExprMode::MatchStruct { match_var, .. }, PropVariant::Predicate(operator, node)) => {
                let set_cond_var = self.alloc_var();
                let let_cond_var = self.mk_let_cond_var(set_cond_var, match_var, meta.span);
                let free_vars = scan_immediate_free_vars(self.expr_arena, [*node]);

                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    let mut body = smallvec![let_cond_var];
                    body.push(zelf.mk_node(
                        Kind::PushCondClauses(
                            match_var,
                            thin_vec![ClausePair(
                                struct_var,
                                Clause::MatchProp(prop_id, *operator, set_cond_var)
                            )],
                        ),
                        Meta::new(&UNIT_TYPE, meta.span),
                    ));

                    body.extend(zelf.write_expr(
                        *node,
                        None,
                        applied_mode.match_set(set_cond_var),
                    )?);
                    Ok(body)
                })
            }
            _ => todo!(),
        }
    }

    fn write_seq_body(
        &mut self,
        entries: &[SetEntry<'m, TypedHir>],
        seq_meta: Meta<'m>,
        out_seq_var: Var,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let mut seq_body: Nodes = smallvec![];

        for SetEntry(label, Attribute { rel, val }) in entries {
            debug!("in_scope: {:?}", self.scope_tracker.in_scope);

            if let Some(label) = label {
                let label = *label.hir();
                let Some(scope_attr) = self.iter_extended_scope_table.get(&label).cloned() else {
                    self.errors.error(
                        CompileError::TODO(smart_format!("no iteration source")),
                        &seq_meta.span,
                    );
                    return Ok(smallvec![]);
                };
                let free_vars = scan_immediate_free_vars(self.expr_arena, [*rel, *val]);

                let for_each = self.prealloc_node();
                let ((rel_binding, val_binding), for_each_body) =
                    self.new_loop_scope(seq_meta.span, move |zelf, scope_body, catcher| {
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

                        let rel = zelf.write_one_expr(*rel, mode)?;
                        let val = zelf.write_one_expr(*val, mode)?;

                        let body = smallvec![zelf.mk_node(
                            Kind::Insert(out_seq_var, Attribute { rel, val }),
                            Meta::new(&UNIT_TYPE, seq_meta.span),
                        )];

                        Ok(((rel_binding, val_binding), body))
                    })?;
                seq_body.push_node(self.write_node(
                    for_each,
                    Kind::ForEach(label.into(), (rel_binding, val_binding), for_each_body),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));
            } else {
                let rel = self.write_one_expr(*rel, mode)?;
                let val = self.write_one_expr(*val, mode)?;
                seq_body.push_node(self.mk_node(
                    Kind::Insert(out_seq_var, Attribute { rel, val }),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));
            }
        }

        Ok(seq_body)
    }

    fn write_match_seq_body(
        &mut self,
        entries: &[SetEntry<'m, TypedHir>],
        seq_meta: Meta<'m>,
        match_var: Var,
        set_cond_var: Var,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let mut seq_body: Nodes = smallvec![];

        for SetEntry(label, Attribute { rel, val }) in entries {
            debug!("in_scope: {:?}", self.scope_tracker.in_scope);

            if let Some(label) = label {
                let label = *label.hir();
                let Some(scope_attr) = self.iter_extended_scope_table.get(&label).cloned() else {
                    // panic!("set prop: no iteration source");
                    return Ok(smallvec![]);
                };
                let free_vars = scan_immediate_free_vars(self.expr_arena, [*rel, *val]);

                let for_each = self.prealloc_node();
                let ((rel_binding, val_binding), for_each_body) =
                    self.new_loop_scope(seq_meta.span, move |zelf, scope_body, catcher| {
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

                        let mut body = smallvec![];

                        let (rel_term, _rel_meta, rel_nodes) =
                            zelf.write_cond_term(*rel, match_var, mode, &mut body)?;
                        let (val_term, _val_meta, val_nodes) =
                            zelf.write_cond_term(*val, match_var, mode, &mut body)?;

                        debug!("in match scope in loop: {:?}", zelf.scope_tracker.in_scope);

                        body.push(zelf.mk_node(
                            Kind::PushCondClauses(
                                match_var,
                                thin_vec![ClausePair(
                                    set_cond_var,
                                    Clause::Member(rel_term, val_term)
                                ),],
                            ),
                            Meta::new(&UNIT_TYPE, seq_meta.span),
                        ));

                        body.extend(rel_nodes);
                        body.extend(val_nodes);

                        Ok(((rel_binding, val_binding), body))
                    })?;
                seq_body.push_node(self.write_node(
                    for_each,
                    Kind::ForEach(label.into(), (rel_binding, val_binding), for_each_body),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));
            } else {
                let (rel_term, _rel_meta, rel_nodes) =
                    self.write_cond_term(*rel, match_var, mode, &mut seq_body)?;
                let (val_term, _val_meta, val_nodes) =
                    self.write_cond_term(*val, match_var, mode, &mut seq_body)?;

                seq_body.push(self.mk_node(
                    Kind::PushCondClauses(
                        match_var,
                        thin_vec![ClausePair(set_cond_var, Clause::Member(rel_term, val_term)),],
                    ),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));

                seq_body.extend(rel_nodes);
                seq_body.extend(val_nodes);
            }
        }

        Ok(seq_body)
    }

    fn write_match_struct_expr(
        &mut self,
        binder: TypedHirData<'m, Binder>,
        input_body: &Nodes,
        node_ref: TypedNodeRef<'_, 'm>,
        type_mapping: TypeMapping<'m>,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let ExprMode::MatchStruct { match_var, .. } = mode else {
            panic!();
        };

        let struct_node = self.prealloc_node();
        let match_body = self.write_match_struct_body(binder, input_body, type_mapping, mode)?;

        self.write_node(
            struct_node,
            Kind::Struct(
                TypedHirData(Binder { var: match_var }, *binder.meta()),
                StructFlags::MATCH,
                match_body,
            ),
            Meta::new(type_mapping.from, node_ref.meta().span),
        );

        self.write_type_map_if_necessary(struct_node, type_mapping)
    }

    fn write_match_struct_body(
        &mut self,
        binder: TypedHirData<'m, Binder>,
        input_body: &Nodes,
        type_mapping: TypeMapping<'m>,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let mut output_body: Nodes = Default::default();

        let ExprMode::MatchStruct {
            match_var,
            match_level,
            ..
        } = mode
        else {
            panic!("ExprMode::Match expected");
        };

        let def_ty = match type_mapping.from {
            Type::Seq(_, val_ty) => val_ty,
            other => other,
        };

        let mut base_clauses = thin_vec![];

        if match_level == 0 {
            output_body.push(self.mk_let_cond_var(binder.hir().var, match_var, binder.meta().span));

            if !matches!(type_mapping.from, Type::Error) && !matches!(def_ty, Type::Error) {
                let def_id = def_ty
                    .get_single_def_id()
                    .ok_or(UnifierError::NonEntityQuery)?;
                let _ = self
                    .relations
                    .identified_by(def_id)
                    .ok_or(UnifierError::NonEntityQuery)?;
            }

            base_clauses.push(ClausePair(binder.hir().var, Clause::Root));
        }

        if let Some(type_def_id) = def_ty.get_single_def_id() {
            if let Some(properties) = self.relations.properties_by_def_id(type_def_id) {
                if properties.identified_by.is_some() {
                    base_clauses.push(ClausePair(binder.hir().var, Clause::IsEntity(type_def_id)));
                }
            }
        }

        if !base_clauses.is_empty() {
            output_body.push(self.mk_node(
                Kind::PushCondClauses(match_var, base_clauses),
                Meta::new(&UNIT_TYPE, binder.meta().span),
            ));
        }

        for node in input_body {
            output_body.extend(self.write_expr(*node, None, mode)?);
        }

        Ok(output_body)
    }

    fn write_cond_term(
        &mut self,
        expr_node: Node,
        match_var: Var,
        mode: ExprMode,
        output: &mut Nodes,
    ) -> UnifierResult<(EvalCondTerm, Meta<'m>, Nodes)> {
        let node_ref = self.expr_arena.node_ref(expr_node);

        match node_ref.kind() {
            Kind::Unit => Ok((
                EvalCondTerm::Wildcard,
                Meta::new(&UNIT_TYPE, node_ref.meta().span),
                smallvec![],
            )),
            Kind::Struct(binder, _, input_body) => {
                output.push(self.mk_let_cond_var(
                    binder.hir().var,
                    match_var,
                    node_ref.meta().span,
                ));

                let inner_body = self.write_match_struct_body(
                    *binder,
                    input_body,
                    TypeMapping {
                        to: node_ref.ty(),
                        from: binder.ty(),
                    },
                    mode.match_struct(match_var),
                )?;

                Ok((
                    EvalCondTerm::QuoteVar(binder.hir().var),
                    *binder.meta(),
                    inner_body,
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

    fn write_type_map_if_necessary(
        &mut self,
        input: Node,
        type_mapping: TypeMapping<'m>,
    ) -> UnifierResult<Nodes> {
        let span = self.out_arena.node_ref(input).span();
        match (type_mapping.from, type_mapping.to) {
            (Type::Seq(rel0, val0), Type::Seq(rel1, val1)) if rel0 == rel1 && val0 == val1 => {
                Ok(smallvec![input])
            }
            (Type::Seq(rel_from, val_from), Type::Seq(rel_to, val_to)) => {
                let inner_set_var = self.alloc_var();
                let mut nodes = smallvec![self.mk_node(
                    Kind::Let(
                        TypedHirData(inner_set_var.into(), *self.out_arena.node_ref(input).meta()),
                        input
                    ),
                    Meta::unit(span),
                )];

                let seq_map = {
                    let rel_var = self.alloc_var();
                    let val_var = self.alloc_var();
                    let mapped_rel = {
                        let input = self.mk_node(Kind::Var(rel_var), Meta::new(rel_from, span));
                        self.write_type_map_if_necessary(
                            input,
                            TypeMapping {
                                from: rel_from,
                                to: rel_to,
                            },
                        )?
                    };
                    let mapped_val = {
                        let input = self.mk_node(Kind::Var(val_var), Meta::new(val_from, span));
                        self.write_type_map_if_necessary(
                            input,
                            TypeMapping {
                                from: val_from,
                                to: val_to,
                            },
                        )?
                    };

                    let target_seq_var = self.alloc_var();
                    let insert = {
                        self.mk_node(
                            Kind::Insert(
                                target_seq_var,
                                Attribute {
                                    rel: *mapped_rel.last().unwrap(),
                                    val: *mapped_val.last().unwrap(),
                                },
                            ),
                            Meta::new(&UNIT_TYPE, span),
                        )
                    };

                    let for_each = self.mk_node(
                        Kind::ForEach(
                            inner_set_var,
                            (
                                Binding::Binder(TypedHirData(
                                    rel_var.into(),
                                    Meta::new(rel_from, span),
                                )),
                                Binding::Binder(TypedHirData(
                                    val_var.into(),
                                    Meta::new(rel_from, span),
                                )),
                            ),
                            [insert].into_iter().collect(),
                        ),
                        Meta::new(&UNIT_TYPE, span),
                    );
                    let copy_sub_seq = self.mk_node(
                        Kind::CopySubSeq(target_seq_var, inner_set_var),
                        Meta::new(&UNIT_TYPE, span),
                    );

                    self.mk_node(
                        Kind::MakeSeq(
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
                Ok(smallvec![
                    self.mk_node(Kind::Map(input), Meta::new(outer, span))
                ])
            }
            _ => Ok(smallvec![input]),
        }
    }

    pub(super) fn write_default_node(&mut self, meta: Meta<'m>) -> Node {
        let Some(def_id) = meta.ty.get_single_def_id() else {
            return match meta.ty {
                Type::Seq(..) => {
                    let var = self.alloc_var();
                    self.mk_node(
                        Kind::MakeSeq(TypedHirData(Binder { var }, meta), smallvec![]),
                        meta,
                    )
                }
                _ => self.mk_node(Kind::Unit, Meta::unit(meta.span)),
            };
        };

        match self.repr_ctx.get_repr_kind(&def_id) {
            Some(ReprKind::Struct | ReprKind::StructIntersection(_)) => {
                let var = self.alloc_var();
                self.mk_node(
                    Kind::Struct(
                        TypedHirData(Binder { var }, meta),
                        StructFlags::default(),
                        smallvec![],
                    ),
                    meta,
                )
            }
            Some(ReprKind::Unit) => self.mk_node(Kind::Unit, Meta::new(&UNIT_TYPE, meta.span)),
            Some(ReprKind::Scalar(scalar_def_id, ReprScalarKind::Text, _)) => {
                match self.defs.def_kind(*scalar_def_id) {
                    DefKind::TextLiteral(str) => self.mk_node(Kind::Text((*str).into()), meta),
                    _ => {
                        self.errors.error(
                            CompileError::TODO(smart_format!(
                                "cannot create a constant out of any `text`"
                            )),
                            &meta.span,
                        );
                        self.mk_node(Kind::Unit, meta)
                    }
                }
            }
            _ => {
                self.errors.error(
                    CompileError::TODO(smart_format!("create constant")),
                    &meta.span,
                );
                self.mk_node(Kind::Unit, meta)
            }
        }
    }

    pub(super) fn alloc_var(&mut self) -> Var {
        self.var_allocator.alloc()
    }

    #[inline]
    pub(super) fn mk_node(&mut self, kind: Kind<'m, TypedHir>, meta: Meta<'m>) -> Node {
        self.out_arena.add(TypedHirData(kind, meta))
    }

    pub(super) fn mk_let_cond_var(
        &mut self,
        cond_var: Var,
        match_var: Var,
        span: SourceSpan,
    ) -> Node {
        // debug!("mk let cond var {cond_var:?}");
        self.scope_tracker.in_scope.insert(cond_var);
        self.mk_node(
            Kind::LetCondVar(cond_var, match_var),
            Meta::new(&UNIT_TYPE, span),
        )
    }

    #[inline]
    pub(super) fn prealloc_node(&mut self) -> Node {
        self.out_arena
            .add(TypedHirData(Kind::NoOp, Meta::unit(NO_SPAN)))
    }

    #[inline]
    pub(super) fn write_node(
        &mut self,
        slot: Node,
        kind: Kind<'m, TypedHir>,
        meta: Meta<'m>,
    ) -> Node {
        *self.out_arena.data_mut(slot) = TypedHirData(kind, meta);
        slot
    }
}

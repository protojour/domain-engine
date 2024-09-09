use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_hir::{
    arena::NodeRef, find_value_node, import::arena_import, Binder, Binding, EvalCondTerm, Kind,
    Label, MatrixRow, Node, Nodes, PropFlags, PropVariant, StructFlags,
};
use ontol_runtime::{
    query::condition::{Clause, ClausePair, SetOperator},
    var::{Var, VarAllocator, VarSet},
    MapDirection, MapFlags, OntolDefTag, PropId,
};
use smallvec::{smallvec, SmallVec};
use thin_vec::thin_vec;
use tracing::{debug, info, trace};

use crate::{
    def::{DefKind, Defs},
    hir_unify::{regex_interpolation::RegexStringInterpolator, ssa_util::NodesExt},
    mem::Intern,
    properties::PropCtx,
    relation::RelCtx,
    repr::{
        repr_ctx::ReprCtx,
        repr_model::{ReprKind, ReprScalarKind},
    },
    typed_hir::{Meta, TypedHir, TypedHirData, TypedNodeRef},
    types::{Type, TypeCtx, TypeRef, UNIT_TYPE},
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
    pub(super) types: &'c mut TypeCtx<'m>,
    #[expect(unused)]
    pub(super) rel_ctx: &'c RelCtx,
    pub(super) prop_ctx: &'c PropCtx,
    pub(super) repr_ctx: &'c ReprCtx,
    pub(super) defs: &'c Defs<'m>,
    pub(super) errors: &'c mut CompileErrors,
    pub(super) var_allocator: VarAllocator,
    pub(super) scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) out_arena: ontol_hir::arena::Arena<'m, TypedHir>,
    pub(super) direction: MapDirection,
    pub(super) map_flags: MapFlags,
    pub(super) root_try_label: Option<Label>,

    pub(super) all_scope_vars_and_labels: VarSet,
    pub(super) iter_extended_scope_table: FnvHashMap<ontol_hir::Label, Vec<ExtendedScope<'m>>>,
    pub(super) iter_tuple_vars: FnvHashMap<Var, FnvHashMap<u8, Var>>,
    pub(super) scope_tracker: ScopeTracker<'m>,
}

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub fn new(
        scope_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        expr_arena: &'c ontol_hir::arena::Arena<'m, TypedHir>,
        var_allocator: VarAllocator,
        direction: MapDirection,
        map_flags: MapFlags,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            types: &mut compiler.ty_ctx,
            rel_ctx: &compiler.rel_ctx,
            prop_ctx: &compiler.prop_ctx,
            repr_ctx: &compiler.repr_ctx,
            defs: &compiler.defs,
            errors: &mut compiler.errors,
            var_allocator,
            scope_arena,
            expr_arena,
            out_arena: Default::default(),
            direction,
            map_flags,
            root_try_label: None,
            all_scope_vars_and_labels: Default::default(),
            iter_extended_scope_table: Default::default(),
            iter_tuple_vars: Default::default(),
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

        self.write_node(
            block,
            if let Some(root_try_label) = self.root_try_label {
                Kind::CatchFunc(root_try_label, block_body)
            } else {
                Kind::Block(block_body)
            },
            *expr.meta(),
        );

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
                .node_ref(nodes.last().copied().unwrap())
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
            Kind::Matrix(rows) => {
                if rows.is_empty() {
                    return Err(UnifierError::MatrixWithoutRows);
                }

                if rows.len() == 1 && rows.first().unwrap().1.len() == 1 {
                    let element = rows.first().unwrap().1.first().unwrap();

                    let kind = self.expr_arena.kind_of(*element);

                    // Handle {..struct match()} at the top level, which is a special case:
                    if let Kind::Struct(binder, flags, struct_body) = kind {
                        if let ExprMode::MatchStruct { match_level: 0, .. } =
                            mode.match_struct(Var(0))
                        {
                            if flags.contains(StructFlags::MATCH) {
                                let match_var = self.alloc_var();

                                let inner_matrix_ty = {
                                    let column_types = self.types.intern([binder.meta().ty]);
                                    self.types.intern(Type::Matrix(column_types))
                                };

                                debug!("HERE: root set query");
                                return self.write_match_struct_expr(
                                    *binder,
                                    struct_body,
                                    self.expr_arena.node_ref(*element),
                                    TypeMapping {
                                        to: node_ref.ty(),
                                        from: inner_matrix_ty,
                                    },
                                    mode.match_struct(match_var),
                                );
                            }
                        }
                    }
                }

                let arity = rows.iter().map(|row| row.1.len()).max().unwrap();

                Ok(match mode {
                    ExprMode::Expr { .. } => {
                        let make_seq = self.prealloc_node();
                        let out_columns: Vec<Var> =
                            (0..arity).map(|_| self.alloc_var()).collect_vec();
                        let seq_body =
                            self.write_matrix_body(rows, *node_ref.meta(), &out_columns, mode)?;

                        let column_seq_types = (0..arity)
                            .map(|idx| node_ref.meta().ty.matrix_column_type(idx, self.types))
                            .collect_vec();

                        smallvec![self.write_node(
                            make_seq,
                            Kind::MakeMatrix(
                                out_columns
                                    .into_iter()
                                    .enumerate()
                                    .map(|(column_idx, var)| {
                                        let column_type = column_seq_types[column_idx];

                                        TypedHirData(
                                            var.into(),
                                            Meta {
                                                ty: column_type,
                                                span: node_ref.meta().span,
                                            },
                                        )
                                    })
                                    .collect(),
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
                    } => self.write_match_matrix_body(
                        rows,
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
            Kind::Prop(flags, var, prop_id, variant) => {
                self.write_prop_expr((*flags, *var, *prop_id), variant, node_ref.meta(), mode)
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
            Kind::Narrow(expr) => {
                // narrowing has no effect on expressions, just peel it off
                Ok(smallvec![arena_import(
                    &mut self.out_arena,
                    self.expr_arena.node_ref(*expr)
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
        (flags, struct_var, prop_id): (PropFlags, Var, PropId),
        variant: &PropVariant,
        meta: &Meta<'m>,
        mode: ExprMode,
    ) -> UnifierResult<ontol_hir::Nodes> {
        let (applied_mode, struct_var) = match mode {
            ExprMode::MatchStruct {
                match_var,
                match_level,
                ..
            } => {
                if prop_id.0 == OntolDefTag::RelationOrder.def_id()
                    || prop_id.0 == OntolDefTag::RelationDirection.def_id()
                {
                    if match_level != 0 {
                        CompileError::TODO("order/direction at incorrect location")
                            .span(meta.span)
                            .report(self);

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
            (ExprMode::Expr { .. }, PropVariant::Unit(val)) => {
                let free_vars = self.scan_immediate_free_vars(self.expr_arena, &[*val]);
                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    let val = zelf.write_one_expr(*val, applied_mode)?;

                    Ok(smallvec![zelf.mk_node(
                        Kind::Prop(flags, struct_var, prop_id, PropVariant::Unit(val),),
                        *meta,
                    )])
                })
            }
            (ExprMode::Expr { .. }, PropVariant::Tuple(tup)) => {
                let free_vars = self.scan_immediate_free_vars(self.expr_arena, tup);
                self.maybe_apply_catch_block(free_vars, meta.span, &|zelf| {
                    let variant = PropVariant::Tuple(
                        tup.iter()
                            .map(|node| zelf.write_one_expr(*node, applied_mode))
                            .collect::<Result<_, _>>()?,
                    );

                    Ok(smallvec![zelf.mk_node(
                        Kind::Prop(flags, struct_var, prop_id, variant,),
                        *meta,
                    )])
                })
            }
            (ExprMode::Expr { .. }, PropVariant::Predicate(..)) => Err(
                UnifierError::Unimplemented("predicate in non-matching expression".to_string()),
            ),
            (
                ExprMode::MatchStruct { match_var, .. },
                PropVariant::Unit(_) | PropVariant::Tuple(_),
            ) => {
                let unit_meta = Meta::new(&UNIT_TYPE, meta.span);
                let mut body = Nodes::default();

                let (in_nodes, clause, out_nodes) = match variant {
                    PropVariant::Unit(node) => {
                        let (term, _meta, nodes) =
                            self.write_cond_term(*node, match_var, applied_mode, &mut body)?;

                        (
                            vec![*node],
                            Clause::Member(EvalCondTerm::Wildcard, term),
                            nodes,
                        )
                    }
                    PropVariant::Tuple(tup) => {
                        if tup.len() != 2 {
                            panic!("tuple length must be 2 for now")
                        }

                        let (val_term, _val_meta, mut val_nodes) =
                            self.write_cond_term(tup[0], match_var, applied_mode, &mut body)?;
                        let (rel_term, _rel_meta, rel_nodes) =
                            self.write_cond_term(tup[1], match_var, applied_mode, &mut body)?;

                        val_nodes.extend(rel_nodes);

                        (
                            vec![tup[0], tup[1]],
                            Clause::Member(rel_term, val_term),
                            val_nodes,
                        )
                    }
                    PropVariant::Predicate(..) => unreachable!(),
                };

                let set_var = self.alloc_var();
                let let_cond_var = self.mk_let_cond_var(set_var, match_var, meta.span);

                if flags.rel_up_optional() {
                    let mut free_vars = self.scan_immediate_free_vars(self.expr_arena, &in_nodes);
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
                                        ClausePair(set_var, clause.clone())
                                    ],
                                ),
                                unit_meta,
                            ),
                        ]);

                        body.extend(out_nodes.clone());

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
                                    ClausePair(set_var, clause)
                                ],
                            ),
                            unit_meta,
                        ),
                    ]);

                    body.extend(out_nodes);
                }

                Ok(body)
            }
            (ExprMode::MatchStruct { match_var, .. }, PropVariant::Predicate(operator, node)) => {
                let set_cond_var = self.alloc_var();
                let let_cond_var = self.mk_let_cond_var(set_cond_var, match_var, meta.span);
                let free_vars = self.scan_immediate_free_vars(self.expr_arena, &[*node]);

                debug!("prop predicate free vars: {free_vars:?}");

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

    fn write_matrix_body(
        &mut self,
        rows: &[MatrixRow<'m, TypedHir>],
        seq_meta: Meta<'m>,
        out_columns: &[Var],
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let mut matrix_body: Nodes = smallvec![];

        for MatrixRow(label, elements) in rows {
            debug!(
                "matrix arity={}, in_scope: {:?}",
                elements.len(),
                self.scope_tracker.in_scope,
            );

            if let Some(label) = label {
                let label = *label.hir();
                let Some(scope_elements) = self.iter_extended_scope_table.get(&label).cloned()
                else {
                    CompileError::TODO("no iteration source")
                        .span(seq_meta.span)
                        .report(self);
                    return Ok(smallvec![]);
                };
                let free_vars = self.scan_immediate_free_vars(self.expr_arena, elements);

                debug!("matrix free vars: {free_vars:?}");

                let for_each = self.prealloc_node();
                let (bindings, for_each_body) =
                    self.new_loop_scope(seq_meta.span, move |zelf, scope_body, catcher| {
                        let mut bindings = vec![];

                        for scope_element in scope_elements {
                            let binding = zelf.define_scope_extended(
                                scope_element,
                                Scoped::Yes,
                                &free_vars,
                                scope_body,
                                catcher,
                            )?;

                            bindings.push(binding);
                        }

                        debug!("in scope in loop: {:?}", zelf.scope_tracker.in_scope);

                        let insertions: Vec<_> = elements
                            .iter()
                            .map(|element| zelf.write_one_expr(*element, mode))
                            .collect::<Result<_, _>>()?;

                        let body: Nodes = insertions
                            .into_iter()
                            .zip(out_columns)
                            .map(|(insertion, out_column)| {
                                zelf.mk_node(
                                    Kind::Insert(*out_column, insertion),
                                    Meta::new(&UNIT_TYPE, seq_meta.span),
                                )
                            })
                            .collect();

                        Ok((bindings, body))
                    })?;

                let for_each_bindings = bindings
                    .into_iter()
                    .enumerate()
                    .map(|(idx, binding)| {
                        (self.get_or_alloc_iter_tuple_var(Var(label.0), idx), binding)
                    })
                    .collect();

                matrix_body.push_node(self.write_node(
                    for_each,
                    Kind::ForEach(for_each_bindings, for_each_body),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));
            } else {
                let insert_params: Vec<_> = elements
                    .iter()
                    .map(|element| self.write_one_expr(*element, mode))
                    .collect::<Result<_, _>>()?;

                for (insert_param, out_column) in insert_params.iter().zip(out_columns) {
                    matrix_body.push_node(self.mk_node(
                        Kind::Insert(*out_column, *insert_param),
                        Meta::new(&UNIT_TYPE, seq_meta.span),
                    ));
                }
            }
        }

        Ok(matrix_body)
    }

    fn write_match_matrix_body(
        &mut self,
        rows: &[MatrixRow<'m, TypedHir>],
        seq_meta: Meta<'m>,
        match_var: Var,
        set_cond_var: Var,
        mode: ExprMode,
    ) -> UnifierResult<Nodes> {
        let mut seq_body: Nodes = smallvec![];

        for MatrixRow(label, elements) in rows {
            debug!("in_scope: {:?}", self.scope_tracker.in_scope);

            if let Some(label) = label {
                let label = *label.hir();
                let Some(scope_elements) = self.iter_extended_scope_table.get(&label).cloned()
                else {
                    // panic!("set prop: no iteration source");
                    return Ok(smallvec![]);
                };
                let free_vars = self.scan_immediate_free_vars(self.expr_arena, elements);

                debug!("matrix(match) free vars: {free_vars:?}");

                let for_each = self.prealloc_node();
                let (bindings, for_each_body) =
                    self.new_loop_scope(seq_meta.span, move |zelf, scope_body, catcher| {
                        let mut bindings = vec![];

                        for scope_element in scope_elements {
                            let binding = zelf.define_scope_extended(
                                scope_element,
                                Scoped::Yes,
                                &free_vars,
                                scope_body,
                                catcher,
                            )?;

                            bindings.push(binding);
                        }

                        let mut term_tuple = vec![];
                        let mut clause_nodes = vec![];
                        let mut body = smallvec![];

                        for element in elements {
                            let (term, _meta, nodes) =
                                zelf.write_cond_term(*element, match_var, mode, &mut body)?;
                            term_tuple.push(term);
                            clause_nodes.extend(nodes);
                        }

                        debug!("in match scope in loop: {:?}", zelf.scope_tracker.in_scope);

                        body.push(zelf.mk_node(
                            Kind::PushCondClauses(
                                match_var,
                                thin_vec![ClausePair(set_cond_var, mk_member_clause(term_tuple))],
                            ),
                            Meta::new(&UNIT_TYPE, seq_meta.span),
                        ));

                        body.extend(clause_nodes);

                        Ok((bindings, body))
                    })?;

                let for_each_bindings = bindings
                    .into_iter()
                    .enumerate()
                    .map(|(idx, binding)| {
                        (self.get_or_alloc_iter_tuple_var(Var(label.0), idx), binding)
                    })
                    .collect();

                seq_body.push_node(self.write_node(
                    for_each,
                    Kind::ForEach(for_each_bindings, for_each_body),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));
            } else {
                let mut term_tuple = vec![];
                let mut clause_nodes = vec![];

                for element in elements {
                    let (term, _rel_meta, nodes) =
                        self.write_cond_term(*element, match_var, mode, &mut seq_body)?;

                    term_tuple.push(term);
                    clause_nodes.extend(nodes);
                }

                seq_body.push(self.mk_node(
                    Kind::PushCondClauses(
                        match_var,
                        thin_vec![ClausePair(set_cond_var, mk_member_clause(term_tuple))],
                    ),
                    Meta::new(&UNIT_TYPE, seq_meta.span),
                ));

                seq_body.extend(clause_nodes);
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
            Type::Seq(val_ty) => val_ty,
            Type::Matrix(tuple) => tuple[0],
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
                    .prop_ctx
                    .identified_by(def_id)
                    .ok_or(UnifierError::NonEntityQuery)?;
            }

            base_clauses.push(ClausePair(binder.hir().var, Clause::Root));
        }

        if let Some(type_def_id) = def_ty.get_single_def_id() {
            if let Some(properties) = self.prop_ctx.properties_by_def_id(type_def_id) {
                if properties.identified_by.is_some() {
                    base_clauses.push(ClausePair(binder.hir().var, Clause::IsDef(type_def_id)));
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
            (Type::Seq(val0), Type::Seq(val1)) if val0 == val1 => Ok(smallvec![input]),
            (Type::Seq(item_from), Type::Seq(item_to)) => self.type_map_sequence(
                input,
                [type_mapping.from, type_mapping.to],
                [item_from, item_to],
                span,
            ),
            (Type::Seq(item_ty), Type::Matrix(columns))
                if columns.len() == 1 && &columns[0] == item_ty =>
            {
                Ok(smallvec![input])
            }
            (Type::Matrix(col_from), Type::Matrix(col_to)) if col_from.iter().eq(col_to.iter()) => {
                Ok(smallvec![input])
            }
            (Type::Matrix(col_from), Type::Matrix(col_to))
                if col_from.len() == 1 && col_to.len() == 1 =>
            {
                self.type_map_sequence(
                    input,
                    [type_mapping.from, type_mapping.to],
                    [col_from[0], col_to[0]],
                    span,
                )
            }
            (Type::Matrix(_), Type::Matrix(_)) => {
                CompileError::TODO("map between matrices of different arity")
                    .span(span)
                    .report(self.errors);
                Err(UnifierError::Reported)
            }
            (inner, outer)
                if inner != outer
                    && !matches!(inner, Type::Error)
                    && !matches!(outer, Type::Error) =>
            {
                trace!("request mapping from {inner:?} to {outer:?}");
                Ok(smallvec![
                    self.mk_node(Kind::Map(input), Meta::new(outer, span))
                ])
            }
            _ => Ok(smallvec![input]),
        }
    }

    fn type_map_sequence(
        &mut self,
        input: Node,
        [seq_from, seq_to]: [TypeRef<'m>; 2],
        [item_from, item_to]: [TypeRef<'m>; 2],
        span: SourceSpan,
    ) -> UnifierResult<Nodes> {
        let inner_set_var = self.alloc_var();
        let mut nodes = smallvec![self.mk_node(
            Kind::Let(
                TypedHirData(inner_set_var.into(), *self.out_arena.node_ref(input).meta()),
                input
            ),
            Meta::unit(span),
        )];

        let seq_map = {
            let val_var = self.alloc_var();
            let mapped_val = {
                let input = self.mk_node(Kind::Var(val_var), Meta::new(item_from, span));
                self.write_type_map_if_necessary(
                    input,
                    TypeMapping {
                        from: item_from,
                        to: item_to,
                    },
                )?
            };

            let target_seq_var = self.alloc_var();
            let insert = {
                self.mk_node(
                    Kind::Insert(target_seq_var, *mapped_val.last().unwrap()),
                    Meta::new(&UNIT_TYPE, span),
                )
            };

            let for_each = self.mk_node(
                Kind::ForEach(
                    thin_vec![(
                        inner_set_var,
                        Binding::Binder(TypedHirData(val_var.into(), Meta::new(item_from, span),)),
                    )],
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
                    Some(TypedHirData(
                        target_seq_var.into(),
                        Meta::new(seq_from, span),
                    )),
                    smallvec![for_each, copy_sub_seq],
                ),
                Meta::new(seq_to, span),
            )
        };

        nodes.push(seq_map);

        Ok(nodes)
    }

    pub(super) fn write_default_node(&mut self, meta: Meta<'m>) -> Node {
        let Some(def_id) = meta.ty.get_single_def_id() else {
            return match meta.ty {
                Type::Seq(..) => self.write_empty_sequence(meta),
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
            Some(ReprKind::Scalar(scalar_def_id, ReprScalarKind::TextConstant(_), _)) => {
                match self.defs.def_kind(*scalar_def_id) {
                    DefKind::TextLiteral(str) => self.mk_node(Kind::Text((*str).into()), meta),
                    _ => {
                        CompileError::TODO("cannot create a constant out of any `text`")
                            .span(meta.span)
                            .report(self);
                        self.mk_node(Kind::Unit, meta)
                    }
                }
            }
            Some(repr) => {
                info!("ERROR: create constant");
                CompileError::TODO(format!("create constant for {repr:?}"))
                    .span(meta.span)
                    .report(self);
                self.mk_node(Kind::Unit, meta)
            }
            None => {
                CompileError::BUG("no repr available")
                    .span(meta.span)
                    .report(self);
                self.mk_node(Kind::Unit, meta)
            }
        }
    }

    pub(super) fn write_empty_sequence(&mut self, meta: Meta<'m>) -> Node {
        self.mk_node(Kind::MakeSeq(None, smallvec![]), meta)
    }

    pub(super) fn alloc_var(&mut self) -> Var {
        self.var_allocator.alloc()
    }

    pub(super) fn get_or_alloc_iter_tuple_var(
        &mut self,
        iter_label: Var,
        element_idx: usize,
    ) -> Var {
        if element_idx == 0 {
            return iter_label;
        }

        let element_idx: u8 = element_idx.try_into().unwrap();

        let entry = self.iter_tuple_vars.entry(iter_label).or_default();

        let var = entry.entry(element_idx).or_insert_with(|| {
            let var = self.var_allocator.alloc();
            self.all_scope_vars_and_labels.insert(var);
            var
        });

        *var
    }

    pub(super) fn mk_root_try_label(&mut self) -> Label {
        if let Some(root_try_label) = self.root_try_label {
            root_try_label
        } else {
            self.root_try_label = Some(Label(self.var_allocator.alloc().0));
            self.root_try_label.unwrap()
        }
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

impl<'c, 'm> AsMut<CompileErrors> for SsaUnifier<'c, 'm> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self.errors
    }
}

fn mk_member_clause(term_tuple: Vec<EvalCondTerm>) -> Clause<Var, EvalCondTerm> {
    match term_tuple.len() {
        0 => Clause::Member(EvalCondTerm::Wildcard, EvalCondTerm::Wildcard),
        1 => Clause::Member(EvalCondTerm::Wildcard, term_tuple[0]),
        _ => Clause::Member(term_tuple[1], term_tuple[0]),
    }
}

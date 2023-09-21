use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::{HasDefault, PropPattern, PropVariant};
use ontol_runtime::{
    smart_format,
    value::PropertyId,
    vm::proc::{BuiltinProc, GetAttrFlags, Local, NParams, OpCode, Predicate, Procedure},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{
        data_flow_analyzer::DataFlowAnalyzer,
        ir::{BlockIndex, BlockOffset, Terminator},
        proc_builder::Delta,
    },
    error::CompileError,
    primitive::Primitives,
    typed_hir::{HirFunc, TypedArena, TypedHir, TypedNodeRef},
    types::Type,
    CompileErrors, Compiler, SourceSpan, NO_SPAN,
};

use super::{
    ir::Ir,
    proc_builder::{Block, ProcBuilder},
    task::ProcTable,
    type_mapper::TypeMapper,
};

pub(super) fn const_codegen<'m>(
    proc_table: &mut ProcTable,
    expr: ontol_hir::RootNode<'m, TypedHir>,
    def_id: DefId,
    compiler: &Compiler<'m>,
) -> CompileErrors {
    let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs, &compiler.seal_ctx);
    let mut errors = CompileErrors::default();

    debug!("Generating code for\n{}", expr);

    let expr_meta = *expr.data().meta();

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Delta(0), expr_meta.span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors: &mut errors,
        primitives: &compiler.primitives,
        type_mapper,
        bug_span: expr_meta.span,
    };
    generator.gen_node(expr.as_ref(), &mut block);
    block.commit(Terminator::Return(builder.top()), &mut builder);

    proc_table.const_procedures.insert(def_id, builder);
    errors
}

/// The intention for this is to be parallelizable,
/// but that won't work with `&mut ProcTable`.
/// Maybe find a solution for that.
pub(super) fn map_codegen<'m>(
    proc_table: &mut ProcTable,
    func: HirFunc<'m>,
    compiler: &Compiler<'m>,
) -> CompileErrors {
    let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs, &compiler.seal_ctx);

    debug!("Generating code for\n{}", func);

    let body = func.body;
    let body_span = body.data().span();

    let data_flow = DataFlowAnalyzer::new(&compiler.defs).analyze(func.arg.0.var, body.as_ref());
    let mut errors = CompileErrors::default();

    let return_ty = body.data().ty();

    let mut builder = ProcBuilder::new(NParams(0));
    let mut root_block = builder.new_block(Delta(1), body_span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors: &mut errors,
        primitives: &compiler.primitives,
        type_mapper,
        bug_span: body_span,
    };
    generator.scope.insert(func.arg.0.var, Local(0));
    generator.gen_node(body.as_ref(), &mut root_block);
    root_block.commit(Terminator::Return(builder.top()), &mut builder);

    match (
        type_mapper.find_domain_mapping_info(func.arg.meta().ty),
        type_mapper.find_domain_mapping_info(return_ty),
    ) {
        (Some(from_info), Some(to_info)) => {
            proc_table
                .map_procedures
                .insert((from_info.key, to_info.key), builder);

            if let Some(data_flow) = data_flow {
                proc_table
                    .propflow_table
                    .insert((from_info.key, to_info.key), data_flow);
            }

            errors
        }
        (from_info, to_info) => {
            panic!("Problem finding def ids: ({from_info:?}, {to_info:?})");
        }
    }
}

pub(super) struct CodeGenerator<'a, 'm> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,
    pub errors: &'a mut CompileErrors,
    pub primitives: &'a Primitives,
    pub type_mapper: TypeMapper<'a, 'm>,
    bug_span: SourceSpan,

    scope: FnvHashMap<ontol_hir::Var, Local>,
}

impl<'a, 'm> CodeGenerator<'a, 'm> {
    fn gen_node<'h>(&mut self, node_ref: TypedNodeRef<'h, 'm>, block: &mut Block) {
        let arena = node_ref.arena();
        let (kind, meta) = (&node_ref.0, &node_ref.1);
        let ty = meta.ty;
        let span = meta.span;
        match kind {
            ontol_hir::Kind::Var(var) => {
                let Some(local) = self.scope.get(var) else {
                    return self.errors.error(CompileError::UnboundVariable, &span);
                };

                block.op(OpCode::Clone(*local), Delta(1), span, self.builder);
            }
            ontol_hir::Kind::Unit => {
                block.op(
                    OpCode::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Delta(1),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::I64(int) => {
                block.op(
                    OpCode::I64(*int, ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::F64(float) => {
                block.op(
                    OpCode::F64(*float, ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Text(string) => {
                block.op(
                    OpCode::String(string.clone(), ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Const(const_def_id) => {
                let proc = Procedure {
                    address: self.proc_table.gen_const_addr(*const_def_id),
                    n_params: NParams(0),
                };
                block.op(OpCode::Call(proc), Delta(1), span, self.builder);
            }
            ontol_hir::Kind::Let(binder, definition, body) => {
                self.gen_node(arena.node_ref(*definition), block);
                self.scope.insert(binder.hir().var, self.builder.top());
                for node_ref in arena.refs(body) {
                    self.gen_node(node_ref, block);
                }
                self.scope.remove(&binder.hir().var);
            }
            ontol_hir::Kind::Call(proc, args) => {
                let stack_delta = Delta(-(args.len() as i32) + 1);
                for param in arena.refs(args) {
                    self.gen_node(param, block);
                }
                let return_def_id = ty.get_single_def_id().unwrap();
                block.op(
                    OpCode::CallBuiltin(*proc, return_def_id),
                    stack_delta,
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Map(param) => {
                let param = arena.node_ref(*param);
                let param_ty = param.ty();
                self.gen_node(param, block);
                match (
                    self.type_mapper.find_domain_mapping_info(param_ty),
                    self.type_mapper.find_domain_mapping_info(ty),
                ) {
                    (Some(from), Some(to)) => match (from.punned, to.punned) {
                        (Some(from_pun), Some(to_pun)) if from.anonymous && to.anonymous => {
                            if from_pun == to_pun {
                                self.gen_pun(block, to.key.def_id, span);
                            } else {
                                let proc = Procedure {
                                    address: self.proc_table.gen_mapping_addr(from_pun, to_pun),
                                    n_params: NParams(1),
                                };

                                block.op(OpCode::Call(proc), Delta(0), span, self.builder);
                            }
                        }
                        _ => {
                            let proc = Procedure {
                                address: self.proc_table.gen_mapping_addr(from.key, to.key),
                                n_params: NParams(1),
                            };

                            block.op(OpCode::Call(proc), Delta(0), span, self.builder);
                        }
                    },
                    (Some(from), None) => {
                        if from.punned.map(|key| key.def_id) == ty.get_single_def_id() {
                            self.gen_pun(block, ty.get_single_def_id().unwrap(), span);
                        } else {
                            self.report_not_mappable(span);
                        }
                    }
                    (None, Some(to)) => {
                        if param_ty.get_single_def_id() == to.punned.map(|key| key.def_id) {
                            self.gen_pun(block, param_ty.get_single_def_id().unwrap(), span);
                        } else {
                            self.report_not_mappable(span);
                        }
                    }
                    _ => {
                        self.report_not_mappable(span);
                    }
                }
            }
            ontol_hir::Kind::Struct(binder, _flags, nodes) => {
                let def_id = ty.get_single_def_id().unwrap();
                let local = block.op(
                    OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id),
                    Delta(1),
                    span,
                    self.builder,
                );
                self.scope.insert(binder.hir().var, local);
                for node_ref in arena.refs(nodes) {
                    self.gen_node(node_ref, block);
                    block.pop_until(local, span, self.builder);
                }
                self.scope.remove(&binder.hir().var);
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, variants) => {
                if let Some(variant) = variants.into_iter().next() {
                    match variant {
                        PropVariant::Singleton(attr) => {
                            self.gen_attribute(*struct_var, *prop_id, *attr, arena, span, block)
                        }
                        PropVariant::Seq(seq_variant) => {
                            if let Some((_iter, attr)) = seq_variant.elements.iter().next() {
                                self.gen_attribute(
                                    *struct_var,
                                    *prop_id,
                                    *attr,
                                    arena,
                                    span,
                                    block,
                                );
                            }
                        }
                    }
                }
            }
            ontol_hir::Kind::MatchProp(struct_var, prop_id, arms) => {
                let Ok(struct_local) = self.var_local(*struct_var) else {
                    return;
                };

                if arms.len() > 1 {
                    if arms
                        .iter()
                        .any(|(pattern, _)| matches!(pattern, ontol_hir::PropPattern::Absent))
                    {
                        block.op(
                            OpCode::GetAttr(
                                struct_local,
                                *prop_id,
                                GetAttrFlags::TRY | GetAttrFlags::REL | GetAttrFlags::VAL,
                            ),
                            // even if three locals are pushed, the `status` one
                            // is not kept track of here.
                            Delta(2),
                            span,
                            self.builder,
                        );

                        let status_local = self.builder.top_minus(1);
                        let post_cond_offset = block.current_offset().plus(1);

                        // These overlap with the status_local, but that will be yanked(!) in the Cond opcode,
                        // leading into the present block.
                        let val_local = self.builder.top();
                        let rel_local = self.builder.top_minus(1);

                        let present_body_index = {
                            let mut present_block = self.builder.new_block(Delta(0), span);

                            for arm in arms {
                                if !matches!(arm.0, ontol_hir::PropPattern::Absent) {
                                    self.gen_match_arm(
                                        arm,
                                        (rel_local, val_local),
                                        arena,
                                        &mut present_block,
                                    );
                                }
                            }

                            present_block.commit(
                                Terminator::PopGoto(block.index(), post_cond_offset),
                                self.builder,
                            )
                        };

                        block.ir(
                            Ir::Cond(Predicate::YankTrue(status_local), present_body_index),
                            Delta(0),
                            span,
                            self.builder,
                        );
                    } else {
                        todo!();
                    }
                } else {
                    let arm = arms.into_iter().next().unwrap();

                    match &arm.0 {
                        PropPattern::Seq(ontol_hir::Binding::Binder(binder), HasDefault(true)) => {
                            block.op(
                                OpCode::GetAttr(
                                    struct_local,
                                    *prop_id,
                                    GetAttrFlags::TRY | GetAttrFlags::REL | GetAttrFlags::VAL,
                                ),
                                Delta(2),
                                span,
                                self.builder,
                            );

                            let status_local = self.builder.top_minus(1);
                            let post_cond_offset = block.current_offset().plus(1);

                            // These overlap with the status_local, but that will be yanked(!) in the Cond opcode,
                            // leading into the present block.
                            let val_local = self.builder.top();
                            let rel_local = self.builder.top_minus(1);

                            let Type::Seq(_, seq_item_ty) = binder.meta().ty else {
                                panic!("Not a sequence:");
                            };

                            // Code for generating the default values:
                            let default_fallback_body_index = {
                                let mut default_block = self.builder.new_block(Delta(0), span);

                                // rel_params (unit)
                                default_block.op(
                                    OpCode::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                                    Delta(0),
                                    NO_SPAN,
                                    self.builder,
                                );
                                // empty sequence
                                default_block.op(
                                    OpCode::CallBuiltin(
                                        BuiltinProc::NewSeq,
                                        seq_item_ty.get_single_def_id().unwrap(),
                                    ),
                                    Delta(0),
                                    NO_SPAN,
                                    self.builder,
                                );

                                default_block.commit(
                                    Terminator::PopGoto(block.index(), post_cond_offset),
                                    self.builder,
                                )
                            };

                            // If the TryTakeAttr2 was false, run the default body
                            block.ir(
                                Ir::Cond(
                                    Predicate::YankFalse(status_local),
                                    default_fallback_body_index,
                                ),
                                Delta(0),
                                span,
                                self.builder,
                            );

                            self.gen_match_arm(arm, (rel_local, val_local), arena, block);
                        }
                        _ => {
                            block.op(
                                OpCode::GetAttr(
                                    struct_local,
                                    *prop_id,
                                    // note: No TRY
                                    GetAttrFlags::REL | GetAttrFlags::VAL,
                                ),
                                Delta(2),
                                span,
                                self.builder,
                            );

                            let val_local = self.builder.top();
                            let rel_local = self.builder.top_minus(1);

                            self.gen_match_arm(arm, (rel_local, val_local), arena, block);
                        }
                    }
                }
            }
            ontol_hir::Kind::Sequence(binder, nodes) => {
                let Type::Seq(_, val_ty) = ty else {
                    panic!("Not a sequence: {ty:?}");
                };
                let seq_local = block.op(
                    OpCode::CallBuiltin(
                        BuiltinProc::NewSeq,
                        val_ty
                            .get_single_def_id()
                            .unwrap_or_else(|| panic!("val_ty: {val_ty:?}")),
                    ),
                    Delta(1),
                    span,
                    self.builder,
                );
                self.gen_in_scope(
                    &[(seq_local, ontol_hir::Binding::Binder(*binder))],
                    nodes,
                    arena,
                    block,
                );
                block.pop_until(seq_local, span, self.builder);
            }
            ontol_hir::Kind::ForEach(seq_var, (rel_binding, val_binding), nodes) => {
                let Ok(seq_local) = self.var_local(*seq_var) else {
                    return;
                };
                let counter = block.op(OpCode::I64(0, DefId::unit()), Delta(1), span, self.builder);

                let iter_offset = block.current_offset();
                let elem_rel_local = self.builder.top_plus(1);
                let elem_val_local = self.builder.top_plus(2);

                let for_each_body_index = {
                    let mut iter_block = self.builder.new_block(Delta(2), span);

                    self.gen_in_scope(
                        &[
                            (elem_rel_local, *rel_binding),
                            (elem_val_local, *val_binding),
                        ],
                        nodes,
                        arena,
                        &mut iter_block,
                    );

                    iter_block.pop_until(counter, span, self.builder);
                    iter_block.commit(
                        Terminator::PopGoto(block.index(), iter_offset),
                        self.builder,
                    )
                };

                block.ir(
                    Ir::Iter(seq_local, counter, for_each_body_index),
                    Delta(0),
                    span,
                    self.builder,
                );
                block.pop_until(counter, span, self.builder);
            }
            ontol_hir::Kind::SeqPush(seq_var, attr) => {
                let top = self.builder.top();
                let Ok(seq_local) = self.var_local(*seq_var) else {
                    return;
                };
                self.gen_node(arena.node_ref(attr.rel), block);
                let rel_local = self.builder.top();

                self.gen_node(arena.node_ref(attr.val), block);

                block.op(OpCode::Clone(rel_local), Delta(1), span, self.builder);
                block.op(
                    OpCode::AppendAttr2(seq_local),
                    Delta(-2),
                    span,
                    self.builder,
                );
                block.pop_until(top, span, self.builder);
            }
            ontol_hir::Kind::StringPush(to_var, node) => {
                let top = self.builder.top();
                let Ok(to_local) = self.var_local(*to_var) else {
                    return;
                };
                self.gen_node(arena.node_ref(*node), block);
                block.op(
                    OpCode::AppendString(to_local),
                    Delta(-1),
                    span,
                    self.builder,
                );
                block.pop_until(top, span, self.builder);
            }
            ontol_hir::Kind::MatchRegex(
                ontol_hir::Iter(false),
                haystack_var,
                regex_def_id,
                match_arms,
            ) => {
                if match_arms.is_empty() {
                    return;
                }
                let Ok(haystack_local) = self.var_local(*haystack_var) else {
                    return;
                };
                let top = self.builder.top();

                let mut pre = self.builder.split_block(block);
                let mut arms_gen =
                    CaptureMatchArmsGenerator::new_with_blocks(match_arms, self.builder, span);

                pre.op(
                    OpCode::RegexCapture(haystack_local, *regex_def_id),
                    Delta((arms_gen.capture_index_union.len() + 1) as i32),
                    span,
                    self.builder,
                );
                pre.op(
                    OpCode::RegexCaptureIndexes(
                        arms_gen.capture_index_union.clone().into_bit_vec(),
                    ),
                    Delta(0),
                    span,
                    self.builder,
                );
                // unconditional match (for now)
                pre.ir(
                    Ir::Cond(
                        Predicate::YankFalse(Local(
                            top.0 + arms_gen.capture_index_union.iter().count() as u16 + 1,
                        )),
                        arms_gen.fail_block_index,
                    ),
                    Delta(-1),
                    span,
                    self.builder,
                );
                pre.commit(
                    Terminator::Goto(arms_gen.first_block_index, BlockOffset(0)),
                    self.builder,
                );

                self.gen_capture_match_arms(
                    &mut arms_gen,
                    Local(top.0 + 1),
                    Terminator::Goto(block.index(), BlockOffset(0)),
                    arena,
                    span,
                );

                block.pop_until(top, span, self.builder);
            }
            ontol_hir::Kind::MatchRegex(
                ontol_hir::Iter(true),
                haystack_var,
                regex_def_id,
                match_arms,
            ) => {
                if match_arms.is_empty() {
                    return;
                }
                let Ok(haystack_local) = self.var_local(*haystack_var) else {
                    return;
                };
                let top = self.builder.top();
                let iter_offset = BlockOffset(block.current_offset().0 + 3);
                let all_matches_seq_local = self.builder.top_plus(1);
                let counter_local = self.builder.top_plus(2);
                let current_matches_seq_local = self.builder.top_plus(4);

                // compensating for the iterated list and the counter below
                self.builder.prealloc_stack(Delta(2));

                let (iter_block_index, arms_gen) = {
                    let mut pre = self.builder.new_block(Delta(2), span);
                    let mut arms_gen =
                        CaptureMatchArmsGenerator::new_with_blocks(match_arms, self.builder, span);
                    let mut post = self.builder.new_block(Delta(0), span);

                    pre.op(
                        OpCode::MoveSeqValsToStack(current_matches_seq_local),
                        Delta((arms_gen.capture_index_union.len()) as i32),
                        span,
                        self.builder,
                    );
                    let for_each_block_index = pre.commit(
                        Terminator::Goto(arms_gen.first_block_index, BlockOffset(0)),
                        self.builder,
                    );

                    self.gen_capture_match_arms(
                        &mut arms_gen,
                        Local(current_matches_seq_local.0 + 1),
                        Terminator::Goto(post.index(), BlockOffset(0)),
                        arena,
                        span,
                    );

                    post.pop_until(counter_local, span, self.builder);
                    post.commit(
                        Terminator::PopGoto(block.index(), iter_offset),
                        self.builder,
                    );

                    (for_each_block_index, arms_gen)
                };

                // Generate sequence
                block.op(
                    OpCode::RegexCaptureIter(haystack_local, *regex_def_id),
                    Delta(0),
                    span,
                    self.builder,
                );
                block.op(
                    OpCode::RegexCaptureIndexes(
                        arms_gen.capture_index_union.clone().into_bit_vec(),
                    ),
                    Delta(0),
                    span,
                    self.builder,
                );
                block.op(OpCode::I64(0, DefId::unit()), Delta(0), span, self.builder);
                block.ir(
                    Ir::Iter(all_matches_seq_local, counter_local, iter_block_index),
                    Delta(0),
                    span,
                    self.builder,
                );
                block.pop_until(top, span, self.builder);
            }
            ontol_hir::Kind::DeclSeq(..) | ontol_hir::Kind::Regex(..) => {
                unreachable!(
                    "{} is only declarative, not used in code generation",
                    node_ref
                );
            }
        }
    }

    fn gen_match_arm(
        &mut self,
        (pattern, nodes): &(ontol_hir::PropPattern<'m, TypedHir>, ontol_hir::Nodes),
        (rel_local, val_local): (Local, Local),
        arena: &TypedArena<'m>,
        block: &mut Block,
    ) {
        match pattern {
            ontol_hir::PropPattern::Attr(rel_binding, val_binding) => self.gen_in_scope(
                &[(rel_local, *rel_binding), (val_local, *val_binding)],
                nodes,
                arena,
                block,
            ),
            ontol_hir::PropPattern::Seq(binding, _has_default) => self.gen_in_scope(
                &[
                    (rel_local, ontol_hir::Binding::Wildcard),
                    (val_local, *binding),
                ],
                nodes,
                arena,
                block,
            ),
            ontol_hir::PropPattern::Absent => {
                todo!("Arm pattern not present")
            }
        }
    }

    fn gen_attribute(
        &mut self,
        struct_var: ontol_hir::Var,
        prop_id: PropertyId,
        ontol_hir::Attribute { rel, val }: ontol_hir::Attribute<ontol_hir::Node>,
        arena: &TypedArena<'m>,
        span: SourceSpan,
        block: &mut Block,
    ) {
        let Ok(struct_local) = self.var_local(struct_var) else {
            return;
        };

        match arena.kind(rel) {
            ontol_hir::Kind::Unit => {
                self.gen_node(arena.node_ref(val), block);
                block.op(
                    OpCode::PutAttr1(struct_local, prop_id),
                    Delta(-1),
                    span,
                    self.builder,
                );
            }
            _ => {
                self.gen_node(arena.node_ref(rel), block);
                let rel_local = self.builder.top();
                self.gen_node(arena.node_ref(val), block);

                block.op(OpCode::Clone(rel_local), Delta(1), span, self.builder);
                block.op(
                    OpCode::PutAttr2(struct_local, prop_id),
                    Delta(-2),
                    span,
                    self.builder,
                );
            }
        }
    }

    fn gen_in_scope<'n>(
        &mut self,
        scopes: &[(Local, ontol_hir::Binding<TypedHir>)],
        nodes: impl IntoIterator<Item = &'n ontol_hir::Node>,
        arena: &TypedArena<'m>,
        block: &mut Block,
    ) {
        for (local, binding) in scopes {
            if let ontol_hir::Binding::Binder(binder) = binding {
                if self.scope.insert(binder.hir().var, *local).is_some() {
                    panic!("Variable {} already in scope", binder.hir().var);
                }
            }
        }

        for node_ref in arena.refs(nodes) {
            self.gen_node(node_ref, block);
        }

        for (_, binding) in scopes {
            if let ontol_hir::Binding::Binder(binder) = binding {
                self.scope.remove(&binder.hir().var);
            }
        }
    }

    fn gen_capture_match_arms(
        &mut self,
        arms_gen: &mut CaptureMatchArmsGenerator<'_, 'm>,
        first_capture_local: Local,
        terminator: Terminator,
        arena: &TypedArena<'m>,
        span: SourceSpan,
    ) {
        let branch_block_indexes: Vec<BlockIndex> = (0..arms_gen.match_arms.len())
            .map(|index| arms_gen.branch_blocks.get(&index).unwrap().index())
            .collect();

        arms_gen.fail_block_index = {
            self.builder.new_block(Delta(0), span).commit(
                Terminator::Panic("Regex did not match".into()),
                self.builder,
            )
        };

        for (arm_index, match_arm) in std::mem::take(&mut arms_gen.match_arms).iter().enumerate() {
            let mut branch_block = arms_gen.branch_blocks.remove(&arm_index).unwrap();

            struct ArmCapture {
                local: Local,
                var: ontol_hir::Var,
                def_id: DefId,
            }

            let mut arm_captures = Vec::with_capacity(match_arm.capture_groups.len());

            for capture_group in &match_arm.capture_groups {
                let (local_position, _) = arms_gen
                    .capture_index_union
                    .iter()
                    .enumerate()
                    .find(|(_, group_index)| *group_index == capture_group.index as usize)
                    .unwrap();

                debug!("guard capture local pos {:?}", local_position);

                arm_captures.push(ArmCapture {
                    local: Local(first_capture_local.0 + local_position as u16),
                    var: capture_group.binder.hir().var,
                    def_id: capture_group.binder.meta().ty.get_single_def_id().unwrap(),
                });
            }

            // guard - advance to the next arm (or panic block) if some condition is not true,
            // in this case that a required capture group was not defined (encoded as a unit value).
            // If there are no bound capture groups, this is a _catch all_ arm.
            if let Some(guard_capture) = arm_captures.first() {
                let else_block_index = if arm_index < branch_block_indexes.len() - 1 {
                    *branch_block_indexes.get(arm_index + 1).unwrap()
                } else {
                    arms_gen.fail_block_index
                };

                branch_block.ir(
                    Ir::Cond(Predicate::IsUnit(guard_capture.local), else_block_index),
                    Delta(0),
                    span,
                    self.builder,
                );
            }

            // define scope for capture and type pun
            for arm_capture in &arm_captures {
                if self
                    .scope
                    .insert(arm_capture.var, arm_capture.local)
                    .is_some()
                {
                    panic!("Variable {} already in scope", arm_capture.var);
                }

                if arm_capture.def_id != self.primitives.text {
                    branch_block.op(
                        OpCode::TypePun(arm_capture.local, arm_capture.def_id),
                        Delta(0),
                        span,
                        self.builder,
                    );
                }
            }

            for node_ref in arena.refs(&match_arm.nodes) {
                self.gen_node(node_ref, &mut branch_block);
            }

            // pop scope
            for arm_capture in arm_captures {
                self.scope.remove(&arm_capture.var);
            }

            branch_block.commit(terminator.clone(), self.builder);
        }
    }

    fn var_local(&mut self, var: ontol_hir::Var) -> Result<Local, ()> {
        match self.scope.get(&var) {
            Some(local) => Ok(*local),
            None => {
                self.errors.error(
                    CompileError::BUG(smart_format!("Variable {var} not in scope")),
                    &self.bug_span,
                );
                Err(())
            }
        }
    }

    fn gen_pun(&mut self, block: &mut Block, def_id: DefId, span: SourceSpan) {
        let local = self.builder.top();
        block.op(OpCode::TypePun(local, def_id), Delta(0), span, self.builder);
    }

    fn report_not_mappable(&mut self, span: SourceSpan) {
        self.errors.error(
            CompileError::TODO(smart_format!("type not mappable")),
            &span,
        );
    }
}

struct CaptureMatchArmsGenerator<'h, 'm> {
    match_arms: &'h [ontol_hir::CaptureMatchArm<'m, TypedHir>],
    branch_blocks: FnvHashMap<usize, Block>,
    capture_index_union: BitSet,
    first_block_index: BlockIndex,
    fail_block_index: BlockIndex,
}

impl<'h, 'm> CaptureMatchArmsGenerator<'h, 'm> {
    fn new_with_blocks(
        match_arms: &'h [ontol_hir::CaptureMatchArm<'m, TypedHir>],
        builder: &mut ProcBuilder,
        span: SourceSpan,
    ) -> Self {
        let mut capture_index_union: BitSet = BitSet::default();
        let mut branch_blocks: FnvHashMap<usize, Block> = Default::default();
        let mut first_block_index = BlockIndex(0);

        for (arm_index, match_arm) in match_arms.iter().enumerate() {
            for capture_group in &match_arm.capture_groups {
                capture_index_union.insert(capture_group.index as usize);
            }

            let branch_block = builder.new_block(Delta(0), span);

            if arm_index == 0 {
                first_block_index = branch_block.index();
            }

            branch_blocks.insert(arm_index, branch_block);
        }

        Self {
            match_arms,
            branch_blocks,
            capture_index_union,
            first_block_index,
            fail_block_index: BlockIndex(0),
        }
    }
}

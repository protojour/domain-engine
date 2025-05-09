use std::collections::BTreeMap;

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::{Binding, EvalCondTerm, Node, OverloadFunc, Pack, PropVariant, StructFlags};
use ontol_parser::source::SourceSpan;
use ontol_runtime::{
    DefId, MapDirection, MapFlags, MapKey, PropId,
    ontology::map::MapLossiness,
    property::{Cardinality, PropertyCardinality, ValueCardinality},
    query::condition::{Clause, ClausePair},
    var::{Var, VarSet},
    vm::proc::{
        BuiltinProc, GetAttrFlags, Local, NParams, OpCode, OpCodeCondTerm, Predicate, Procedure,
    },
};
use thin_vec::ThinVec;
use tracing::debug;

use crate::{
    CompileErrors, Compiler,
    codegen::{
        data_flow_analyzer::DataFlowAnalyzer,
        ir::{BlockLabel, BlockOffset, Terminator},
        proc_builder::Delta,
    },
    def::Defs,
    error::CompileError,
    properties::PropCtx,
    relation::{RelCtx, rel_def_meta},
    repr::{
        repr_ctx::ReprCtx,
        repr_model::{ReprKind, ReprScalarKind},
    },
    strings::StringCtx,
    typed_hir::{HirFunc, TypedArena, TypedHir, TypedNodeRef},
    types::{Type, UNIT_TYPE},
};

use super::{
    ir::Ir,
    proc_builder::{Block, ProcBuilder},
    task::{MapOutputMeta, ProcTable},
    type_mapper::TypeMapper,
};

pub(super) fn const_codegen<'m>(
    expr: ontol_hir::RootNode<'m, TypedHir>,
    def_id: DefId,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) {
    let type_mapper = TypeMapper::new(&compiler.rel_ctx, &compiler.defs, &compiler.repr_ctx);

    debug!("Generating const code for\n{}", expr);

    let expr_meta = *expr.data().meta();

    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(Delta(0), expr_meta.span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        errors: &mut compiler.errors,
        strings: &mut compiler.str_ctx,
        repr_ctx: &compiler.repr_ctx,
        type_mapper,
        scope: Default::default(),
        catch_points: Default::default(),
    };
    generator.gen_node(expr.as_ref(), &mut block);
    block.commit(Terminator::Return, &mut builder);

    proc_table.const_procedures.insert(def_id, builder);
}

/// The intention for this is to be parallelizable,
/// but that won't work with `&mut ProcTable`.
/// Maybe find a solution for that.
pub(super) fn map_codegen<'m>(
    proc_table: &mut ProcTable,
    func: &HirFunc<'m>,
    map_flags: MapFlags,
    direction: MapDirection,
    compiler: &mut Compiler<'m>,
) -> MapKey {
    let type_mapper = TypeMapper::new(&compiler.rel_ctx, &compiler.defs, &compiler.repr_ctx);

    let body = &func.body;
    let body_span = body.data().span();

    let lossiness = match ontol_hir::find_value_node(body.as_ref()) {
        Some(node_ref) => match node_ref.kind() {
            ontol_hir::Kind::Struct(_, flags, _) => {
                if flags.contains(StructFlags::MATCH) {
                    MapLossiness::Lossy
                } else {
                    MapLossiness::Complete
                }
            }
            _ => MapLossiness::Complete,
        },
        None => MapLossiness::Complete,
    };

    fn get_subject_cardinality<'c>(
        prop_id: PropId,
        rel_ctx: &'c RelCtx,
        prop_ctx: &'c PropCtx,
        defs: &'c Defs,
    ) -> Cardinality {
        match prop_ctx.property_by_id(prop_id) {
            Some(property) => {
                rel_def_meta(property.rel_id, rel_ctx, defs)
                    .relationship
                    .subject_cardinality
            }
            None => (PropertyCardinality::Mandatory, ValueCardinality::Unit),
        }
    }

    let data_flow = DataFlowAnalyzer::new(
        &compiler.defs,
        &compiler.rel_ctx,
        &compiler.prop_ctx,
        &get_subject_cardinality,
    )
    .analyze(func.arg.0.var, body.as_ref());

    let return_ty = body.data().ty();

    let mut builder = ProcBuilder::new(NParams(1));
    let mut root_block = builder.new_block(Delta(1), body_span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        errors: &mut compiler.errors,
        strings: &mut compiler.str_ctx,
        repr_ctx: &compiler.repr_ctx,
        type_mapper,
        scope: Default::default(),
        catch_points: Default::default(),
    };
    generator.scope_insert(func.arg.0.var, Local(0), &body_span);
    generator.gen_node(body.as_ref(), &mut root_block);
    root_block.commit(Terminator::Return, &mut builder);

    match (
        type_mapper.find_domain_mapping_info(func.arg.meta().ty),
        type_mapper.find_domain_mapping_info(return_ty),
    ) {
        (Some(input_info), Some(output_info)) => {
            let input = input_info.map_def;
            let output = output_info.map_def;

            let map_key = MapKey {
                input,
                output,
                flags: map_flags,
            };

            proc_table.map_procedures.insert(map_key, builder);

            if let Some(data_flow) = data_flow {
                proc_table.propflow_table.insert(map_key, data_flow);
            }

            proc_table.metadata_table.insert(
                map_key,
                MapOutputMeta {
                    lossiness,
                    direction,
                },
            );

            map_key
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
    pub strings: &'a mut StringCtx<'m>,
    pub repr_ctx: &'a ReprCtx,
    pub type_mapper: TypeMapper<'a, 'm>,

    scope: FnvHashMap<Var, Local>,
    catch_points: FnvHashMap<ontol_hir::Label, BlockLabel>,
}

impl<'m> CodeGenerator<'_, 'm> {
    fn gen_node<'h>(&mut self, node_ref: TypedNodeRef<'h, 'm>, block: &mut Block) {
        let arena = node_ref.arena();
        let (kind, meta) = (&node_ref.0, &node_ref.1);
        let ty = meta.ty;
        let span = meta.span;
        match kind {
            ontol_hir::Kind::NoOp => {}
            ontol_hir::Kind::Var(var) => {
                let Some(local) = self.scope.get(var) else {
                    debug!("unbound variable {var}");
                    return CompileError::UnboundVariable.span(span).report(self.errors);
                };

                block.op(OpCode::Clone(*local), Delta(1), span, self.builder);
            }
            ontol_hir::Kind::Block(body) => {
                for node_ref in arena.node_refs(body) {
                    self.gen_node(node_ref, block);
                }
            }
            ontol_hir::Kind::Catch(label, body) => {
                let top = self.builder.top();

                {
                    let (_label, mut pre) = self.builder.split_block(block);
                    self.catch_points.insert(*label, block.label());

                    for node_ref in arena.node_refs(body) {
                        self.gen_node(node_ref, &mut pre);
                    }

                    pre.commit(
                        Terminator::Goto(block.label(), BlockOffset(0)),
                        self.builder,
                    );
                }

                block.pop_until(top, span, self.builder);

                self.catch_points.remove(label);
            }
            ontol_hir::Kind::CatchFunc(label, body) => {
                {
                    let (_label, mut pre) = self.builder.split_block(block);
                    self.catch_points.insert(*label, block.label());

                    for node_ref in arena.node_refs(body) {
                        self.gen_node(node_ref, &mut pre);
                    }

                    pre.commit(Terminator::Return, self.builder);
                }

                block.op(
                    OpCode::CallBuiltin(BuiltinProc::NewVoid, DefId::unit()),
                    Delta(1),
                    span,
                    self.builder,
                );

                self.catch_points.remove(label);
            }
            ontol_hir::Kind::Try(catch_label, var) => {
                let catch = self.catch_dest(*catch_label, &span);
                let Ok(local) = self.var_local(*var, &span) else {
                    return;
                };

                block.ir(
                    Ir::Cond(Predicate::IsVoid(local), catch),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::TryNarrow(catch_label, var) => {
                let catch = self.catch_dest(*catch_label, &span);
                let Ok(local) = self.var_local(*var, &span) else {
                    return;
                };
                let def_id = ty.get_single_def_id().unwrap();
                block.ir(
                    Ir::Cond(Predicate::NotMatchesDiscriminant(local, def_id), catch),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Let(binder, definition) => {
                self.gen_node(arena.node_ref(*definition), block);
                self.scope_insert(binder.hir().var, self.builder.top(), &span);
            }
            ontol_hir::Kind::TryLet(catch_label, binder, definition) => {
                let catch = self.catch_dest(*catch_label, &span);
                let local = match arena.node_ref(*definition).hir() {
                    ontol_hir::Kind::Var(source_var) => {
                        let Ok(local) = self.var_local(*source_var, &span) else {
                            return;
                        };

                        // make an alias
                        self.scope_insert(binder.hir().var, local, &span);

                        local
                    }
                    _ => {
                        self.gen_node(arena.node_ref(*definition), block);
                        self.scope_insert(binder.hir().var, self.builder.top(), &span);
                        self.builder.top()
                    }
                };

                block.ir(
                    Ir::Cond(Predicate::IsVoid(local), catch),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::LetProp(bind_pack, (struct_var, prop_id)) => {
                let Ok(struct_local) = self.var_local(*struct_var, &span) else {
                    return;
                };

                let mut delta = 0;
                let mut len: u8 = 0;

                match bind_pack {
                    Pack::Unit(binding) => {
                        if let ontol_hir::Binding::Binder(binder) = binding {
                            delta += 1;
                            len += 1;
                            self.scope_insert(
                                binder.hir().var,
                                self.builder.top_plus(delta),
                                &binder.meta().span,
                            );
                        }
                    }
                    Pack::Tuple(t) => {
                        for binding in t {
                            if let ontol_hir::Binding::Binder(binder) = binding {
                                delta += 1;
                                len += 1;
                                self.scope_insert(
                                    binder.hir().var,
                                    self.builder.top_plus(delta),
                                    &binder.meta().span,
                                );
                            }
                        }
                    }
                }

                if delta > 0 {
                    block.op(
                        OpCode::GetAttr(struct_local, *prop_id, len, GetAttrFlags::TAKE),
                        Delta(delta as i32),
                        span,
                        self.builder,
                    );
                }
            }
            ontol_hir::Kind::LetPropDefault(bind_pack, (struct_var, prop_id), default) => {
                let Ok(struct_local) = self.var_local(*struct_var, &span) else {
                    return;
                };

                let mut delta = 0;

                let before = self.builder.top();

                let mut len: u8 = 0;

                match bind_pack {
                    Pack::Unit(binding) => {
                        if let ontol_hir::Binding::Binder(binder) = binding {
                            delta += 1;
                            len += 1;
                            self.scope_insert(
                                binder.hir().var,
                                self.builder.top_plus(delta),
                                &binder.meta().span,
                            );
                        }
                    }
                    Pack::Tuple(t) => {
                        for binding in t {
                            if let ontol_hir::Binding::Binder(binder) = binding {
                                delta += 1;
                                len += 1;
                                self.scope_insert(
                                    binder.hir().var,
                                    self.builder.top_plus(delta),
                                    &binder.meta().span,
                                );
                            }
                        }
                    }
                }

                if delta == 0 {
                    return;
                }

                block.op(
                    OpCode::GetAttr(struct_local, *prop_id, len, GetAttrFlags::empty()),
                    Delta(delta as i32),
                    span,
                    self.builder,
                );

                let post_cond_offset = block.current_offset();

                let default_block_label = {
                    let mut default_block = self.builder.new_block(Delta(0), span);
                    let label = default_block.label();

                    default_block.pop_until(before, span, self.builder);

                    match bind_pack {
                        Pack::Unit(binding) => {
                            let default = default.iter().next().unwrap();

                            if matches!(binding, Binding::Binder(_)) {
                                self.gen_node(arena.node_ref(*default), &mut default_block);
                            }
                        }
                        Pack::Tuple(bindings) => {
                            for (binding, default) in bindings.iter().zip(default.iter()) {
                                if matches!(binding, Binding::Binder(_)) {
                                    self.gen_node(arena.node_ref(*default), &mut default_block);
                                }
                            }
                        }
                    }

                    default_block.commit(
                        Terminator::PopGoto(block.label(), post_cond_offset),
                        self.builder,
                    );
                    label
                };

                block.ir(
                    Ir::Cond(Predicate::IsVoid(self.builder.top()), default_block_label),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::TryLetProp(..) => {
                todo!()
            }
            ontol_hir::Kind::TryLetTup(catch_label, bindings, source) => {
                let catch = self.catch_dest(*catch_label, &span);

                self.gen_node(arena.node_ref(*source), block);

                let mut try_locals = vec![];

                for (index, binding) in bindings.iter().enumerate() {
                    if let ontol_hir::Binding::Binder(binder) = binding {
                        let local = self.builder.top_plus(1 + index as u16);
                        self.scope_insert(binder.hir().var, local, &span);
                        try_locals.push(local);
                    }
                }

                block.op(
                    OpCode::MoveSeqValsToStack(self.builder.top()),
                    Delta(bindings.len() as i32),
                    span,
                    self.builder,
                );

                for local in try_locals {
                    block.ir(
                        Ir::Cond(Predicate::IsVoid(local), catch),
                        Delta(0),
                        span,
                        self.builder,
                    );
                }
            }

            ontol_hir::Kind::LetRegex(groups_list, regex_def_id, haystack_var) => {
                if groups_list.is_empty() {
                    return;
                }
                let Ok(haystack_local) = self.var_local(*haystack_var, &span) else {
                    return;
                };

                let capture_index_union =
                    self.gen_regex_capture_index_union(groups_list, span, true);
                let bind_size = capture_index_union.len();

                let (_label, mut pre) = self.builder.split_block(block);
                let (fallback_label, mut fallback) = self.builder.split_block(block);

                pre.op(
                    OpCode::RegexCapture(haystack_local, *regex_def_id),
                    Delta(1),
                    span,
                    self.builder,
                );
                pre.op(
                    OpCode::RegexCaptureIndexes(capture_index_union.into_bit_vec()),
                    Delta(0),
                    span,
                    self.builder,
                );
                pre.ir(
                    Ir::Cond(Predicate::IsVoid(self.builder.top()), fallback_label),
                    Delta(0),
                    span,
                    self.builder,
                );
                pre.op(
                    OpCode::MoveSeqValsToStack(self.builder.top()),
                    Delta(bind_size as i32),
                    span,
                    self.builder,
                );
                pre.commit(
                    Terminator::Goto(block.label(), BlockOffset(0)),
                    self.builder,
                );
                for _ in 0..bind_size {
                    // push as many voids as there are bindings
                    fallback.op(
                        OpCode::CallBuiltin(BuiltinProc::NewVoid, DefId::unit()),
                        Delta(0),
                        span,
                        self.builder,
                    );
                }
                fallback.commit(
                    Terminator::Goto(block.label(), BlockOffset(0)),
                    self.builder,
                );
            }
            ontol_hir::Kind::LetRegexIter(binder, groups_list, regex_def_id, haystack_var) => {
                let Ok(haystack_local) = self.var_local(*haystack_var, &span) else {
                    return;
                };
                let capture_index_union =
                    self.gen_regex_capture_index_union(groups_list, span, false);
                block.op(
                    OpCode::RegexCaptureIter(haystack_local, *regex_def_id),
                    Delta(1),
                    span,
                    self.builder,
                );
                block.op(
                    OpCode::RegexCaptureIndexes(capture_index_union.into_bit_vec()),
                    Delta(0),
                    span,
                    self.builder,
                );
                self.scope_insert(binder.hir().var, self.builder.top(), &binder.meta().span);
            }
            ontol_hir::Kind::Unit => {
                block.op(
                    OpCode::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Delta(1),
                    span,
                    self.builder,
                );
                if ty != &UNIT_TYPE {
                    if let Some(def_id) = ty.get_single_def_id() {
                        block.op(OpCode::TypePunTop(def_id), Delta(0), span, self.builder);
                    }
                }
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
                let constant = self.strings.intern_constant(string);
                block.op(
                    OpCode::String(constant, ty.get_single_def_id().unwrap()),
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
            ontol_hir::Kind::With(binder, definition, body) => {
                self.gen_node(arena.node_ref(*definition), block);
                self.scope_insert(binder.hir().var, self.builder.top(), &span);
                for node_ref in arena.node_refs(body) {
                    self.gen_node(node_ref, block);
                }
                self.scope.remove(&binder.hir().var);
            }
            ontol_hir::Kind::Call(func, args) => {
                let stack_delta = Delta(-(args.len() as i32) + 1);
                for param in arena.node_refs(args) {
                    self.gen_node(param, block);
                }
                let return_def_id = ty.get_single_def_id().unwrap();
                let proc = match func {
                    OverloadFunc::Add => self.func_number_repr_switch(
                        (return_def_id, span),
                        BuiltinProc::AddI64,
                        BuiltinProc::AddF64,
                    ),
                    OverloadFunc::Sub => self.func_number_repr_switch(
                        (return_def_id, span),
                        BuiltinProc::SubI64,
                        BuiltinProc::SubF64,
                    ),
                    OverloadFunc::Mul => self.func_number_repr_switch(
                        (return_def_id, span),
                        BuiltinProc::MulI64,
                        BuiltinProc::MulF64,
                    ),
                    OverloadFunc::Div => self.func_number_repr_switch(
                        (return_def_id, span),
                        BuiltinProc::DivI64,
                        BuiltinProc::DivF64,
                    ),
                    OverloadFunc::Append => BuiltinProc::Append,
                    OverloadFunc::NewStruct => BuiltinProc::NewStruct,
                    OverloadFunc::NewSeq => BuiltinProc::NewSeq,
                    OverloadFunc::NewUnit => BuiltinProc::NewUnit,
                    OverloadFunc::NewFilter => BuiltinProc::NewFilter,
                    OverloadFunc::NewVoid => BuiltinProc::NewVoid,
                };

                block.op(
                    OpCode::CallBuiltin(proc, return_def_id),
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
                    (Some(input), Some(output)) => match (input.punned, output.punned) {
                        (Some(input_pun), Some(output_pun))
                            if input.anonymous && output.anonymous =>
                        {
                            if input_pun == output_pun {
                                block.op(
                                    OpCode::TypePunTop(output.map_def.def_id),
                                    Delta(0),
                                    span,
                                    self.builder,
                                );
                            } else {
                                let proc = Procedure {
                                    address: self.proc_table.gen_mapping_addr(MapKey {
                                        input: input_pun,
                                        output: output_pun,
                                        flags: MapFlags::empty(),
                                    }),
                                    n_params: NParams(1),
                                };

                                block.op(OpCode::Call(proc), Delta(0), span, self.builder);
                            }
                        }
                        _ => {
                            let proc = Procedure {
                                address: self.proc_table.gen_mapping_addr(MapKey {
                                    input: input.map_def,
                                    output: output.map_def,
                                    flags: MapFlags::empty(),
                                }),
                                n_params: NParams(1),
                            };

                            block.op(OpCode::Call(proc), Delta(0), span, self.builder);
                        }
                    },
                    _ => {
                        if ty.get_single_def_id() != param_ty.get_single_def_id() {
                            block.op(
                                OpCode::TypePunTop(ty.get_single_def_id().unwrap()),
                                Delta(0),
                                span,
                                self.builder,
                            );
                        }
                    }
                }
            }
            ontol_hir::Kind::Pun(param) => {
                let param = arena.node_ref(*param);
                self.gen_node(param, block);

                block.op(
                    OpCode::TypePunTop(ty.get_single_def_id().unwrap()),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Narrow(expr) => {
                let expr = arena.node_ref(*expr);
                self.gen_node(expr, block);
            }
            ontol_hir::Kind::Struct(binder, flags, nodes) => {
                if flags.contains(StructFlags::MATCH) {
                    // warn!("Skipping match-struct for now");

                    let value_cardinality = match ty {
                        Type::Seq(..) | Type::Matrix(..) => ValueCardinality::IndexSet,
                        _ => ValueCardinality::Unit,
                    };

                    let condition_local = block.op(
                        OpCode::CallBuiltin(BuiltinProc::NewFilter, DefId::unit()),
                        Delta(1),
                        span,
                        self.builder,
                    );
                    self.scope_insert(binder.hir().var, condition_local, &span);
                    for node_ref in arena.node_refs(nodes) {
                        self.gen_node(node_ref, block);

                        // block.pop_until(condition_local, span, self.builder);
                    }
                    self.scope.remove(&binder.hir().var);
                    block.pop_until(condition_local, span, self.builder);
                    block.op(
                        OpCode::MatchFilter(binder.0.var, value_cardinality),
                        Delta(0),
                        span,
                        self.builder,
                    );
                } else {
                    let Some(def_id) = ty.get_single_def_id() else {
                        panic!("No def_id for {ty:?}");
                    };
                    let local = block.op(
                        OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id),
                        Delta(1),
                        span,
                        self.builder,
                    );
                    self.scope_insert(binder.hir().var, local, &span);
                    for node_ref in arena.node_refs(nodes) {
                        self.gen_node(node_ref, block);
                        block.pop_until(local, span, self.builder);
                    }
                    self.scope.remove(&binder.hir().var);
                }
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, PropVariant::Unit(unit_node)) => {
                let Ok(struct_local) = self.var_local(*struct_var, &span) else {
                    return;
                };

                let unit_ref = arena.node_ref(*unit_node);

                if let Type::Matrix(elements) = unit_ref.meta().ty {
                    // The property is matrix-typed

                    if let ontol_hir::Kind::MakeMatrix(columns, body) = unit_ref.kind() {
                        let mut scope = vec![];

                        for binder in columns.iter() {
                            let Type::Seq(item_ty) = binder.meta().ty else {
                                panic!(
                                    "matrix column must be sequence-typed: {:?}",
                                    binder.meta().ty
                                );
                            };

                            let seq_local = block.op(
                                OpCode::CallBuiltin(
                                    BuiltinProc::NewSeq,
                                    item_ty
                                        .get_single_def_id()
                                        .unwrap_or_else(|| panic!("item_ty: {item_ty:?}")),
                                ),
                                Delta(1),
                                span,
                                self.builder,
                            );

                            scope.push((seq_local, ontol_hir::Binding::Binder(*binder)));
                        }

                        self.gen_in_scope(&scope, body, arena, block);

                        // PutAttrMat must see the values in reverse order
                        for (local, _) in scope.iter().rev() {
                            block.op(OpCode::Clone(*local), Delta(1), span, self.builder);
                        }

                        block.op(
                            OpCode::PutAttrMat(struct_local, columns.len() as u8, *prop_id),
                            Delta(-(columns.len() as i32)),
                            span,
                            self.builder,
                        );
                    } else if elements.len() == 1 {
                        self.gen_node(unit_ref, block);
                        block.op(
                            OpCode::PutAttrMat(struct_local, 1, *prop_id),
                            Delta(-1),
                            span,
                            self.builder,
                        );
                    } else {
                        panic!("multi-column matrix of this type not handled");
                    }
                } else {
                    self.gen_node(unit_ref, block);
                    block.op(
                        OpCode::PutAttrUnit(struct_local, *prop_id),
                        Delta(-1),
                        span,
                        self.builder,
                    );
                }
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, PropVariant::Tuple(tup)) => {
                let Ok(struct_local) = self.var_local(*struct_var, &span) else {
                    return;
                };

                let mut top: FnvHashMap<Node, Local> = Default::default();

                for node in tup.iter() {
                    self.gen_node(arena.node_ref(*node), block);
                    top.insert(*node, self.builder.top());
                }

                // PutAttrTup must see the values in reverse order
                for node in tup.iter().rev().skip(1) {
                    let local = top.get(node).unwrap();
                    block.op(OpCode::Clone(*local), Delta(1), span, self.builder);
                }

                block.op(
                    OpCode::PutAttrTup(struct_local, tup.len() as u8, *prop_id),
                    Delta(-(tup.len() as i32)),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::Prop(.., PropVariant::Predicate(..)) => todo!(),
            ontol_hir::Kind::MoveRestAttrs(target, source) => {
                let Ok(target_local) = self.var_local(*target, &span) else {
                    return;
                };
                let Ok(source_local) = self.var_local(*source, &span) else {
                    return;
                };

                block.op(
                    OpCode::MoveRestAttrs(target_local, source_local),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::MakeSeq(binder, nodes) => {
                let item_ty = match ty {
                    Type::Seq(item_ty) => item_ty,
                    Type::Matrix(columns) if columns.len() == 1 => columns[0],
                    _ => {
                        CompileError::TODO(format!("unable to coerce to sequence: {ty:?}"))
                            .span(span)
                            .report(self.errors);
                        return;
                    }
                };
                let seq_local = block.op(
                    OpCode::CallBuiltin(
                        BuiltinProc::NewSeq,
                        item_ty
                            .get_single_def_id()
                            .unwrap_or_else(|| panic!("val_ty: {item_ty:?}")),
                    ),
                    Delta(1),
                    span,
                    self.builder,
                );
                let mut scope = vec![];
                if let Some(binder) = binder {
                    scope.push((seq_local, ontol_hir::Binding::Binder(*binder)));
                }

                self.gen_in_scope(&scope, nodes, arena, block);
                block.pop_until(seq_local, span, self.builder);
            }
            ontol_hir::Kind::MakeMatrix(binders, nodes) => {
                // Matrix of arity=1 is supported outside properties
                let Type::Matrix(column_types) = ty else {
                    panic!("not a matrix: {ty:?}");
                };
                if binders.len() != 1 {
                    CompileError::TODO("standalone matrix must have arity=1")
                        .span(span)
                        .report(self.errors);
                    return;
                }
                let val_ty = column_types[0];

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
                let scope = vec![(seq_local, ontol_hir::Binding::Binder(binders[0]))];

                self.gen_in_scope(&scope, nodes, arena, block);
                block.pop_until(seq_local, span, self.builder);
            }
            ontol_hir::Kind::CopySubSeq(target, source) => {
                let Ok(target_local) = self.var_local(*target, &span) else {
                    return;
                };
                let Ok(source_local) = self.var_local(*source, &span) else {
                    return;
                };

                block.op(
                    OpCode::CopySubSeq(target_local, source_local),
                    Delta(0),
                    span,
                    self.builder,
                );
            }
            ontol_hir::Kind::ForEach(elements, nodes) => {
                let Some((first_seq_var, _)) = elements.iter().next() else {
                    return;
                };
                let Ok(seq_local) = self.var_local(*first_seq_var, &meta.span) else {
                    return;
                };
                let counter = block.op(OpCode::I64(0, DefId::unit()), Delta(1), span, self.builder);

                let mut tuple_elements: Vec<(Local, Binding<TypedHir>)> = vec![];
                let mut plus = 1;

                for (_, binding) in elements {
                    tuple_elements.push((self.builder.top_plus(plus), *binding));
                    plus += 1;
                }

                let loop_label = {
                    let mut loop_block = self
                        .builder
                        .new_block(Delta(tuple_elements.len() as i32), span);
                    let loop_label = loop_block.label();
                    let old_scope = self.scope.clone();

                    self.gen_in_scope(&tuple_elements, nodes, arena, &mut loop_block);

                    self.scope = old_scope;

                    loop_block.pop_until(counter, span, self.builder);
                    loop_block.commit(
                        Terminator::PopGoto(block.label(), block.current_offset()),
                        self.builder,
                    );
                    loop_label
                };

                block.ir(
                    Ir::Iter(seq_local, tuple_elements.len() as u8, counter, loop_label),
                    Delta(0),
                    span,
                    self.builder,
                );
                block.pop_until(counter, span, self.builder);
            }
            ontol_hir::Kind::Insert(seq_var, node) => {
                let before = self.builder.top();
                let Ok(seq_local) = self.var_local(*seq_var, &meta.span) else {
                    return;
                };

                self.gen_node(arena.node_ref(*node), block);

                block.op(
                    OpCode::SeqAppendN(seq_local, 1),
                    Delta(-1),
                    span,
                    self.builder,
                );
                block.pop_until(before, span, self.builder);
            }
            ontol_hir::Kind::StringPush(to_var, node) => {
                let before = self.builder.top();
                let Ok(to_local) = self.var_local(*to_var, &span) else {
                    return;
                };
                self.gen_node(arena.node_ref(*node), block);
                block.op(
                    OpCode::AppendString(to_local),
                    Delta(-1),
                    span,
                    self.builder,
                );
                block.pop_until(before, span, self.builder);
            }
            ontol_hir::Kind::LetCondVar(bind_var, cond) => {
                let Ok(cond_local) = self.var_local(*cond, &span) else {
                    return;
                };

                block.op(OpCode::CondVar(cond_local), Delta(1), span, self.builder);

                self.scope_insert(*bind_var, self.builder.top(), &span);
            }
            ontol_hir::Kind::PushCondClauses(cond_var, clauses) => {
                let Ok(cond_local) = self.var_local(*cond_var, &span) else {
                    return;
                };

                let before = self.builder.top();

                for ClausePair(var, clause_op) in clauses {
                    let vm_clause_op = match clause_op {
                        Clause::Root => Clause::Root,
                        Clause::IsDef(def_id) => Clause::IsDef(*def_id),
                        Clause::MatchProp(prop_id, set_operator, set_var) => {
                            let Ok(set_local) = self.var_local(*set_var, &span) else {
                                return;
                            };
                            Clause::MatchProp(*prop_id, *set_operator, set_local)
                        }
                        Clause::Member(rel, val) => {
                            let rel = self.gen_eval_cond_term(rel, arena, span, block);
                            let val = self.gen_eval_cond_term(val, arena, span, block);
                            Clause::Member(rel, val)
                        }
                        Clause::SetPredicate(predicate, term) => {
                            let term = self.gen_eval_cond_term(term, arena, span, block);
                            Clause::SetPredicate(*predicate, term)
                        }
                    };

                    let Ok(clause_local) = self.var_local(*var, &span) else {
                        return;
                    };

                    block.op(
                        OpCode::PushCondClause(cond_local, ClausePair(clause_local, vm_clause_op)),
                        Delta(0),
                        span,
                        self.builder,
                    );
                }

                block.pop_until(before, span, self.builder);
            }
            ontol_hir::Kind::Matrix(..) | ontol_hir::Kind::Regex(..) => {
                unreachable!(
                    "{} is only declarative, not used in code generation",
                    node_ref
                );
            }
        }
    }

    fn func_number_repr_switch(
        &mut self,
        (def_id, span): (DefId, SourceSpan),
        i64: BuiltinProc,
        f64: BuiltinProc,
    ) -> BuiltinProc {
        match self.repr_ctx.get_repr_kind(&def_id) {
            Some(ReprKind::Scalar(_, ReprScalarKind::I64(_), _)) => i64,
            Some(ReprKind::Scalar(_, ReprScalarKind::F64(_), _)) => f64,
            _ => {
                CompileError::BUG("Unable to select instruction")
                    .span(span)
                    .report(self.errors);
                BuiltinProc::NewVoid
            }
        }
    }

    fn gen_eval_cond_term(
        &mut self,
        term: &EvalCondTerm,
        arena: &TypedArena<'m>,
        span: SourceSpan,
        block: &mut Block,
    ) -> OpCodeCondTerm {
        match term {
            EvalCondTerm::Wildcard => OpCodeCondTerm::Wildcard,
            EvalCondTerm::QuoteVar(var) => match self.var_local(*var, &span) {
                Ok(local) => OpCodeCondTerm::CondVar(local),
                Err(_) => OpCodeCondTerm::Wildcard,
            },
            EvalCondTerm::Eval(node) => {
                self.gen_node(arena.node_ref(*node), block);
                let top = self.builder.top();

                OpCodeCondTerm::Value(top)
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
                self.scope_insert(binder.hir().var, *local, &binder.meta().span);
            }
        }

        for node_ref in arena.node_refs(nodes) {
            self.gen_node(node_ref, block);
        }

        for (_, binding) in scopes {
            if let ontol_hir::Binding::Binder(binder) = binding {
                self.scope.remove(&binder.hir().var);
            }
        }
    }

    fn gen_regex_capture_index_union(
        &mut self,
        groups_list: &[ThinVec<ontol_hir::CaptureGroup<'m, TypedHir>>],
        span: SourceSpan,
        define_vars: bool,
    ) -> BitSet {
        let mut capture_index_union = BitSet::default();
        let mut groups_by_index_ordered: BTreeMap<u32, VarSet> = Default::default();

        for groups in groups_list {
            for group in groups {
                capture_index_union.insert(group.index as usize);
                groups_by_index_ordered
                    .entry(group.index)
                    .or_default()
                    .insert(group.binder.hir().var);
            }
        }

        if define_vars {
            // assign consecutive locals to each (potentially overlapping) variable
            for (offset, (_, var_set)) in groups_by_index_ordered.iter().enumerate() {
                for var in var_set {
                    let mut local = self.builder.top_plus(2);
                    local.0 += offset as u16;
                    self.scope_insert(var, local, &span);
                }
            }
        }

        capture_index_union
    }

    fn scope_insert(&mut self, var: Var, local: Local, span: &SourceSpan) {
        if self.scope.insert(var, local).is_some() {
            debug!("Error: {var} already in scope");
            assert!(
                !span.is_native(),
                "var {var} was already in scope, but span is native. scope={:?}",
                self.scope
            );
            CompileError::BUG("Variable already in scope")
                .span(*span)
                .report(self.errors);
        }
    }

    fn var_local(&mut self, var: Var, span: &SourceSpan) -> Result<Local, ()> {
        match self.scope.get(&var) {
            Some(local) => Ok(*local),
            None => {
                debug!("Error: {var} not in scope");
                assert!(
                    !span.is_native(),
                    "var {var} was not in scope, but span is native."
                );
                CompileError::BUG("Variable not in scope")
                    .span(*span)
                    .report(self.errors);
                Err(())
            }
        }
    }

    fn catch_dest(&mut self, hir_label: ontol_hir::Label, span: &SourceSpan) -> BlockLabel {
        let Some(fail_label) = self.catch_points.get(&hir_label).cloned() else {
            CompileError::TODO("catch block not found")
                .span(*span)
                .report(self.errors);
            return BlockLabel(Var(0));
        };
        fail_label
    }
}

use fnv::FnvHashMap;
use ontol_hir::{GetKind, HasDefault, PropPattern, PropVariant};
use ontol_runtime::{
    smart_format,
    value::PropertyId,
    vm::proc::{BuiltinProc, Local, NParams, PatternCaptureGroup, Predicate, Procedure},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{data_flow_analyzer::DataFlowAnalyzer, ir::Terminator, proc_builder::Delta},
    error::CompileError,
    typed_hir::{HirFunc, TypedHir, TypedHirNode},
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
    expr: TypedHirNode<'m>,
    def_id: DefId,
    compiler: &Compiler<'m>,
) -> CompileErrors {
    let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs, &compiler.seal_ctx);
    let mut errors = CompileErrors::default();

    debug!("Generating code for\n{}", expr);

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Delta(0), expr.span());
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors: &mut errors,
        type_mapper,
    };
    generator.gen_node(expr, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

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

    let data_flow = DataFlowAnalyzer::new(&compiler.defs).analyze(func.arg.var, &func.body);
    let mut errors = CompileErrors::default();

    let return_ty = func.body.ty();

    let mut builder = ProcBuilder::new(NParams(0));
    let mut root_block = builder.new_block(Delta(1), func.body.span());
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors: &mut errors,
        type_mapper,
    };
    generator.scope.insert(func.arg.var, Local(0));
    generator.gen_node(func.body, &mut root_block);
    builder.commit(root_block, Terminator::Return(builder.top()));

    match (
        type_mapper.find_domain_mapping_info(func.arg.meta.ty),
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
    pub type_mapper: TypeMapper<'a, 'm>,

    scope: FnvHashMap<ontol_hir::Var, Local>,
}

impl<'a, 'm> CodeGenerator<'a, 'm> {
    fn gen_node(&mut self, TypedHirNode(kind, meta): TypedHirNode<'m>, block: &mut Block) {
        let ty = meta.ty;
        let span = meta.span;
        match kind {
            ontol_hir::Kind::Var(var) => {
                let Some(local) = self.scope.get(&var) else {
                    return self.errors.error(CompileError::UnboundVariable, &span);
                };

                self.builder
                    .append(block, Ir::Clone(*local), Delta(1), span);
            }
            ontol_hir::Kind::Unit => {
                self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Delta(1),
                    span,
                );
            }
            ontol_hir::Kind::I64(int) => {
                self.builder.append(
                    block,
                    Ir::I64(int, ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                );
            }
            ontol_hir::Kind::F64(float) => {
                self.builder.append(
                    block,
                    Ir::F64(float, ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                );
            }
            ontol_hir::Kind::String(string) => {
                self.builder.append(
                    block,
                    Ir::String(string, ty.get_single_def_id().unwrap()),
                    Delta(1),
                    span,
                );
            }
            ontol_hir::Kind::Const(const_def_id) => {
                let proc = Procedure {
                    address: self.proc_table.gen_const_addr(const_def_id),
                    n_params: NParams(0),
                };
                self.builder.append(block, Ir::Call(proc), Delta(1), span);
            }
            ontol_hir::Kind::Let(binder, definition, body) => {
                self.gen_node(*definition, block);
                self.scope.insert(binder.var, self.builder.top());
                for node in body {
                    self.gen_node(node, block);
                }
                self.scope.remove(&binder.var);
            }
            ontol_hir::Kind::Call(proc, params) => {
                let stack_delta = Delta(-(params.len() as i32) + 1);
                for param in params {
                    self.gen_node(param, block);
                }
                let return_def_id = ty.get_single_def_id().unwrap();
                self.builder.append(
                    block,
                    Ir::CallBuiltin(proc, return_def_id),
                    stack_delta,
                    span,
                );
            }
            ontol_hir::Kind::Map(param) => {
                let param_ty = param.ty();
                match (
                    self.type_mapper.find_domain_mapping_info(param_ty),
                    self.type_mapper.find_domain_mapping_info(ty),
                ) {
                    (Some(from), Some(to)) => {
                        self.gen_node(*param, block);

                        match (from.punned, to.punned) {
                            (Some(from_pun), Some(to_pun)) if from.anonymous && to.anonymous => {
                                if from_pun == to_pun {
                                    self.gen_pun(block, to.key.def_id, span);
                                } else {
                                    let proc = Procedure {
                                        address: self.proc_table.gen_mapping_addr(from_pun, to_pun),
                                        n_params: NParams(1),
                                    };

                                    self.builder.append(block, Ir::Call(proc), Delta(0), span);
                                }
                            }
                            _ => {
                                let proc = Procedure {
                                    address: self.proc_table.gen_mapping_addr(from.key, to.key),
                                    n_params: NParams(1),
                                };

                                self.builder.append(block, Ir::Call(proc), Delta(0), span);
                            }
                        }
                    }
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
                let local = self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewStruct, def_id),
                    Delta(1),
                    span,
                );
                self.scope.insert(binder.var, local);
                for node in nodes {
                    self.gen_node(node, block);
                    self.builder.append_pop_until(block, local, span);
                }
                self.scope.remove(&binder.var);
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, variants) => {
                if let Some(variant) = variants.into_iter().next() {
                    match variant {
                        PropVariant::Singleton(attr) => self
                            .gen_attribute(struct_var, prop_id, *attr.rel, *attr.val, span, block),
                        PropVariant::Seq(seq_variant) => {
                            if let Some(element) = seq_variant.elements.into_iter().next() {
                                self.gen_attribute(
                                    struct_var,
                                    prop_id,
                                    element.attribute.rel,
                                    element.attribute.val,
                                    span,
                                    block,
                                );
                            }
                        }
                    }
                }
            }
            ontol_hir::Kind::MatchProp(struct_var, id, arms) => {
                let struct_local = self.var_local(struct_var);

                if arms.len() > 1 {
                    if arms
                        .iter()
                        .any(|arm| matches!(arm.pattern, ontol_hir::PropPattern::Absent))
                    {
                        self.builder.append(
                            block,
                            Ir::TryTakeAttr2(struct_local, id),
                            // even if three locals are pushed, the `status` one
                            // is not kept track of here.
                            Delta(2),
                            span,
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
                                if !matches!(arm.pattern, ontol_hir::PropPattern::Absent) {
                                    self.gen_match_arm(
                                        arm,
                                        (rel_local, val_local),
                                        &mut present_block,
                                    );
                                }
                            }

                            self.builder.commit(
                                present_block,
                                Terminator::PopGoto(block.index(), post_cond_offset),
                            )
                        };

                        self.builder.append(
                            block,
                            Ir::Cond(Predicate::YankTrue(status_local), present_body_index),
                            Delta(0),
                            span,
                        );
                    } else {
                        todo!();
                    }
                } else {
                    let arm = arms.into_iter().next().unwrap();

                    match &arm.pattern {
                        PropPattern::Seq(ontol_hir::Binding::Binder(binder), HasDefault(true)) => {
                            self.builder.append(
                                block,
                                Ir::TryTakeAttr2(struct_local, id),
                                Delta(2),
                                span,
                            );

                            let status_local = self.builder.top_minus(1);
                            let post_cond_offset = block.current_offset().plus(1);

                            // These overlap with the status_local, but that will be yanked(!) in the Cond opcode,
                            // leading into the present block.
                            let val_local = self.builder.top();
                            let rel_local = self.builder.top_minus(1);

                            let Type::Seq(_, seq_item_ty) = binder.meta.ty else {
                                panic!("Not a sequence");
                            };

                            // Code for generating the default values:
                            let default_fallback_body_index = {
                                let mut default_block = self.builder.new_block(Delta(0), span);

                                // rel_params (unit)
                                self.builder.append(
                                    &mut default_block,
                                    Ir::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                                    Delta(0),
                                    NO_SPAN,
                                );
                                // empty sequence
                                self.builder.append(
                                    &mut default_block,
                                    Ir::CallBuiltin(
                                        BuiltinProc::NewSeq,
                                        seq_item_ty.get_single_def_id().unwrap(),
                                    ),
                                    Delta(0),
                                    NO_SPAN,
                                );

                                self.builder.commit(
                                    default_block,
                                    Terminator::PopGoto(block.index(), post_cond_offset),
                                )
                            };

                            // If the TryTakeAttr2 was false, run the default body
                            self.builder.append(
                                block,
                                Ir::Cond(
                                    Predicate::YankFalse(status_local),
                                    default_fallback_body_index,
                                ),
                                Delta(0),
                                span,
                            );

                            self.gen_match_arm(arm, (rel_local, val_local), block);
                        }
                        _ => {
                            self.builder.append(
                                block,
                                Ir::TakeAttr2(struct_local, id),
                                Delta(2),
                                span,
                            );

                            let val_local = self.builder.top();
                            let rel_local = self.builder.top_minus(1);

                            self.gen_match_arm(arm, (rel_local, val_local), block);
                        }
                    }
                }
            }
            ontol_hir::Kind::Sequence(binder, nodes) => {
                let Type::Seq(_, val_ty) = ty else {
                    panic!("Not a sequence");
                };
                let seq_local = self.builder.append(
                    block,
                    Ir::CallBuiltin(
                        BuiltinProc::NewSeq,
                        val_ty
                            .get_single_def_id()
                            .unwrap_or_else(|| panic!("val_ty: {val_ty:?}")),
                    ),
                    Delta(1),
                    span,
                );
                self.gen_in_scope(
                    &[(seq_local, ontol_hir::Binding::Binder(binder))],
                    nodes.into_iter(),
                    block,
                );
                self.builder.append_pop_until(block, seq_local, span);
            }
            ontol_hir::Kind::ForEach(seq_var, (rel_binding, val_binding), nodes) => {
                let seq_local = self.var_local(seq_var);
                let counter = self
                    .builder
                    .append(block, Ir::I64(0, DefId::unit()), Delta(1), span);

                let iter_offset = block.current_offset();
                let elem_rel_local = self.builder.top_plus(1);
                let elem_val_local = self.builder.top_plus(2);

                let for_each_body_index = {
                    let mut iter_block = self.builder.new_block(Delta(2), span);

                    self.gen_in_scope(
                        &[(elem_rel_local, rel_binding), (elem_val_local, val_binding)],
                        nodes.into_iter(),
                        &mut iter_block,
                    );

                    self.builder
                        .append_pop_until(&mut iter_block, counter, span);

                    self.builder
                        .commit(iter_block, Terminator::PopGoto(block.index(), iter_offset))
                };

                self.builder.append(
                    block,
                    Ir::Iter(seq_local, counter, for_each_body_index),
                    Delta(0),
                    span,
                );

                self.builder.append_pop_until(block, counter, span);
            }
            ontol_hir::Kind::SeqPush(seq_var, attr) => {
                let top = self.builder.top();
                let seq_local = self.var_local(seq_var);
                self.gen_node(*attr.rel, block);
                let rel_local = self.builder.top();

                self.gen_node(*attr.val, block);

                self.builder
                    .append(block, Ir::Clone(rel_local), Delta(1), span);

                self.builder
                    .append(block, Ir::AppendAttr2(seq_local), Delta(-2), span);

                self.builder.append_pop_until(block, top, span);
            }
            ontol_hir::Kind::StringPush(to_var, node) => {
                let top = self.builder.top();
                let to_local = self.var_local(to_var);
                self.gen_node(*node, block);
                self.builder
                    .append(block, Ir::AppendString(to_local), Delta(-1), span);
                self.builder.append_pop_until(block, top, span);
            }
            ontol_hir::Kind::MatchRegex(haystack_var, regex_def_id, capture_groups, nodes) => {
                let haystack_local = self.var_local(haystack_var);

                let vm_capture_groups: Box<[_]> = capture_groups
                    .iter()
                    .map(|capture_group| PatternCaptureGroup {
                        group_index: capture_group.index,
                        type_def_id: capture_group
                            .binder
                            .meta
                            .ty
                            .get_single_def_id()
                            .expect("Unable to get type of capture group"),
                    })
                    .collect();

                let top = self.builder.top();

                self.builder.append(
                    block,
                    Ir::RegexCapture(haystack_local, regex_def_id, vm_capture_groups),
                    Delta((capture_groups.len() + 1) as i32),
                    span,
                );
                // unconditional match (for now)
                self.builder.append(block, Ir::AssertTrue, Delta(-1), span);

                {
                    // define scope for capture
                    for (index, capture_group) in capture_groups.iter().enumerate() {
                        let local = Local(top.0 + index as u16 + 1);
                        if self.scope.insert(capture_group.binder.var, local).is_some() {
                            panic!("Variable {} already in scope", capture_group.binder.var);
                        }
                    }

                    for node in nodes {
                        self.gen_node(node, block);
                    }

                    // pop scope
                    for capture_group in capture_groups {
                        self.scope.remove(&capture_group.binder.var);
                    }
                }
            }
            ontol_hir::Kind::DeclSeq(..) | ontol_hir::Kind::Regex(..) => {
                unreachable!("decl-seq is only declarative, not used in code generation");
            }
        }
    }

    fn gen_match_arm(
        &mut self,
        arm: ontol_hir::MatchArm<'m, TypedHir>,
        (rel_local, val_local): (Local, Local),
        block: &mut Block,
    ) {
        match arm.pattern {
            ontol_hir::PropPattern::Attr(rel_binding, val_binding) => self.gen_in_scope(
                &[(rel_local, rel_binding), (val_local, val_binding)],
                arm.nodes.into_iter(),
                block,
            ),
            ontol_hir::PropPattern::Seq(binding, _has_default) => self.gen_in_scope(
                &[
                    (rel_local, ontol_hir::Binding::Wildcard),
                    (val_local, binding),
                ],
                arm.nodes.into_iter(),
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
        rel: TypedHirNode<'m>,
        val: TypedHirNode<'m>,
        span: SourceSpan,
        block: &mut Block,
    ) {
        let struct_local = self.var_local(struct_var);

        match rel.kind() {
            ontol_hir::Kind::Unit => {
                self.gen_node(val, block);
                self.builder
                    .append(block, Ir::PutAttr1(struct_local, prop_id), Delta(-1), span);
            }
            _ => {
                self.gen_node(rel, block);
                let rel_local = self.builder.top();
                self.gen_node(val, block);

                self.builder
                    .append(block, Ir::Clone(rel_local), Delta(1), span);

                self.builder
                    .append(block, Ir::PutAttr2(struct_local, prop_id), Delta(-2), span);
            }
        }
    }

    fn gen_in_scope(
        &mut self,
        scopes: &[(Local, ontol_hir::Binding<TypedHir>)],
        nodes: impl Iterator<Item = TypedHirNode<'m>>,
        block: &mut Block,
    ) {
        for (local, binding) in scopes {
            if let ontol_hir::Binding::Binder(binder) = binding {
                if self.scope.insert(binder.var, *local).is_some() {
                    panic!("Variable {} already in scope", binder.var);
                }
            }
        }

        for node in nodes {
            self.gen_node(node, block);
        }

        for (_, binding) in scopes {
            if let ontol_hir::Binding::Binder(binder) = binding {
                self.scope.remove(&binder.var);
            }
        }
    }

    fn var_local(&self, var: ontol_hir::Var) -> Local {
        *self
            .scope
            .get(&var)
            .unwrap_or_else(|| panic!("Variable {var} not in scope"))
    }

    fn gen_pun(&mut self, block: &mut Block, def_id: DefId, span: SourceSpan) {
        let local = self.builder.top();
        self.builder
            .append(block, Ir::TypePun(local, def_id), Delta(0), span);
    }

    fn report_not_mappable(&mut self, span: SourceSpan) {
        self.errors.error(
            CompileError::TODO(smart_format!("type not mappable")),
            &span,
        );
    }
}

use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, BuiltinProc, Lib, NParams, OpCode, Procedure},
    DefId,
};

mod equation;
mod equation_solver;
mod ir;
mod link;
mod map_obj;
mod proc_builder;
mod translate;
mod value_obj;

use tracing::{debug, warn};

use crate::{
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

use self::{
    equation::TypedExprEquation,
    ir::{Ir, Terminator},
    link::{link, LinkResult},
    proc_builder::{Block, ProcBuilder},
    translate::{codegen_translate_solve, DebugDirection},
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_translations: FnvHashMap<(DefId, DefId), Procedure>,
}

impl<'m> Debug for CodegenTasks<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodegenTasks")
            .field("tasks", &self.tasks)
            .finish()
    }
}

impl<'m> CodegenTasks<'m> {
    pub fn push(&mut self, task: CodegenTask<'m>) {
        self.tasks.push(task);
    }
}

#[derive(Debug)]
pub enum CodegenTask<'m> {
    Map(MapCodegenTask<'m>),
}

#[derive(Debug)]
pub struct MapCodegenTask<'m> {
    pub expressions: TypedExprTable<'m>,
    pub node_a: ExprRef,
    pub node_b: ExprRef,
    pub span: SourceSpan,
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub procedures: FnvHashMap<(DefId, DefId), ProcBuilder>,
    pub translate_calls: Vec<TranslateCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a translate call.
    /// This will be resolved to final "physical" ID in the link phase.
    fn gen_translate_addr(&mut self, from: DefId, to: DefId) -> Address {
        let address = Address(self.translate_calls.len() as u32);
        self.translate_calls.push(TranslateCall {
            translation: (from, to),
        });
        address
    }
}

pub(super) struct TranslateCall {
    pub translation: (DefId, DefId),
}

trait Codegen {
    fn codegen_variable(
        &mut self,
        builder: &mut ProcBuilder,
        block: &mut Block,
        var: SyntaxVar,
        span: &SourceSpan,
    );

    fn codegen_expr(
        &mut self,
        proc_table: &mut ProcTable,
        builder: &mut ProcBuilder,
        block: &mut Block,
        equation: &TypedExprEquation,
        expr_id: ExprRef,
    ) {
        let (_, expr, span) = equation.resolve_expr(&equation.expansions, expr_id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                for param in params.iter() {
                    self.codegen_expr(proc_table, builder, block, equation, *param);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                // New
                builder.ir_push(1, Ir::CallBuiltin(*proc, return_def_id), span, block);

                // Old
                builder.push_stack_old(1, (OpCode::CallBuiltin(*proc, return_def_id), span), block);
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();

                // New
                builder.ir_push(1, Ir::Constant(*k, return_def_id), span, block);

                // Old
                builder.push_stack_old(1, (OpCode::PushConstant(*k, return_def_id), span), block);
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(builder, block, *var, &span);
            }
            TypedExprKind::VariableRef(_) => panic!(),
            TypedExprKind::Translate(param_id, from_ty) => {
                self.codegen_expr(proc_table, builder, block, equation, *param_id);

                debug!(
                    "translate from {from_ty:?} to {:?}, span = {span:?}",
                    expr.ty
                );
                let from = find_translation_key(from_ty).unwrap();
                let to = find_translation_key(&expr.ty).unwrap();

                let proc = Procedure {
                    address: proc_table.gen_translate_addr(from, to),
                    n_params: NParams(1),
                };

                // New
                builder.ir_push(0, Ir::Call(proc), span, block);

                // Old
                block.opcodes.push((OpCode::Call(proc), span));
            }
            TypedExprKind::SequenceMap(expr_ref, _iter_var, body, _) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let output_seq = builder.ir_push(
                    1,
                    Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                    span,
                    block,
                );

                self.codegen_expr(proc_table, builder, block, equation, *expr_ref);
                let input_seq = builder.top();

                let iterator = builder.ir_push(1, Ir::Constant(0, DefId::unit()), span, block);

                let for_each_offset = block.ir.len();

                let for_each_body_index = {
                    // inside the for-each body there are two items on the stack, value (top), then rel_params
                    builder.depth += 2;

                    let mut map_block = builder
                        .new_block(Terminator::Goto(block.index, for_each_offset as u32), span);

                    self.codegen_expr(proc_table, builder, &mut map_block, equation, *body);

                    // still two items on the stack: append to original sequence
                    // for now, rel_params are untranslated
                    builder.ir_pop(2, Ir::AppendAttr(output_seq), span, &mut map_block);

                    builder.commit(map_block)
                };

                builder.ir_pop(
                    0,
                    Ir::Iter(input_seq, iterator, for_each_body_index),
                    span,
                    block,
                );
                builder.ir_pop(1, Ir::Remove(iterator), span, block);

                // Old
                /*
                {
                    let return_def_id = expr.ty.get_single_def_id().unwrap();
                    let input_seq = Local(builder.stack_size - 1);
                    let output_seq =
                        builder.push_stack(1, (OpCode::PushSequence(return_def_id), span), block);
                    let iterator = builder.push_stack(
                        1,
                        (OpCode::PushConstant(0, DefId::unit()), span),
                        block,
                    );

                    let for_each_offset = block.opcodes.len();

                    let for_each_body_index = {
                        // inside the for-each body there are two items on the stack, value (top), then rel_params
                        builder.stack_size += 2;

                        let mut map_block = builder.new_block(
                            Terminator::Goto(block.index, for_each_offset as u32),
                            span,
                            BlockKind::Op,
                        );

                        let index = map_block.index;
                        self.codegen_expr(proc_table, builder, &mut map_block, equation, *expr_ref);

                        // still two items on the stack: append to original sequence
                        // for now, rel_params are untranslated
                        builder.pop_stack(
                            2,
                            (OpCode::AppendAttr(output_seq), span),
                            &mut map_block,
                        );

                        builder.commit(map_block);
                        index
                    };

                    block.opcodes.push((
                        OpCode::ForEach(input_seq, iterator, AddressOffset(for_each_body_index.0)),
                        span,
                    ));

                    builder.pop_stack(1, (OpCode::Remove(iterator), span), block);
                }
                */
            }
            TypedExprKind::ValueObjPattern(_) => {
                todo!()
            }
            TypedExprKind::MapObjPattern(_) => {
                todo!()
            }
            TypedExprKind::Unit => {
                todo!()
            }
        }
    }
}

fn find_translation_key(ty: &TypeRef) -> Option<DefId> {
    match ty {
        Type::Domain(def_id) => Some(*def_id),
        other => {
            warn!("unable to get translation key: {other:?}");
            None
        }
    }
}

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let tasks = std::mem::take(&mut compiler.codegen_tasks.tasks);

    let mut proc_table = ProcTable::default();

    for task in tasks {
        match task {
            CodegenTask::Map(map_task) => {
                let mut equation = TypedExprEquation::new(map_task.expressions);

                for (index, expr) in equation.expressions.0.iter().enumerate() {
                    debug!("{{{index}}}: {expr:?}");
                }

                debug!(
                    "equation before solve:\n left: {:#?}\nright: {:#?}",
                    equation.debug_tree(map_task.node_a, &equation.reductions),
                    equation.debug_tree(map_task.node_b, &equation.expansions),
                );

                // a -> b
                codegen_translate_solve(
                    &mut proc_table,
                    &mut equation,
                    (map_task.node_a, map_task.node_b),
                    DebugDirection::Forward,
                );

                equation.reset();

                // b -> a
                /*
                codegen_translate_solve(
                    &mut proc_table,
                    &mut equation,
                    (map_task.node_b, map_task.node_a),
                    DebugDirection::Backward,
                );
                */
            }
        }
    }

    let LinkResult { lib, translations } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_translations = translations;
}

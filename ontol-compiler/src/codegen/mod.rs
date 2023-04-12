use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, AddressOffset, Lib, Local, NParams, OpCode, Procedure},
    DefId,
};

mod equation;
mod equation_solver;
mod link;
mod map_obj;
mod translate;
mod value_obj;

use smallvec::{smallvec, SmallVec};
use tracing::{debug, warn};

use crate::{
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

use self::{
    equation::TypedExprEquation,
    link::{link, LinkResult},
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
    pub procedures: FnvHashMap<(DefId, DefId), UnlinkedProc>,
    pub translate_calls: Vec<TranslateCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a translate call.
    /// This will be resolved to final "physical" ID in the link phase.
    fn gen_translate_call(&mut self, from: DefId, to: DefId) -> OpCode {
        let address = Address(self.translate_calls.len() as u32);
        self.translate_calls.push(TranslateCall {
            translation: (from, to),
        });
        OpCode::Call(Procedure {
            address,
            n_params: NParams(1),
        })
    }
}

pub(super) struct TranslateCall {
    pub translation: (DefId, DefId),
}

pub struct ProcBuilder {
    pub n_params: NParams,
    pub blocks: SmallVec<[Block; 8]>,
    pub stack_size: u32,
}

impl ProcBuilder {
    pub fn new(n_params: NParams) -> Self {
        Self {
            n_params,
            blocks: Default::default(),
            stack_size: n_params.0 as u32,
        }
    }

    pub fn new_block(&mut self, terminator: Terminator, span: SourceSpan) -> Block {
        let address = self.blocks.len() as u32;
        self.blocks.push(Block {
            index: address,
            opcodes: Default::default(),
            terminator: terminator.clone(),
            terminator_span: span,
        });
        Block {
            index: address,
            opcodes: Default::default(),
            terminator,
            terminator_span: span,
        }
    }

    pub fn commit(&mut self, block: Block) {
        let index = block.index;
        self.blocks[index as usize].opcodes = block.opcodes;
    }

    pub fn push_stack(
        &mut self,
        n: u32,
        spanned_opcode: (OpCode, SourceSpan),
        block: &mut Block,
    ) -> Local {
        let local = self.stack_size;
        self.stack_size += n;
        block.opcodes.push(spanned_opcode);
        Local(local)
    }

    pub fn pop_stack(&mut self, n: u32, spanned_opcode: (OpCode, SourceSpan), block: &mut Block) {
        self.stack_size -= n;
        block.opcodes.push(spanned_opcode);
    }

    pub fn build(mut self) -> SpannedOpCodes {
        let mut block_addresses: SmallVec<[u32; 8]> = smallvec![];

        // compute addresses
        let mut block_addr = 0;
        for block in &self.blocks {
            block_addresses.push(block_addr);
            block_addr += block.opcodes.len() as u32;
            // account for the terminator:
            block_addr += 1;
        }

        // update addresses
        for block in &mut self.blocks {
            for (opcode, _) in &mut block.opcodes {
                // Important: Handle all opcodes with AddressOffset
                match opcode {
                    OpCode::Goto(addr_offset) => {
                        addr_offset.0 = block_addresses[addr_offset.0 as usize];
                    }
                    OpCode::ForEach(_, _, addr_offset) => {
                        addr_offset.0 = block_addresses[addr_offset.0 as usize];
                    }
                    _ => {}
                }
            }
        }

        let mut output = smallvec![];
        for block in self.blocks {
            for spanned_opcode in block.opcodes {
                output.push(spanned_opcode);
            }

            let span = block.terminator_span;
            match block.terminator {
                Terminator::Return(Local(0)) => output.push((OpCode::Return0, span)),
                Terminator::Return(local) => output.push((OpCode::Return(local), span)),
                Terminator::Goto { block, offset } => output.push((
                    OpCode::Goto(AddressOffset(block_addresses[block as usize] + offset)),
                    span,
                )),
            }
        }

        debug!(
            "Built proc: {:?} {:#?}",
            self.n_params,
            output
                .iter()
                .enumerate()
                .map(|(index, (opcode, _))| format!("{index}: {opcode:?}"))
                .collect::<Vec<_>>()
        );

        output
    }
}

#[derive(Clone)]
pub enum Terminator {
    Return(Local),
    Goto { block: u32, offset: u32 },
}

pub struct Block {
    pub index: u32,
    pub opcodes: SmallVec<[(OpCode, SourceSpan); 32]>,
    pub terminator: Terminator,
    pub terminator_span: SourceSpan,
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;

/// Procedure that has been generated but not linked.
/// i.e. OpCode::Call has incorrect parameters, and
/// we need to look up a table to resolve that in the link phase.
///
/// FIXME: There is difference between this and ProcBuilder
pub(super) struct UnlinkedProc {
    pub n_params: NParams,
    pub builder: ProcBuilder,
}

impl UnlinkedProc {
    pub fn new(builder: ProcBuilder) -> Self {
        Self {
            n_params: builder.n_params,
            builder,
        }
    }
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

                builder.push_stack(1, (OpCode::CallBuiltin(*proc, return_def_id), span), block);
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                builder.push_stack(1, (OpCode::PushConstant(*k, return_def_id), span), block);
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
                block
                    .opcodes
                    .push((proc_table.gen_translate_call(from, to), span));
            }
            TypedExprKind::SequenceMap(expr_ref, _) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let input_seq = Local(builder.stack_size - 1);
                let output_seq =
                    builder.push_stack(1, (OpCode::PushSequence(return_def_id), span), block);
                let iterator =
                    builder.push_stack(1, (OpCode::PushConstant(0, DefId::unit()), span), block);

                let for_each_offset = block.opcodes.len();

                let map_item_index = {
                    // inside the for-each body there are two items on the stack, value (top), then rel_params
                    builder.stack_size += 2;

                    let mut map_block = builder.new_block(
                        Terminator::Goto {
                            block: block.index,
                            offset: for_each_offset as u32,
                        },
                        span,
                    );
                    let index = map_block.index;
                    self.codegen_expr(proc_table, builder, &mut map_block, equation, *expr_ref);

                    // still two items on the stack: append to original sequence
                    // for now, rel_params are untranslated
                    builder.pop_stack(2, (OpCode::AppendAttr(output_seq), span), &mut map_block);

                    builder.commit(map_block);
                    index
                };

                block.opcodes.push((
                    OpCode::ForEach(input_seq, iterator, AddressOffset(map_item_index)),
                    span,
                ));
                builder.pop_stack(1, (OpCode::Remove(iterator), span), block);
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
                    "equation before solve: left: {:#?} right: {:#?}",
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
                codegen_translate_solve(
                    &mut proc_table,
                    &mut equation,
                    (map_task.node_b, map_task.node_a),
                    DebugDirection::Backward,
                );
            }
        }
    }

    let LinkResult { lib, translations } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_translations = translations;
}

use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, Lib, NParams, OpCode, Procedure},
    DefId,
};

mod equation;
mod equation_solver;
mod link;
mod map_obj;
mod translate;
mod value_obj;

use smallvec::SmallVec;
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

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;

/// Procedure that has been generated but not linked.
/// i.e. OpCode::Call has incorrect parameters, and
/// we need to look up a table to resolve that in the link phase.
pub(super) struct UnlinkedProc {
    pub n_params: NParams,
    pub opcodes: SpannedOpCodes,
}

trait Codegen {
    fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut SpannedOpCodes, span: &SourceSpan);

    fn codegen_expr(
        &mut self,
        proc_table: &mut ProcTable,
        equation: &TypedExprEquation,
        expr_id: ExprRef,
        opcodes: &mut SpannedOpCodes,
    ) {
        let (_, expr, span) = equation.resolve_expr(&equation.expansions, expr_id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                for param in params.iter() {
                    self.codegen_expr(proc_table, equation, *param, opcodes);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                opcodes.push((OpCode::CallBuiltin(*proc, return_def_id), span));
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                opcodes.push((OpCode::PushConstant(*k, return_def_id), span));
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(*var, opcodes, &span);
            }
            TypedExprKind::VariableRef(_) => panic!(),
            TypedExprKind::Translate(param_id, from_ty) => {
                self.codegen_expr(proc_table, equation, *param_id, opcodes);

                debug!(
                    "translate from {from_ty:?} to {:?}, span = {span:?}",
                    expr.ty
                );
                let from = find_translation_key(from_ty).unwrap();
                let to = find_translation_key(&expr.ty).unwrap();
                opcodes.push((proc_table.gen_translate_call(from, to), span));
            }
            TypedExprKind::SequenceMap(..) => {
                debug!("array type: {:?}", expr.ty);
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                opcodes.push((OpCode::PushSequence(return_def_id), span));
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

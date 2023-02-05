use std::{collections::HashMap, fmt::Debug};

use ontol_runtime::{
    proc::{Lib, NParams, OpCode, Procedure},
    DefId,
};

pub mod rewrite;

mod codegen;
mod link;
mod map_obj;
mod value_obj;

pub use codegen::execute_codegen_tasks;
use tracing::{debug, warn};

use crate::{
    typed_expr::{ExprRef, SealedTypedExprTable, SyntaxVar, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
    SourceSpan,
};

use self::codegen::SpannedOpCodes;

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_translations: HashMap<(DefId, DefId), Procedure>,
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
    Eq(EqCodegenTask<'m>),
}

#[derive(Debug)]
pub struct EqCodegenTask<'m> {
    pub typed_expr_table: SealedTypedExprTable<'m>,
    pub node_a: ExprRef,
    pub node_b: ExprRef,
    pub span: SourceSpan,
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub procedures: HashMap<(DefId, DefId), UnlinkedProc>,
    pub translate_calls: Vec<TranslateCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure id for a translate call.
    /// This will be resolved to final "physical" ID in the link phase.
    fn gen_translate_call(&mut self, from: DefId, to: DefId) -> OpCode {
        let id = self.translate_calls.len() as u32;
        self.translate_calls.push(TranslateCall {
            translation: (from, to),
        });
        OpCode::Call(Procedure {
            start: id,
            n_params: NParams(1),
        })
    }
}

pub(super) struct TranslateCall {
    pub translation: (DefId, DefId),
}

/// Procedure that has been generated but not linked.
/// i.e. OpCode::Call has incorrect parameters, and
/// we need to look up a table to resolve that in the link phase.
pub(super) struct UnlinkedProc {
    pub n_params: NParams,
    pub opcodes: SpannedOpCodes,
}

trait Codegen {
    fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut SpannedOpCodes, span: &SourceSpan);

    fn codegen_expr<'m>(
        &mut self,
        proc_table: &mut ProcTable,
        expr_table: &TypedExprTable<'m>,
        expr_id: ExprRef,
        opcodes: &mut SpannedOpCodes,
    ) {
        let (_, expr, span) = expr_table.resolve_expr(&expr_table.target_rewrites, expr_id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                for param in params.iter() {
                    self.codegen_expr(proc_table, expr_table, *param, opcodes);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                opcodes.push((OpCode::CallBuiltin(*proc, return_def_id), span));
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                opcodes.push((OpCode::Constant(*k, return_def_id), span));
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(*var, opcodes, &span);
            }
            TypedExprKind::VariableRef(_) => panic!(),
            TypedExprKind::Translate(param_id, from_ty) => {
                self.codegen_expr(proc_table, expr_table, *param_id, opcodes);

                debug!(
                    "translate from {from_ty:?} to {:?}, span = {span:?}",
                    expr.ty
                );
                let from = find_translation_key(from_ty).unwrap();
                let to = find_translation_key(&expr.ty).unwrap();
                opcodes.push((proc_table.gen_translate_call(from, to), span));
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

use std::collections::HashMap;

use ontol_runtime::{proc::OpCode, DefId};
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::{
    codegen::map_obj::codegen_map_obj_origin,
    compiler::Compiler,
    typed_expr::{ExprRef, SealedTypedExprTable, SyntaxVar, TypedExprKind, TypedExprTable},
    SourceSpan,
};

use super::{
    find_translation_key,
    link::{link, LinkResult},
    value_obj::codegen_value_obj_origin,
    CodegenTask, ProcTable, UnlinkedProc,
};

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let tasks = std::mem::take(&mut compiler.codegen_tasks.tasks);

    let mut proc_table = ProcTable::default();

    for task in tasks {
        match task {
            CodegenTask::Eq(mut eq_task) => {
                // a -> b
                codegen_translate_rewrite(
                    &mut proc_table,
                    &mut eq_task.typed_expr_table,
                    (eq_task.node_a, eq_task.node_b),
                    DebugDirection::Forward,
                );

                eq_task.typed_expr_table.reset();

                // b -> a
                codegen_translate_rewrite(
                    &mut proc_table,
                    &mut eq_task.typed_expr_table,
                    (eq_task.node_b, eq_task.node_a),
                    DebugDirection::Backward,
                );
            }
        }
    }

    let LinkResult { lib, translations } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_translations = translations;
}

enum DebugDirection {
    Forward,
    Backward,
}

fn codegen_translate_rewrite(
    proc_table: &mut ProcTable,
    table: &mut SealedTypedExprTable,
    (from, to): (ExprRef, ExprRef),
    direction: DebugDirection,
) -> bool {
    // perform rewrite
    let mut rewriter = table.inner.rewriter();
    rewriter.rewrite_expr(from).unwrap_or_else(|error| {
        panic!("TODO: could not rewrite: {error:?}");
    });

    let from_key = find_translation_key(&table.inner.expressions[from].ty);
    let to_key = find_translation_key(&table.inner.expressions[to].ty);

    match (from_key, to_key) {
        (Some(from_key), Some(to_key)) => {
            let procedure =
                codegen_translate(proc_table, to_key, &table.inner, (from, to), direction);

            proc_table.procedures.insert((from_key, to_key), procedure);
            true
        }
        other => {
            warn!("unable to save translation: key = {other:?}");
            false
        }
    }
}

fn codegen_translate<'m>(
    proc_table: &mut ProcTable,
    return_def_id: DefId,
    expr_table: &TypedExprTable<'m>,
    (from, to): (ExprRef, ExprRef),
    direction: DebugDirection,
) -> UnlinkedProc {
    let (_, from_expr, _) = expr_table.resolve_expr(&expr_table.source_rewrites, from);

    // for easier readability:
    match direction {
        DebugDirection::Forward => debug!(
            "codegen source: {} target: {}",
            expr_table.debug_tree(&expr_table.source_rewrites, from),
            expr_table.debug_tree(&expr_table.target_rewrites, to),
        ),
        DebugDirection::Backward => debug!(
            "codegen target: {} source: {}",
            expr_table.debug_tree(&expr_table.target_rewrites, from),
            expr_table.debug_tree(&expr_table.source_rewrites, to),
        ),
    }

    match &from_expr.kind {
        TypedExprKind::ValueObjPattern(_) => {
            codegen_value_obj_origin(proc_table, return_def_id, expr_table, to)
        }
        TypedExprKind::MapObjPattern(attributes) => {
            codegen_map_obj_origin(proc_table, expr_table, attributes, to)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

#[derive(Default)]
pub struct VarFlowTracker {
    // for determining whether to clone
    states: HashMap<SyntaxVar, VarFlowState>,
}

impl VarFlowTracker {
    /// count usages when building up the full state
    pub fn count_use(&mut self, var: SyntaxVar) {
        self.states.entry(var).or_default().use_count += 1;
    }

    /// actually use the variable. Returns previous state.
    pub fn do_use(&mut self, var: SyntaxVar) -> VarFlowState {
        let stored_state = self.states.entry(var).or_default();
        let clone = stored_state.clone();
        stored_state.use_count -= 1;
        stored_state.reused = true;
        clone
    }
}

#[derive(Clone, Default)]
pub struct VarFlowState {
    pub use_count: usize,
    pub reused: bool,
}

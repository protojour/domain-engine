use std::collections::HashMap;

use ontol_runtime::DefId;
use tracing::{debug, warn};

use crate::{
    codegen::map_obj::codegen_map_obj_origin,
    typed_expr::{ExprRef, SealedTypedExprTable, SyntaxVar, TypedExprKind, TypedExprTable},
};

use super::{find_translation_key, value_obj::codegen_value_obj_origin, ProcTable, UnlinkedProc};

pub(super) enum DebugDirection {
    Forward,
    Backward,
}

pub(super) fn codegen_translate_rewrite(
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

    let from_def = find_translation_key(&table.inner.expressions[from].ty);
    let to_def = find_translation_key(&table.inner.expressions[to].ty);

    match (from_def, to_def) {
        (Some(from_def), Some(to_def)) => {
            let procedure =
                codegen_translate(proc_table, &table.inner, (from, to), to_def, direction);

            proc_table.procedures.insert((from_def, to_def), procedure);
            true
        }
        other => {
            warn!("unable to save translation: key = {other:?}");
            false
        }
    }
}

fn codegen_translate(
    proc_table: &mut ProcTable,
    expr_table: &TypedExprTable,
    (from, to): (ExprRef, ExprRef),
    to_def: DefId,
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
            codegen_value_obj_origin(proc_table, expr_table, to, to_def)
        }
        TypedExprKind::MapObjPattern(attributes) => {
            codegen_map_obj_origin(proc_table, expr_table, to, attributes)
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

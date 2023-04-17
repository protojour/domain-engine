use ontol_runtime::DefId;
use tracing::{debug, warn};

use crate::{
    codegen::map_obj::codegen_map_obj_origin,
    typed_expr::{ExprRef, TypedExprKind},
};

use super::{
    equation::TypedExprEquation, find_translation_key, value_obj::codegen_value_obj_origin,
    ProcBuilder, ProcTable,
};

#[allow(unused)]
pub(super) enum DebugDirection {
    Forward,
    Backward,
}

pub(super) fn codegen_translate_solve(
    proc_table: &mut ProcTable,
    equation: &mut TypedExprEquation,
    (from, to): (ExprRef, ExprRef),
    direction: DebugDirection,
) -> bool {
    // solve equation
    let mut solver = equation.solver();
    solver.reduce_expr(from).unwrap_or_else(|error| {
        panic!("TODO: could not solve: {error:?}");
    });

    let from_def = find_translation_key(&equation.expressions[from].ty);
    let to_def = find_translation_key(&equation.expressions[to].ty);

    match (from_def, to_def) {
        (Some(from_def), Some(to_def)) => {
            let procedure = codegen_translate(proc_table, equation, (from, to), to_def, direction);

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
    equation: &TypedExprEquation,
    (from, to): (ExprRef, ExprRef),
    to_def: DefId,
    direction: DebugDirection,
) -> ProcBuilder {
    let (_, from_expr, _) = equation.resolve_expr(&equation.reductions, from);

    debug!("expansions: {:?}", equation.expansions.debug_table());
    debug!("reductions: {:?}", equation.reductions.debug_table());

    // for easier readability:
    match direction {
        DebugDirection::Forward => debug!(
            "(forward) codegen\nreductions: {:#?}\nexpansions: {:#?}",
            equation.debug_tree(from, &equation.reductions),
            equation.debug_tree(to, &equation.expansions),
        ),
        DebugDirection::Backward => debug!(
            "(backward) codegen\nexpansions: {:#?}\nreductions: {:#?}",
            equation.debug_tree(to, &equation.expansions),
            equation.debug_tree(from, &equation.reductions),
        ),
    }

    match &from_expr.kind {
        TypedExprKind::ValueObjPattern(_) => {
            codegen_value_obj_origin(proc_table, equation, to, to_def)
        }
        TypedExprKind::MapObjPattern(attributes) => {
            codegen_map_obj_origin(proc_table, equation, to, attributes)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

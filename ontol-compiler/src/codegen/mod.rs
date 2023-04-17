use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, Lib, Procedure},
    DefId,
};

mod equation;
mod equation_solver;
mod generator;
mod ir;
mod link;
mod optimize;
mod proc_builder;
mod struct_pattern;
mod value_pattern;

use tracing::{debug, warn};

use crate::{
    codegen::{
        struct_pattern::codegen_struct_pattern_origin, value_pattern::codegen_value_pattern_origin,
    },
    typed_expr::{ExprRef, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

use self::{
    equation::TypedExprEquation,
    ir::Terminator,
    link::{link, LinkResult},
    proc_builder::{Block, ProcBuilder},
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
                codegen_map_solve(
                    &mut proc_table,
                    &mut equation,
                    (map_task.node_a, map_task.node_b),
                    DebugDirection::Forward,
                );

                equation.reset();

                // b -> a
                codegen_map_solve(
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

#[allow(unused)]
pub(super) enum DebugDirection {
    Forward,
    Backward,
}

fn codegen_map_solve(
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
            let procedure = codegen_map(proc_table, equation, (from, to), to_def, direction);

            proc_table.procedures.insert((from_def, to_def), procedure);
            true
        }
        other => {
            warn!("unable to save translation: key = {other:?}");
            false
        }
    }
}

fn codegen_map(
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
        TypedExprKind::ValuePattern(_) => {
            codegen_value_pattern_origin(proc_table, equation, to, to_def)
        }
        TypedExprKind::StructPattern(attributes) => {
            codegen_struct_pattern_origin(proc_table, equation, to, attributes)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

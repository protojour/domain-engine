use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, Lib, NParams, Procedure},
    MapKey,
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
    codegen::{generator::CodeGenerator, proc_builder::Stack},
    ir_node::{Body, CodeDirection, IrNodeTable},
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

use self::{
    equation::IrNodeEquation,
    ir::Terminator,
    link::{link, LinkResult},
    proc_builder::{Block, ProcBuilder},
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_map_procs: FnvHashMap<(MapKey, MapKey), Procedure>,
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
    pub nodes: IrNodeTable<'m>,
    pub bodies: Vec<Body>,
    pub span: SourceSpan,
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub procedures: FnvHashMap<(MapKey, MapKey), ProcBuilder>,
    pub map_calls: Vec<MapCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a map call.
    /// This will be resolved to final "physical" ID in the link phase.
    fn gen_mapping_addr(&mut self, from: MapKey, to: MapKey) -> Address {
        let address = Address(self.map_calls.len() as u32);
        self.map_calls.push(MapCall {
            mapping: (from, to),
        });
        address
    }
}

pub(super) struct MapCall {
    pub mapping: (MapKey, MapKey),
}

fn find_mapping_key(ty: &TypeRef) -> Option<MapKey> {
    match ty {
        Type::Domain(def_id) => Some(MapKey {
            def_id: *def_id,
            seq: false,
        }),
        Type::Array(element) => {
            let def_id = element.get_single_def_id()?;
            Some(MapKey { def_id, seq: true })
        }
        other => {
            warn!("unable to get mapping key: {other:?}");
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
                let bodies = map_task.bodies;
                let mut equation = IrNodeEquation::new(map_task.nodes);

                for (index, node) in equation.nodes.0.iter().enumerate() {
                    debug!("{{{index}}}: {node:?}");
                }

                for (index, body) in bodies.iter().enumerate() {
                    debug!(
                        "BodyId({index}) equation before solve:\n first: {:#?}\nsecond: {:#?}",
                        equation.debug_tree(body.first, &equation.reductions),
                        equation.debug_tree(body.second, &equation.expansions),
                    );
                }

                codegen_map_solve(
                    &mut proc_table,
                    &mut equation,
                    &bodies,
                    CodeDirection::Forward,
                );

                equation.reset();

                codegen_map_solve(
                    &mut proc_table,
                    &mut equation,
                    &bodies,
                    CodeDirection::Backward,
                );
            }
        }
    }

    let LinkResult { lib, map_procs } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_map_procs = map_procs;
}

fn codegen_map_solve(
    proc_table: &mut ProcTable,
    equation: &mut IrNodeEquation,
    bodies: &[Body],
    direction: CodeDirection,
) -> bool {
    let mut solver = equation.solver();

    // solve equation(s)
    for body in bodies {
        solver
            .reduce_node(body.bindings_node(direction))
            .unwrap_or_else(|error| {
                panic!("TODO: could not solve: {error:?}");
            });
    }

    for (index, body) in bodies.iter().enumerate() {
        let (from, to) = body.order(direction);
        match direction {
            CodeDirection::Forward => debug!(
                "BodyId({index}) (forward) codegen\nreductions: {:#?}\nexpansions: {:#?}",
                equation.debug_tree(from, &equation.reductions),
                equation.debug_tree(to, &equation.expansions),
            ),
            CodeDirection::Backward => debug!(
                "BodyId({index}) (backward codegen\nexpansions: {:#?}\nreductions: {:#?}",
                equation.debug_tree(to, &equation.expansions),
                equation.debug_tree(from, &equation.reductions),
            ),
        }
    }

    let root_body = bodies.first().unwrap();
    let (from, to) = root_body.order(direction);

    let from_def = find_mapping_key(&equation.nodes[from].ty);
    let to_def = find_mapping_key(&equation.nodes[to].ty);

    match (from_def, to_def) {
        (Some(from_def), Some(to_def)) => {
            let (_, _, span) = equation.resolve_node(&equation.expansions, to);

            let mut builder = ProcBuilder::new(NParams(1));
            let mut block = builder.new_block(Stack(1), span);
            let mut generator = CodeGenerator::new(proc_table, &mut builder, direction);

            generator.codegen_body(&mut block, equation, root_body);

            builder.commit(block, Terminator::Return(builder.top()));

            proc_table.procedures.insert((from_def, to_def), builder);
            true
        }
        other => {
            warn!("unable to save mapping: key = {other:?}");
            false
        }
    }
}

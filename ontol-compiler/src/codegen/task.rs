use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    vm::proc::{Address, Lib, NParams, Procedure},
    DefId, MapKey,
};
use tracing::{debug, error, warn};

use crate::{
    codegen::{generator::CodeGenerator, proc_builder::Stack},
    hir_node::{CodeDirection, HirBody, HirBodyIdx, HirIdx, HirNodeTable},
    typed_ontos::{lang::OntosNode, unify::unifier::unify_to_function},
    types::{Type, TypeRef},
    Compiler, IrVariant, SourceSpan, IR_VARIANT,
};

use super::{
    equation::HirEquation,
    ir::Terminator,
    link::{link, LinkResult},
    proc_builder::ProcBuilder,
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_const_procs: FnvHashMap<DefId, Procedure>,
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
    // A procedure with 0 arguments, used to produce a constant value
    Const(ConstCodegenTask<'m>),
    Map(MapCodegenTask<'m>),
    OntosMap(OntosMapCodegenTask<'m>),
}

#[derive(Debug)]
pub struct ConstCodegenTask<'m> {
    pub def_id: DefId,
    pub nodes: HirNodeTable<'m>,
    pub root: HirIdx,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub struct MapCodegenTask<'m> {
    pub nodes: HirNodeTable<'m>,
    pub bodies: Vec<HirBody>,
    pub span: SourceSpan,
}

pub struct OntosMapCodegenTask<'m> {
    pub first: OntosNode<'m>,
    pub second: OntosNode<'m>,
    pub span: SourceSpan,
}

impl<'m> Debug for OntosMapCodegenTask<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OntosMapCodegenTask")
            .field("first", &DebugViaDisplay(&self.first))
            .field("second", &DebugViaDisplay(&self.second))
            .finish()
    }
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub map_procedures: FnvHashMap<(MapKey, MapKey), ProcBuilder>,
    pub const_procedures: FnvHashMap<DefId, ProcBuilder>,
    pub map_calls: Vec<MapCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a map call.
    /// This will be resolved to final "physical" ID in the link phase.
    pub(super) fn gen_mapping_addr(&mut self, from: MapKey, to: MapKey) -> Address {
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

pub(super) fn find_mapping_key(ty: &TypeRef) -> Option<MapKey> {
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
            CodegenTask::Const(ConstCodegenTask {
                def_id,
                nodes,
                root,
                span,
            }) => {
                let equation = HirEquation::new(nodes);
                let mut builder = ProcBuilder::new(NParams(0));
                let mut block = builder.new_block(Stack(0), span);
                let mut generator =
                    CodeGenerator::new(&mut proc_table, &mut builder, &[], CodeDirection::Forward);

                match generator.codegen_expr(&mut block, &equation, root) {
                    Ok(_) => {
                        builder.commit(block, Terminator::Return(builder.top()));
                        proc_table.const_procedures.insert(def_id, builder);
                    }
                    Err(error) => panic!("BUG: Problem generating const expression: {error:?}"),
                }
            }
            CodegenTask::Map(map_task) => {
                let bodies = map_task.bodies;
                let mut equation = HirEquation::new(map_task.nodes);

                for (index, node) in equation.nodes.0.iter().enumerate() {
                    debug!("{{{index}}}: {node:?}");
                }

                for (index, body) in bodies.iter().enumerate() {
                    debug!(
                        "HirBodyIdx({index}) equation before solve:\n=={:#?}\n=={:#?}",
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
            CodegenTask::OntosMap(map_task) => {
                if IR_VARIANT != IrVariant::Ontos {
                    continue;
                }

                debug!("1st: {}", map_task.first);
                debug!("2nd: {}", map_task.second);

                debug!("Ontos forward");
                unify_to_function(map_task.first.clone(), map_task.second.clone());
                debug!("Ontos backward");
                unify_to_function(map_task.second, map_task.first);
            }
        }
    }

    let LinkResult {
        lib,
        const_procs,
        map_procs,
    } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_const_procs = const_procs;
    compiler.codegen_tasks.result_map_procs = map_procs;
}

fn codegen_map_solve(
    proc_table: &mut ProcTable,
    equation: &mut HirEquation,
    bodies: &[HirBody],
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
                "HirBodyIdx({index}) forward solved:\n<={:#?}\n=>{:#?}",
                equation.debug_tree(from, &equation.reductions),
                equation.debug_tree(to, &equation.expansions),
            ),
            CodeDirection::Backward => debug!(
                "HirBodyIdx({index}) backward solved:\n=>{:#?}\n<={:#?}",
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

            let mut builder = ProcBuilder::new(NParams(0));
            let mut block = builder.new_block(Stack(1), span);
            let mut generator = CodeGenerator::new(proc_table, &mut builder, bodies, direction);

            match generator.codegen_body(&mut block, equation, HirBodyIdx(0)) {
                Ok(()) => {
                    builder.commit(block, Terminator::Return(builder.top()));

                    proc_table
                        .map_procedures
                        .insert((from_def, to_def), builder);
                    true
                }
                Err(e) => {
                    error!("Codegen problem, ignoring this (for now): {e:?}");
                    false
                }
            }
        }
        other => {
            warn!("unable to save mapping: key = {other:?}");
            false
        }
    }
}

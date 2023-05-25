use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    vm::proc::{Address, Lib, NParams, Procedure},
    DefId, MapKey,
};
use tracing::{debug, warn};

use crate::{
    codegen::{
        hir_code_generator::HirCodeGenerator, ontos_code_generator::map_codegen_ontos,
        proc_builder::Stack,
    },
    hir_node::{CodeDirection, HirBody, HirIdx, HirNodeTable},
    typed_ontos::{lang::OntosNode, unify::unifier::unify_to_function},
    types::{Type, TypeRef},
    Compiler, IrVariant, SourceSpan, CODE_GENERATOR, TYPE_CHECKER,
};

use super::{
    hir_code_generator::codegen_map_hir_solve,
    hir_equation::HirEquation,
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

pub(super) fn find_mapping_key(ty: TypeRef) -> Option<MapKey> {
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
                let mut generator = HirCodeGenerator::new(
                    &mut proc_table,
                    &mut builder,
                    &[],
                    CodeDirection::Forward,
                );

                match generator.codegen_expr(&mut block, &equation, root) {
                    Ok(_) => {
                        builder.commit(block, Terminator::Return(builder.top()));
                        proc_table.const_procedures.insert(def_id, builder);
                    }
                    Err(error) => panic!("BUG: Problem generating const expression: {error:?}"),
                }
            }
            CodegenTask::Map(map_task) => {
                if CODE_GENERATOR != IrVariant::Hir {
                    continue;
                }

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

                codegen_map_hir_solve(
                    &mut proc_table,
                    &mut equation,
                    &bodies,
                    CodeDirection::Forward,
                );

                equation.reset();

                codegen_map_hir_solve(
                    &mut proc_table,
                    &mut equation,
                    &bodies,
                    CodeDirection::Backward,
                );
            }
            CodegenTask::OntosMap(map_task) => {
                if TYPE_CHECKER != IrVariant::Ontos {
                    continue;
                }

                debug!("1st:\n{}", map_task.first);
                debug!("2nd:\n{}", map_task.second);

                debug!("Ontos forward start");
                if let Ok(func) =
                    unify_to_function(map_task.first.clone(), map_task.second.clone(), compiler)
                {
                    map_codegen_ontos(&mut proc_table, func);
                }

                debug!("Ontos backward start");
                if let Ok(func) = unify_to_function(map_task.second, map_task.first, compiler) {
                    map_codegen_ontos(&mut proc_table, func);
                }
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

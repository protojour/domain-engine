use std::fmt::Debug;

use fnv::FnvHashMap;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    vm::proc::{Address, Lib, Procedure},
    DefId, MapKey,
};
use tracing::{debug, warn};

use crate::{
    codegen::code_generator::map_codegen,
    hir_unify::tree::unify3::unify_to_function,
    typed_hir::TypedHirNode,
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

use super::{
    code_generator::const_codegen,
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
}

pub struct ConstCodegenTask<'m> {
    pub def_id: DefId,
    pub node: TypedHirNode<'m>,
}

pub struct MapCodegenTask<'m> {
    pub first: TypedHirNode<'m>,
    pub second: TypedHirNode<'m>,
    pub span: SourceSpan,
}

impl<'m> Debug for ConstCodegenTask<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstCodegenTask")
            .field("node", &DebugViaDisplay(&self.node))
            .finish()
    }
}

impl<'m> Debug for MapCodegenTask<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapCodegenTask")
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
            CodegenTask::Const(ConstCodegenTask { def_id, node }) => {
                const_codegen(&mut proc_table, node, def_id, &mut compiler.errors);
            }
            CodegenTask::Map(map_task) => {
                debug!("1st (ty={:?}):\n{}", map_task.first.meta.ty, map_task.first);
                debug!(
                    "2nd (ty={:?}):\n{}",
                    map_task.second.meta.ty, map_task.second
                );

                debug!("Forward start");
                if let Ok(func) =
                    unify_to_function(map_task.first.clone(), map_task.second.clone(), compiler)
                {
                    map_codegen(&mut proc_table, func, &mut compiler.errors);
                }

                debug!("Backward start");
                if let Ok(func) = unify_to_function(map_task.second, map_task.first, compiler) {
                    map_codegen(&mut proc_table, func, &mut compiler.errors);
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

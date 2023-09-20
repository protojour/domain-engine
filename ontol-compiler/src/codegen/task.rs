use std::fmt::Debug;

use fnv::FnvHashMap;
use indexmap::{map::Entry, IndexMap};
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    ontology::PropertyFlow,
    vm::proc::{Address, Lib, Procedure},
    DefId, MapKey,
};
use tracing::{debug, warn};

use crate::{
    codegen::code_generator::map_codegen, hir_unify::unify_to_function, typed_hir::TypedHir,
    Compiler, SourceSpan,
};

use super::{
    auto_map::autogenerate_mapping,
    code_generator::const_codegen,
    link::{link, LinkResult},
    proc_builder::ProcBuilder,
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    const_tasks: Vec<ConstCodegenTask<'m>>,
    pub map_tasks: IndexMap<MapKeyPair, MapCodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_const_procs: FnvHashMap<DefId, Procedure>,
    pub result_map_proc_table: FnvHashMap<(MapKey, MapKey), Procedure>,
    pub result_propflow_table: FnvHashMap<(MapKey, MapKey), Vec<PropertyFlow>>,
}

impl<'m> Debug for CodegenTasks<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodegenTasks")
            .field("tasks", &self.const_tasks)
            .finish()
    }
}

impl<'m> CodegenTasks<'m> {
    pub fn add_map_task(&mut self, pair: MapKeyPair, task: MapCodegenTask<'m>) {
        match self.map_tasks.entry(pair) {
            Entry::Occupied(mut occupied) => {
                if let (MapCodegenTask::Auto, MapCodegenTask::Explicit(_)) = (occupied.get(), &task)
                {
                    // Explicit maps may overwrite auto-generated maps
                    occupied.insert(task);
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(task);
            }
        }
    }

    pub fn add_const_task(&mut self, const_task: ConstCodegenTask<'m>) {
        self.const_tasks.push(const_task);
    }
}

pub struct ConstCodegenTask<'m> {
    pub def_id: DefId,
    pub node: ontol_hir::RootNode<'m, TypedHir>,
}

pub enum MapCodegenTask<'m> {
    Auto,
    Explicit(ExplicitMapCodegenTask<'m>),
}

pub struct ExplicitMapCodegenTask<'m> {
    pub first: MapArm<'m>,
    pub second: MapArm<'m>,
    pub span: SourceSpan,
}

pub struct MapArm<'m> {
    pub node: ontol_hir::RootNode<'m, TypedHir>,
    pub is_match: bool,
}

impl<'m> Debug for ConstCodegenTask<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstCodegenTask")
            .field("node", &DebugViaDisplay(&self.node))
            .finish()
    }
}

impl<'m> Debug for ExplicitMapCodegenTask<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapCodegenTask")
            .field("first", &DebugViaDisplay(&self.first.node))
            .field("second", &DebugViaDisplay(&self.second.node))
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MapKeyPair {
    first: MapKey,
    second: MapKey,
}

impl MapKeyPair {
    pub fn new(a: MapKey, b: MapKey) -> Self {
        if a < b {
            Self {
                first: a,
                second: b,
            }
        } else {
            Self {
                first: b,
                second: a,
            }
        }
    }

    pub fn first(&self) -> MapKey {
        self.first
    }

    pub fn second(&self) -> MapKey {
        self.second
    }
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub map_procedures: FnvHashMap<(MapKey, MapKey), ProcBuilder>,
    pub const_procedures: FnvHashMap<DefId, ProcBuilder>,
    pub procedure_calls: Vec<ProcedureCall>,
    pub propflow_table: FnvHashMap<(MapKey, MapKey), Vec<PropertyFlow>>,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a map call.
    /// This will be resolved to final "physical" ID in the link phase.
    pub(super) fn gen_mapping_addr(&mut self, from: MapKey, to: MapKey) -> Address {
        let address = Address(self.procedure_calls.len() as u32);
        self.procedure_calls.push(ProcedureCall::Map(from, to));
        address
    }

    pub(super) fn gen_const_addr(&mut self, const_def_id: DefId) -> Address {
        let address = Address(self.procedure_calls.len() as u32);
        self.procedure_calls
            .push(ProcedureCall::Const(const_def_id));
        address
    }
}

pub(super) enum ProcedureCall {
    Map(MapKey, MapKey),
    Const(DefId),
}

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let mut explicit_map_tasks = Vec::with_capacity(compiler.codegen_tasks.map_tasks.len());

    for (def_pair, map_task) in std::mem::take(&mut compiler.codegen_tasks.map_tasks) {
        match map_task {
            MapCodegenTask::Auto => {
                if let Some(task) = autogenerate_mapping(def_pair, compiler) {
                    explicit_map_tasks.push(task);
                }
            }
            MapCodegenTask::Explicit(explicit) => {
                explicit_map_tasks.push(explicit);
            }
        }
    }

    let mut proc_table = ProcTable::default();

    for ConstCodegenTask { def_id, node } in std::mem::take(&mut compiler.codegen_tasks.const_tasks)
    {
        let errors = const_codegen(&mut proc_table, node, def_id, compiler);
        compiler.errors.extend(errors);
    }

    for map_task in explicit_map_tasks {
        debug!(
            "1st (ty={:?}):\n{}",
            map_task.first.node.as_ref().meta().ty,
            map_task.first.node
        );
        debug!(
            "2nd (ty={:?}):\n{}",
            map_task.second.node.as_ref().meta().ty,
            map_task.second.node
        );

        if !map_task.second.is_match {
            debug!("Forward start");
            match unify_to_function(&map_task.first.node, &map_task.second.node, compiler) {
                Ok(func) => {
                    let errors = map_codegen(&mut proc_table, func, compiler);
                    compiler.errors.extend(errors);
                }
                Err(err) => warn!("unifier error: {err:?}"),
            }
        }

        if !map_task.first.is_match {
            debug!("Backward start");
            match unify_to_function(&map_task.second.node, &map_task.first.node, compiler) {
                Ok(func) => {
                    let errors = map_codegen(&mut proc_table, func, compiler);
                    compiler.errors.extend(errors);
                }
                Err(err) => warn!("unifier error: {err:?}"),
            }
        }
    }

    let LinkResult {
        lib,
        const_procs,
        map_proc_table,
    } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_const_procs = const_procs;
    compiler.codegen_tasks.result_map_proc_table = map_proc_table;
    compiler.codegen_tasks.result_propflow_table = proc_table.propflow_table;
}

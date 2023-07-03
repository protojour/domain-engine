use std::{collections::HashSet, fmt::Debug};

use fnv::FnvHashMap;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    vm::proc::{Address, Lib, Procedure},
    DefId, MapKey,
};
use tracing::{debug, warn};

use crate::{
    codegen::{code_generator::map_codegen, type_mapper::TypeMapper},
    def::MapDirection,
    hir_unify::unify_to_function,
    typed_hir::TypedHirNode,
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
    tasks: Vec<CodegenTask<'m>>,
    pub auto_maps: HashSet<AutoMapKey>,
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
    pub direction: MapDirection,
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AutoMapKey {
    first: DefId,
    second: DefId,
}

impl AutoMapKey {
    // TODO: improve this function
    #[allow(clippy::comparison_chain)]
    pub fn new(a: DefId, b: DefId) -> Self {
        if a.package_id() == b.package_id() {
            if a.1 < b.1 {
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
        } else if a.package_id() < b.package_id() {
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

    pub fn first(&self) -> DefId {
        self.first
    }

    pub fn second(&self) -> DefId {
        self.second
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

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let mut tasks = std::mem::take(&mut compiler.codegen_tasks.tasks);
    let auto_maps = std::mem::take(&mut compiler.codegen_tasks.auto_maps);

    for auto_map_key in auto_maps {
        if let Some(task) = autogenerate_mapping(auto_map_key, compiler) {
            tasks.push(task);
        }
    }

    let mut proc_table = ProcTable::default();

    for task in tasks {
        match task {
            CodegenTask::Const(ConstCodegenTask { def_id, node }) => {
                let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs);
                const_codegen(
                    &mut proc_table,
                    node,
                    def_id,
                    type_mapper,
                    &mut compiler.errors,
                );
            }
            CodegenTask::Map(map_task) => {
                debug!("1st (ty={:?}):\n{}", map_task.first.ty(), map_task.first);
                debug!("2nd (ty={:?}):\n{}", map_task.second.ty(), map_task.second);

                debug!("Forward start");
                match unify_to_function(&map_task.first, &map_task.second, compiler) {
                    Ok(func) => {
                        let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs);
                        map_codegen(&mut proc_table, func, type_mapper, &mut compiler.errors);
                    }
                    Err(err) => warn!("unifier error: {err:?}"),
                }

                if matches!(map_task.direction, MapDirection::Omni) {
                    debug!("Backward start");
                    match unify_to_function(&map_task.second, &map_task.first, compiler) {
                        Ok(func) => {
                            let type_mapper = TypeMapper::new(&compiler.relations, &compiler.defs);
                            map_codegen(&mut proc_table, func, type_mapper, &mut compiler.errors);
                        }
                        Err(err) => warn!("unifier error: {err:?}"),
                    }
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

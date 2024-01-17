use std::{collections::HashMap, fmt::Debug};

use fnv::FnvHashMap;
use indexmap::{map::Entry, IndexMap};
use ontol_hir::StructFlags;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    ontology::{MapLossiness, PropertyFlow},
    smart_format,
    vm::proc::{Address, Lib, Procedure},
    DefId, MapFlags, MapKey, PackageId,
};
use smartstring::alias::String;
use tracing::{debug, debug_span};

use crate::{
    codegen::code_generator::map_codegen, def::DefKind, hir_unify::unify_to_function,
    map::UndirectedMapKey, typed_hir::TypedRootNode, types::Type, CompileError, CompileErrors,
    Compiler, SourceSpan, SpannedCompileError,
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
    pub map_tasks: IndexMap<UndirectedMapKey, MapCodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_const_procs: FnvHashMap<DefId, Procedure>,
    pub result_map_proc_table: FnvHashMap<MapKey, Procedure>,
    pub result_named_forward_maps: HashMap<(PackageId, String), MapKey>,
    pub result_propflow_table: FnvHashMap<MapKey, Vec<PropertyFlow>>,
    pub result_metadata_table: FnvHashMap<MapKey, MapOutputMeta>,
}

impl<'m> Debug for CodegenTasks<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodegenTasks")
            .field("tasks", &self.const_tasks)
            .finish()
    }
}

impl<'m> CodegenTasks<'m> {
    pub fn add_map_task(&mut self, pair: UndirectedMapKey, task: MapCodegenTask<'m>) {
        match self.map_tasks.entry(pair) {
            Entry::Occupied(mut occupied) => {
                if let (MapCodegenTask::Auto(_), MapCodegenTask::Explicit(_)) =
                    (occupied.get(), &task)
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
    pub node: TypedRootNode<'m>,
}

pub enum MapCodegenTask<'m> {
    Auto(PackageId),
    Explicit(ExplicitMapCodegenTask<'m>),
}

pub struct ExplicitMapCodegenTask<'m> {
    pub def_id: DefId,
    pub arms: [TypedRootNode<'m>; 2],
    pub span: SourceSpan,
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
            .field("first", &DebugViaDisplay(&self.arms[0]))
            .field("second", &DebugViaDisplay(&self.arms[1]))
            .finish()
    }
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub map_procedures: FnvHashMap<MapKey, ProcBuilder>,
    pub const_procedures: FnvHashMap<DefId, ProcBuilder>,
    pub procedure_calls: Vec<ProcedureCall>,
    pub propflow_table: FnvHashMap<MapKey, Vec<PropertyFlow>>,
    pub metadata_table: FnvHashMap<MapKey, MapOutputMeta>,
    pub named_forward_maps: HashMap<(PackageId, String), MapKey>,
}

pub struct MapOutputMeta {
    pub lossiness: MapLossiness,
}

impl ProcTable {
    /// Allocate a temporary procedure address for a map call.
    /// This will be resolved to final "physical" ID in the link phase.
    pub(super) fn gen_mapping_addr(&mut self, key: MapKey) -> Address {
        let address = Address(self.procedure_calls.len() as u32);
        self.procedure_calls.push(ProcedureCall::Map(key));
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
    Map(MapKey),
    Const(DefId),
}

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let mut explicit_map_tasks = Vec::with_capacity(compiler.codegen_tasks.map_tasks.len());

    for (def_pair, map_task) in std::mem::take(&mut compiler.codegen_tasks.map_tasks) {
        match map_task {
            MapCodegenTask::Auto(package_id) => {
                if let Some(task) = autogenerate_mapping(def_pair, package_id, compiler) {
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
        let mut errors = CompileErrors::default();
        const_codegen(node, def_id, &mut proc_table, compiler, &mut errors);
        compiler.errors.extend(errors);
    }

    for task in explicit_map_tasks {
        generate_explicit_map(task, &mut proc_table, compiler);
    }

    let LinkResult {
        lib,
        const_procs,
        map_proc_table,
    } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_const_procs = const_procs;
    compiler.codegen_tasks.result_map_proc_table = map_proc_table;
    compiler.codegen_tasks.result_named_forward_maps = proc_table.named_forward_maps;
    compiler.codegen_tasks.result_propflow_table = proc_table.propflow_table;
    compiler.codegen_tasks.result_metadata_table = proc_table.metadata_table;
}

fn generate_explicit_map<'m>(
    ExplicitMapCodegenTask { def_id, arms, span }: ExplicitMapCodegenTask<'m>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) {
    debug!("1st (ty={:?}):\n{}", arms[0].data().ty(), arms[0]);
    debug!("2nd (ty={:?}):\n{}", arms[1].data().ty(), arms[1]);

    let forward_key = {
        let _entered = debug_span!("fwd").entered();
        generate_map_procs(&arms[0], &arms[1], proc_table, compiler)
    };

    {
        let _entered = debug_span!("backwd").entered();
        generate_map_procs(&arms[1], &arms[0], proc_table, compiler);
    }

    match (compiler.map_ident(def_id), forward_key) {
        (Some(ident), Some(forward_key)) => {
            proc_table
                .named_forward_maps
                .insert((def_id.package_id(), ident.into()), forward_key);
        }
        (Some(_), None) => {
            compiler.errors.push(SpannedCompileError {
                error: CompileError::BUG(smart_format!("Failed to generate forward mapping")),
                span,
                notes: vec![],
            });
        }
        _ => {}
    }
}

fn generate_map_procs<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) -> Option<MapKey> {
    let needs_pure_partial = match expr.as_ref().kind() {
        ontol_hir::Kind::Struct(_, flags, _) => flags.contains(StructFlags::MATCH),
        _ => false,
    };

    let key = generate_map_proc(scope, expr, MapFlags::empty(), proc_table, compiler);

    if needs_pure_partial {
        let _entered = debug_span!("pure").entered();
        generate_map_proc(scope, expr, MapFlags::PURE_PARTIAL, proc_table, compiler);
    }

    key
}

fn generate_map_proc<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    map_flags: MapFlags,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) -> Option<MapKey> {
    let func = match unify_to_function(scope, expr, map_flags, compiler) {
        Ok(func) => func,
        Err(err) => {
            debug!("unifier error: {err:?}");
            return None;
        }
    };

    debug!("unified, ready for codegen:\n{}", func);

    // NB: Error is used in unification tests, so don't assert on types matching if there are type errors
    if !matches!(scope.data().ty(), Type::Error) {
        assert_eq!(func.arg.ty(), scope.data().ty());
    }
    if !matches!(expr.data().ty(), Type::Error) {
        assert_eq!(func.body.data().ty(), expr.data().ty());
    }

    debug!("body type: {:?}", func.body.data().ty());

    let mut errors = CompileErrors::default();
    let key = map_codegen(proc_table, &func, map_flags, compiler, &mut errors);
    compiler.errors.extend(errors);

    Some(key)
}

impl<'m> Compiler<'m> {
    fn map_ident(&self, def_id: DefId) -> Option<&'m str> {
        if let DefKind::Mapping { ident, .. } = self.defs.def_kind(def_id) {
            *ident
        } else {
            None
        }
    }
}

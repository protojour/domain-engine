use std::fmt::Debug;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::{map::Entry, IndexMap};
use ontol_hir::StructFlags;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    ontology::{
        map::{MapLossiness, PropertyFlow},
        ontol::TextConstant,
    },
    query::condition::Condition,
    vm::proc::{Address, Lib, NParams, OpCode, Procedure},
    DefId, MapDef, MapDirection, MapFlags, MapKey, PackageId,
};
use tracing::{debug, debug_span, warn};

use crate::{
    codegen::{
        code_generator::map_codegen, static_condition::generate_static_condition_from_scope,
    },
    def::{DefKind, Defs},
    hir_unify::unify_to_function,
    map::UndirectedMapKey,
    typed_hir::TypedRootNode,
    types::Type,
    CompileError, CompileErrors, Compiler, Note, SourceSpan, SpannedCompileError, SpannedNote,
};

use super::{
    auto_map::autogenerate_mapping,
    code_generator::const_codegen,
    ir::Terminator,
    link::{link, LinkResult},
    proc_builder::{Delta, ProcBuilder},
    union_map_generator::generate_union_maps,
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    const_tasks: Vec<ConstCodegenTask<'m>>,
    map_tasks: IndexMap<UndirectedMapKey, MapCodegenTask<'m>>,

    pub result_lib: Lib,
    pub result_const_procs: FnvHashMap<DefId, Procedure>,
    pub result_map_proc_table: FnvHashMap<MapKey, Procedure>,
    pub result_named_downmaps: FnvHashMap<(PackageId, TextConstant), MapKey>,
    pub result_propflow_table: FnvHashMap<MapKey, Vec<PropertyFlow>>,
    pub result_static_conditions: FnvHashMap<MapKey, Condition>,
    pub result_metadata_table: FnvHashMap<MapKey, MapOutputMeta>,
}

impl<'m> CodegenTasks<'m> {
    pub fn add_map_task(
        &mut self,
        pair: UndirectedMapKey,
        request: MapCodegenRequest<'m>,
        _defs: &Defs,
        _errors: &mut CompileErrors,
    ) {
        match self.map_tasks.entry(pair) {
            Entry::Occupied(mut occupied) => {
                match (occupied.get_mut(), request) {
                    (MapCodegenTask::Auto(_), MapCodegenRequest::ExplicitOntol(ontol_map)) => {
                        // Explicit maps may overwrite auto-generated maps
                        occupied.insert(MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                            ontol_map: Some(ontol_map),
                            forward_extern: None,
                            backward_extern: None,
                        }));
                    }
                    (
                        MapCodegenTask::Explicit(expl),
                        MapCodegenRequest::ExplicitOntol(ontol_map),
                    ) => {
                        expl.ontol_map = Some(ontol_map);
                    }
                    (MapCodegenTask::Explicit(expl), MapCodegenRequest::ExternForward(extern_)) => {
                        expl.forward_extern = Some(extern_);
                    }
                    (
                        MapCodegenTask::Explicit(expl),
                        MapCodegenRequest::ExternBackward(extern_),
                    ) => {
                        expl.backward_extern = Some(extern_);
                    }
                    _ => {
                        warn!("TODO: Invalid mix of map strategies");
                    }
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(match request {
                    MapCodegenRequest::Auto(auto) => MapCodegenTask::Auto(auto),
                    MapCodegenRequest::ExplicitOntol(ontol_map) => {
                        MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                            ontol_map: Some(ontol_map),
                            forward_extern: None,
                            backward_extern: None,
                        })
                    }
                    MapCodegenRequest::ExternForward(extern_id) => {
                        MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                            ontol_map: None,
                            forward_extern: Some(extern_id),
                            backward_extern: None,
                        })
                    }
                    MapCodegenRequest::ExternBackward(extern_id) => {
                        MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                            ontol_map: None,
                            forward_extern: None,
                            backward_extern: Some(extern_id),
                        })
                    }
                });
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

/// A request for code generation.
/// It goes together with a key.
pub enum MapCodegenRequest<'m> {
    /// Autogenerate
    Auto(PackageId),
    ExplicitOntol(OntolMap<'m>),
    ExternForward(DefId),
    ExternBackward(DefId),
}

/// A native ontol `map` with full body expressed in ontol-hir
pub struct OntolMap<'m> {
    pub def_id: DefId,
    pub arms: OntolMapArms<'m>,
    pub span: SourceSpan,
}

pub enum OntolMapArms<'m> {
    Patterns([TypedRootNode<'m>; 2]),
    Abstract(DefId, DefId),
}

pub(super) enum MapCodegenTask<'m> {
    Auto(PackageId),
    Explicit(ExplicitMapCodegenTask<'m>),
}

pub(super) struct ExplicitMapCodegenTask<'m> {
    /// The native ONTOL mapping, if any
    pub ontol_map: Option<OntolMap<'m>>,
    /// extern forward override of the ontol mapping.
    /// note: The direction is relative to [UndirectedMapKey].
    pub forward_extern: Option<DefId>,
    /// extern forward override of the ontol mapping.
    /// note: The direction is relative to [UndirectedMapKey].
    pub backward_extern: Option<DefId>,
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
        let mut dbg = f.debug_struct("MapCodegenTask");
        if let Some(ontol) = &self.ontol_map {
            match &ontol.arms {
                OntolMapArms::Patterns(arms) => {
                    dbg.field("first", &DebugViaDisplay(&arms[0]));
                    dbg.field("second", &DebugViaDisplay(&arms[1]));
                }
                OntolMapArms::Abstract(upper, lower) => {
                    dbg.field("first", &upper);
                    dbg.field("second", &lower);
                }
            }
        }
        dbg.finish()
    }
}

#[derive(Default)]
pub(super) struct ProcTable {
    pub map_procedures: FnvHashMap<MapKey, ProcBuilder>,
    pub const_procedures: FnvHashMap<DefId, ProcBuilder>,
    pub procedure_calls: Vec<ProcedureCall>,
    pub propflow_table: FnvHashMap<MapKey, Vec<PropertyFlow>>,
    pub metadata_table: FnvHashMap<MapKey, MapOutputMeta>,
    pub static_conditions: FnvHashMap<MapKey, Condition>,
    pub named_downmaps: FnvHashMap<(PackageId, TextConstant), MapKey>,
}

pub struct MapOutputMeta {
    pub direction: MapDirection,
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
    let mut proc_table = ProcTable::default();

    for (key, map_task) in std::mem::take(&mut compiler.codegen_tasks.map_tasks) {
        match map_task {
            MapCodegenTask::Auto(package_id) => {
                if let Some(task) = autogenerate_mapping(key, package_id, compiler) {
                    explicit_map_tasks.push((key, task));
                }
            }
            MapCodegenTask::Explicit(explicit) => {
                explicit_map_tasks.push((key, explicit));
            }
        }
    }

    for ConstCodegenTask { def_id, node } in std::mem::take(&mut compiler.codegen_tasks.const_tasks)
    {
        const_codegen(node, def_id, &mut proc_table, compiler);
    }

    for (key, task) in explicit_map_tasks {
        generate_explicit_map(key, task, &mut proc_table, compiler);
    }

    generate_union_maps(&mut proc_table, compiler);

    let LinkResult {
        lib,
        const_procs,
        map_proc_table,
    } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_const_procs = const_procs;
    compiler.codegen_tasks.result_map_proc_table = map_proc_table;
    compiler.codegen_tasks.result_named_downmaps = proc_table.named_downmaps;
    compiler.codegen_tasks.result_static_conditions = proc_table.static_conditions;
    compiler.codegen_tasks.result_propflow_table = proc_table.propflow_table;
    compiler.codegen_tasks.result_metadata_table = proc_table.metadata_table;
}

fn generate_explicit_map<'m>(
    undirected_key: UndirectedMapKey,
    ExplicitMapCodegenTask {
        ontol_map,
        forward_extern,
        backward_extern,
    }: ExplicitMapCodegenTask<'m>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) {
    let mut externed_outputs = FnvHashSet::default();

    fn unknown_extern_map_direction(compiler: &mut Compiler, extern_def_id: DefId) {
        let span = compiler.defs.def_span(extern_def_id);
        compiler.push_error(CompileError::ExternMapUnknownDirection.spanned(&span).note(
            SpannedNote {
                note: Note::AbtractMapSuggestion,
                span,
            },
        ));
    }

    // the extern directions are in relation to the _undirected key_, not the ontol map arms!
    if let Some(extern_def_id) = forward_extern {
        if ontol_map.is_none() {
            unknown_extern_map_direction(compiler, extern_def_id);
        }

        generate_extern_map(
            undirected_key.first(),
            undirected_key.second(),
            extern_def_id,
            proc_table,
            compiler,
        );
        externed_outputs.insert(undirected_key.second().def_id);
    }
    if let Some(extern_def_id) = backward_extern {
        if ontol_map.is_none() {
            unknown_extern_map_direction(compiler, extern_def_id);
        }

        generate_extern_map(
            undirected_key.second(),
            undirected_key.first(),
            extern_def_id,
            proc_table,
            compiler,
        );
        externed_outputs.insert(undirected_key.first().def_id);
    }

    if let Some(OntolMap { def_id, arms, span }) = ontol_map {
        match arms {
            OntolMapArms::Patterns(arms) => {
                debug!("1st (ty={:?}):\n{}", arms[0].data().ty(), arms[0]);
                debug!("2nd (ty={:?}):\n{}", arms[1].data().ty(), arms[1]);

                let down_key = {
                    let _entered = debug_span!("down").entered();
                    generate_ontol_map_procs(
                        &arms[0],
                        &arms[1],
                        MapDirection::Down,
                        &externed_outputs,
                        proc_table,
                        compiler,
                    )
                };

                {
                    let _entered = debug_span!("up").entered();
                    generate_ontol_map_procs(
                        &arms[1],
                        &arms[0],
                        MapDirection::Up,
                        &externed_outputs,
                        proc_table,
                        compiler,
                    );
                }

                match (compiler.map_ident(def_id), down_key) {
                    (Some(ident), Some(down_key)) => {
                        let ident_constant = compiler.strings.intern_constant(ident);
                        proc_table
                            .named_downmaps
                            .insert((def_id.package_id(), ident_constant), down_key);
                    }
                    (Some(_), None) => {
                        compiler.errors.push(SpannedCompileError {
                            error: CompileError::BUG("Failed to generate forward mapping"),
                            span,
                            notes: vec![],
                        });
                    }
                    _ => {}
                }
            }
            OntolMapArms::Abstract(upper, lower) => {
                proc_table.metadata_table.insert(
                    MapKey {
                        input: upper.into(),
                        output: lower.into(),
                        flags: MapFlags::default(),
                    },
                    MapOutputMeta {
                        direction: MapDirection::Down,
                        lossiness: MapLossiness::Complete,
                    },
                );
                proc_table.metadata_table.insert(
                    MapKey {
                        input: lower.into(),
                        output: upper.into(),
                        flags: MapFlags::default(),
                    },
                    MapOutputMeta {
                        direction: MapDirection::Up,
                        lossiness: MapLossiness::Complete,
                    },
                );
            }
        }
    }
}

fn generate_ontol_map_procs<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    direction: MapDirection,
    externed_outputs: &FnvHashSet<DefId>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) -> Option<MapKey> {
    let needs_pure_partial = match expr.as_ref().kind() {
        ontol_hir::Kind::Struct(_, flags, _) if flags.contains(StructFlags::MATCH) => expr
            .as_ref()
            .meta()
            .ty
            .get_single_def_id()
            .and_then(|def_id| compiler.relations.properties_by_def_id(def_id))
            .map(|properties| properties.identified_by.is_some())
            .unwrap_or(false),
        _ => false,
    };

    let key = generate_map_proc(
        scope,
        expr,
        direction,
        MapFlags::empty(),
        externed_outputs,
        proc_table,
        compiler,
    );

    if needs_pure_partial {
        let _entered = debug_span!("pure").entered();
        generate_map_proc(
            scope,
            expr,
            direction,
            MapFlags::PURE_PARTIAL,
            externed_outputs,
            proc_table,
            compiler,
        );
    }

    if let Some(key) = key {
        let needs_static_condition = scope
            .arena()
            .iter_data()
            .any(|hir_data| matches!(hir_data.hir(), ontol_hir::Kind::Narrow(_)));

        if needs_static_condition {
            let _entered = debug_span!("cond").entered();

            let condition = generate_static_condition_from_scope(scope, compiler);

            proc_table.static_conditions.insert(key, condition);
        }
    }

    key
}

fn generate_map_proc<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    direction: MapDirection,
    map_flags: MapFlags,
    externed_outputs: &FnvHashSet<DefId>,
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
        if let Some(output_def_id) = expr.data().ty().get_single_def_id() {
            if externed_outputs.contains(&output_def_id) {
                debug!("Direction is covered by extern; skipping codegen");
                return None;
            }
        }
    }

    debug!("body type: {:?}", func.body.data().ty());

    let key = map_codegen(proc_table, &func, map_flags, direction, compiler);

    Some(key)
}

fn generate_extern_map(
    input: MapDef,
    output: MapDef,
    extern_def_id: DefId,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler,
) {
    let key = MapKey {
        input,
        output,
        flags: MapFlags::empty(),
    };

    let span = compiler.defs.def_span(extern_def_id);

    let mut builder = ProcBuilder::new(NParams(0));
    let mut root_block = builder.new_block(Delta(1), span);
    root_block.op(
        OpCode::CallExtern(extern_def_id, output.def_id),
        Delta(0),
        span,
        &mut builder,
    );
    root_block.commit(Terminator::Return, &mut builder);

    proc_table.map_procedures.insert(key, builder);
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

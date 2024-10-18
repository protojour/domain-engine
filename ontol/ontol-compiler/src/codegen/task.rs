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
    var::VarAllocator,
    vm::proc::{Address, Lib, NParams, OpCode, Procedure},
    DefId, DomainIndex, MapDef, MapDirection, MapFlags, MapKey,
};
use tracing::{debug, debug_span, warn};

use crate::{
    codegen::{
        code_generator::map_codegen, static_condition::generate_static_condition_from_scope,
    },
    def::{DefKind, Defs},
    hir_unify::{unify_to_function, UnifierError, UnifierResult},
    map::UndirectedMapKey,
    pattern::PatId,
    type_check::MapArmsKind,
    typed_hir::TypedRootNode,
    types::Type,
    CompileError, CompileErrors, Compiler, Note, SourceSpan, SpannedCompileError,
};

use super::{
    auto_map::autogenerate_mapping,
    code_generator::const_codegen,
    ir::Terminator,
    link::{link, LinkResult},
    proc_builder::{Delta, ProcBuilder},
    union_map_generator::generate_union_maps,
};

/// The code context tracks all structures related to code that needs to run on ONTOL VM,
/// and other information resulting from mapping of definitions.
#[derive(Default)]
pub struct CodeCtx<'m> {
    const_tasks: Vec<ConstCodegenTask<'m>>,
    map_tasks: IndexMap<UndirectedMapKey, MapCodegenTask<'m>>,

    pub abstract_templates: IndexMap<UndirectedMapKey, AbstractTemplate>,

    pub result_lib: Lib,
    pub result_const_procs: FnvHashMap<DefId, Procedure>,
    pub result_map_proc_table: FnvHashMap<MapKey, Procedure>,
    pub result_named_downmaps: FnvHashMap<(DomainIndex, TextConstant), MapKey>,
    pub result_propflow_table: FnvHashMap<MapKey, Vec<PropertyFlow>>,
    pub result_static_conditions: FnvHashMap<MapKey, Condition>,
    pub result_metadata_table: FnvHashMap<MapKey, MapOutputMeta>,
}

pub struct AbstractTemplate {
    pub pat_ids: [PatId; 2],
    pub var_allocator: VarAllocator,
}

pub struct AbstractTemplateApplication {
    pub pat_ids: [PatId; 2],
    pub def_ids: [DefId; 2],
    pub var_allocator: VarAllocator,
}

impl<'m> CodeCtx<'m> {
    pub fn add_map_task(
        &mut self,
        pair: UndirectedMapKey,
        request: MapCodegenRequest<'m>,
        defs: &Defs,
        errors: &mut CompileErrors,
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
                        MapCodegenTask::Explicit(old),
                        MapCodegenRequest::ExplicitOntol(ontol_map),
                    ) => {
                        if let Some(old_ontol_map) = &old.ontol_map {
                            CompileError::ConflictingMap
                                .span(defs.def_span(ontol_map.map_def_id))
                                .with_note(
                                    Note::AlreadyDefinedHere
                                        .span(defs.def_span(old_ontol_map.map_def_id)),
                                )
                                .report(errors);
                        } else {
                            old.ontol_map = Some(ontol_map);
                        }
                    }
                    (
                        MapCodegenTask::Explicit(expl),
                        MapCodegenRequest::ExternForward(extern_map),
                    ) => {
                        expl.forward_extern = Some(extern_map);
                    }
                    (
                        MapCodegenTask::Explicit(expl),
                        MapCodegenRequest::ExternBackward(extern_map),
                    ) => {
                        expl.backward_extern = Some(extern_map);
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
    Auto(DomainIndex),
    ExplicitOntol(OntolMap<'m>),
    ExternForward(ExternMap),
    ExternBackward(ExternMap),
}

/// A native ontol `map` with full body expressed in ontol-hir
pub struct OntolMap<'m> {
    pub map_def_id: DefId,
    pub arms: OntolMapArms<'m>,
    pub span: SourceSpan,
}

pub enum OntolMapArms<'m> {
    Patterns([TypedRootNode<'m>; 2]),
    Abstract([DefId; 2]),
    Template(AbstractTemplateApplication),
}

pub(super) enum MapCodegenTask<'m> {
    Auto(DomainIndex),
    Explicit(ExplicitMapCodegenTask<'m>),
}

pub(super) struct ExplicitMapCodegenTask<'m> {
    /// The native ONTOL mapping, if any
    pub ontol_map: Option<OntolMap<'m>>,
    /// extern forward override of the ontol mapping.
    /// note: The direction is relative to [UndirectedMapKey].
    pub forward_extern: Option<ExternMap>,
    /// extern forward override of the ontol mapping.
    /// note: The direction is relative to [UndirectedMapKey].
    pub backward_extern: Option<ExternMap>,
}

impl<'m> ExplicitMapCodegenTask<'m> {
    fn domain_index(&self) -> Option<DomainIndex> {
        if let Some(ontol_map) = self.ontol_map.as_ref() {
            Some(ontol_map.map_def_id.domain_index())
        } else if let Some(ext) = self.forward_extern.as_ref() {
            Some(ext.map_def_id.domain_index())
        } else {
            self.backward_extern
                .as_ref()
                .map(|ext| ext.map_def_id.domain_index())
        }
    }
}

pub struct ExternMap {
    pub extern_def_id: DefId,
    pub map_def_id: DefId,
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
                    dbg.field("upper", &DebugViaDisplay(&arms[0]));
                    dbg.field("lower", &DebugViaDisplay(&arms[1]));
                }
                OntolMapArms::Abstract([upper, lower]) => {
                    dbg.field("upper", &upper);
                    dbg.field("lower", &lower);
                }
                OntolMapArms::Template(template) => {
                    dbg.field("upper", &template.pat_ids[0]);
                    dbg.field("lower", &template.pat_ids[1]);
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
    pub named_downmaps: FnvHashMap<(DomainIndex, TextConstant), MapKey>,
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
    let mut proc_table = ProcTable::default();

    let mut run = 0;

    while !compiler.code_ctx.map_tasks.is_empty() || !compiler.code_ctx.const_tasks.is_empty() {
        let mut explicit_map_tasks = Vec::with_capacity(compiler.code_ctx.map_tasks.len());

        for (key, task) in std::mem::take(&mut compiler.code_ctx.map_tasks) {
            match task {
                MapCodegenTask::Auto(domain_index) => {
                    let _entered =
                        debug_span!("auto_map", run, idx = ?domain_index.index()).entered();

                    if let Some(task) = autogenerate_mapping(key, domain_index, compiler) {
                        explicit_map_tasks.push((key, task));
                    }
                }
                MapCodegenTask::Explicit(explicit) => {
                    explicit_map_tasks.push((key, explicit));
                }
            }
        }

        for ConstCodegenTask { def_id, node } in std::mem::take(&mut compiler.code_ctx.const_tasks)
        {
            const_codegen(node, def_id, &mut proc_table, compiler);
        }

        for (key, task) in explicit_map_tasks {
            let _entered =
                debug_span!("map", run, idx = ?task.domain_index().unwrap_or(DomainIndex::ontol()).index())
                    .entered();

            generate_explicit_map(key, task, &mut proc_table, compiler);
        }

        run += 1;
    }

    generate_union_maps(&mut proc_table, compiler);

    let LinkResult {
        lib,
        const_procs,
        map_proc_table,
    } = link(compiler, &mut proc_table);

    compiler.code_ctx.result_lib = lib;
    compiler.code_ctx.result_const_procs = const_procs;
    compiler.code_ctx.result_map_proc_table = map_proc_table;
    compiler.code_ctx.result_named_downmaps = proc_table.named_downmaps;
    compiler.code_ctx.result_static_conditions = proc_table.static_conditions;
    compiler.code_ctx.result_propflow_table = proc_table.propflow_table;
    compiler.code_ctx.result_metadata_table = proc_table.metadata_table;
}

fn generate_explicit_map<'m>(
    undirected_key: UndirectedMapKey,
    codegen_task: ExplicitMapCodegenTask<'m>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) {
    let ExplicitMapCodegenTask {
        ontol_map,
        forward_extern,
        backward_extern,
    } = codegen_task;

    let mut externed_outputs = FnvHashSet::default();

    fn unknown_extern_map_direction(map_def_id: DefId, compiler: &mut Compiler) {
        let span = compiler.defs.def_span(map_def_id);
        CompileError::ExternMapUnknownDirection
            .span(span)
            .with_note(Note::AbtractMapSuggestion.span(span))
            .report(compiler);
    }

    // the extern directions are in relation to the _undirected key_, not the ontol map arms!
    if let Some(extern_map) = forward_extern {
        if ontol_map.is_none() {
            unknown_extern_map_direction(extern_map.map_def_id, compiler);
        }

        generate_extern_map(
            undirected_key.first(),
            undirected_key.second(),
            extern_map.extern_def_id,
            proc_table,
            compiler,
        );
        externed_outputs.insert(undirected_key.second().def_id);
    }
    if let Some(extern_map) = backward_extern {
        if ontol_map.is_none() {
            unknown_extern_map_direction(extern_map.map_def_id, compiler);
        }

        generate_extern_map(
            undirected_key.second(),
            undirected_key.first(),
            extern_map.extern_def_id,
            proc_table,
            compiler,
        );
        externed_outputs.insert(undirected_key.first().def_id);
    }

    if let Some(OntolMap {
        map_def_id,
        arms,
        span,
    }) = ontol_map
    {
        match arms {
            OntolMapArms::Patterns(arms) => {
                generate_pattern_map(
                    map_def_id,
                    arms,
                    span,
                    &externed_outputs,
                    proc_table,
                    compiler,
                );
            }
            OntolMapArms::Abstract([upper, lower]) => {
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
            OntolMapArms::Template(application) => {
                let _entered = debug_span!("tmpl").entered();

                let map_def_span = compiler.defs.def_span(map_def_id);
                let _ = compiler.type_check().check_map(
                    (map_def_id, map_def_span),
                    &application.var_allocator,
                    application.pat_ids,
                    MapArmsKind::Template(application.def_ids),
                );
            }
        }
    }
}

fn generate_pattern_map<'m>(
    map_def_id: DefId,
    arms: [TypedRootNode<'m>; 2],
    span: SourceSpan,
    externed_outputs: &FnvHashSet<DefId>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) {
    debug!("1st (ty={:?}):\n{}", arms[0].data().ty(), arms[0]);
    debug!("2nd (ty={:?}):\n{}", arms[1].data().ty(), arms[1]);

    let down_result = {
        let _entered = debug_span!("down").entered();
        generate_ontol_map_procs(
            &arms[0],
            &arms[1],
            MapDirection::Down,
            externed_outputs,
            proc_table,
            compiler,
        )
    };

    let up_result = {
        let _entered = debug_span!("up").entered();
        generate_ontol_map_procs(
            &arms[1],
            &arms[0],
            MapDirection::Up,
            externed_outputs,
            proc_table,
            compiler,
        )
    };

    match (compiler.map_ident(map_def_id), down_result.clone()) {
        (Some(ident), Ok(Some(down_key))) => {
            let ident_constant = compiler.str_ctx.intern_constant(ident);
            proc_table
                .named_downmaps
                .insert((map_def_id.domain_index(), ident_constant), down_key);
        }
        (Some(_), _) => {
            CompileError::BUG("Failed to generate forward mapping")
                .span(span)
                .report(compiler);
        }
        _ => {}
    }

    if let (Err(down), Err(up)) = (down_result, up_result) {
        if down == up {
            if let Some(error) = unifier_error_to_compiler(down) {
                error.report(compiler);
            }
        } else {
            if let Some(down) = unifier_error_to_compiler(down) {
                down.report(compiler);
            }
            if let Some(up) = unifier_error_to_compiler(up) {
                up.report(compiler);
            }
        }
    }
}

fn unifier_error_to_compiler(error: UnifierError) -> Option<SpannedCompileError> {
    match error {
        UnifierError::PatternRequiresIteratedVariable(span) => {
            Some(CompileError::PatternRequiresIteratedVariable.span(span))
        }
        _ => None,
    }
}

fn generate_ontol_map_procs<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    direction: MapDirection,
    externed_outputs: &FnvHashSet<DefId>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<Option<MapKey>> {
    let needs_pure_partial = match expr.as_ref().kind() {
        ontol_hir::Kind::Struct(_, flags, _) if flags.contains(StructFlags::MATCH) => expr
            .as_ref()
            .meta()
            .ty
            .get_single_def_id()
            .and_then(|def_id| compiler.prop_ctx.properties_by_def_id(def_id))
            .map(|properties| properties.identified_by.is_some())
            .unwrap_or(false),
        _ => false,
    };

    let key_result = generate_map_proc(
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
        let _ = generate_map_proc(
            scope,
            expr,
            direction,
            MapFlags::PURE_PARTIAL,
            externed_outputs,
            proc_table,
            compiler,
        );
    }

    if let Ok(Some(key)) = key_result {
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

    key_result
}

fn generate_map_proc<'m>(
    scope: &TypedRootNode<'m>,
    expr: &TypedRootNode<'m>,
    direction: MapDirection,
    map_flags: MapFlags,
    externed_outputs: &FnvHashSet<DefId>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<Option<MapKey>> {
    let func = match unify_to_function(scope, expr, direction, map_flags, compiler) {
        Ok(func) => func,
        Err(err) => {
            debug!("unifier error: {err:?}");
            return Err(err);
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
                return Ok(None);
            }
        }
    }

    debug!(ty = ?func.body.data().ty(), "body");

    let key = map_codegen(proc_table, &func, map_flags, direction, compiler);

    Ok(Some(key))
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

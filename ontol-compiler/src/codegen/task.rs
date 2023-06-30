use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use fnv::FnvHashMap;
use ontol_hir::VarAllocator;
use ontol_runtime::{
    format_utils::DebugViaDisplay,
    vm::proc::{Address, Lib, Procedure},
    DefId, MapKey,
};
use tracing::{debug, warn};

use crate::{
    codegen::{code_generator::map_codegen, type_mapper::TypeMapper},
    hir_unify::unify_to_function,
    mem::Intern,
    patterns::StringPatternSegment,
    relation::Constructor,
    typed_hir::{Meta, TypedHirNode},
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

    let LinkResult {
        lib,
        const_procs,
        map_procs,
    } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_const_procs = const_procs;
    compiler.codegen_tasks.result_map_procs = map_procs;
}

fn autogenerate_mapping<'m>(
    key: AutoMapKey,
    compiler: &mut Compiler<'m>,
) -> Option<CodegenTask<'m>> {
    let first_def_id = key.first();
    let second_def_id = key.second();
    let unit_type = compiler.types.intern(Type::Unit(DefId::unit()));

    let first_properties = compiler.relations.properties_by_def_id(first_def_id)?;
    let second_properties = compiler.relations.properties_by_def_id(second_def_id)?;

    let first_segment = match &first_properties.constructor {
        Constructor::StringFmt(segment) => segment,
        _ => return None,
    };
    let second_segment = match &second_properties.constructor {
        Constructor::StringFmt(segment) => segment,
        _ => return None,
    };

    let mut var_allocator = VarAllocator::default();
    let first_var = var_allocator.alloc();
    let second_var = var_allocator.alloc();
    let mut var_map = Default::default();

    let first_node = autogenerate_fmt_hir_struct(
        Some(&mut var_allocator),
        first_def_id,
        first_var,
        first_segment,
        &mut var_map,
        unit_type,
        compiler,
    )?;
    let second_node = autogenerate_fmt_hir_struct(
        None,
        second_def_id,
        second_var,
        second_segment,
        &mut var_map,
        unit_type,
        compiler,
    )?;

    Some(CodegenTask::Map(MapCodegenTask {
        first: first_node,
        second: second_node,
        span: SourceSpan::none(),
    }))
}

fn autogenerate_fmt_hir_struct<'m>(
    mut var_allocator: Option<&mut VarAllocator>,
    def_id: DefId,
    binder_var: ontol_hir::Var,
    segment: &StringPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    unit_type: TypeRef<'m>,
    compiler: &Compiler<'m>,
) -> Option<TypedHirNode<'m>> {
    let mut nodes: Vec<TypedHirNode<'m>> = vec![];

    if let StringPatternSegment::Concat(segments) = segment {
        for child_segment in segments {
            if let Some(node) = autogenerate_fmt_segment_property(
                &mut var_allocator,
                binder_var,
                child_segment,
                var_map,
                unit_type,
                compiler,
            ) {
                nodes.push(node);
            }
        }
    }

    let ty = compiler.def_types.map.get(&def_id)?;

    Some(TypedHirNode(
        ontol_hir::Kind::Struct(ontol_hir::Binder(binder_var), nodes),
        Meta {
            ty,
            span: SourceSpan::none(),
        },
    ))
}

fn autogenerate_fmt_segment_property<'m>(
    mut var_allocator: &mut Option<&mut VarAllocator>,
    binder_var: ontol_hir::Var,
    segment: &StringPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    unit_type: TypeRef<'m>,
    compiler: &Compiler<'m>,
) -> Option<TypedHirNode<'m>> {
    if let StringPatternSegment::Property {
        property_id,
        type_def_id,
        segment: _,
    } = segment
    {
        let object_ty = compiler.def_types.map.get(type_def_id)?;
        let meta = Meta {
            ty: object_ty,
            span: SourceSpan::none(),
        };

        let var_node = if let Some(var_allocator) = &mut var_allocator {
            // first arm
            let var = var_allocator.alloc();
            var_map.insert(*type_def_id, var);
            TypedHirNode(ontol_hir::Kind::Var(var), meta)
        } else {
            // second arm
            if let Some(var) = var_map.get(type_def_id) {
                TypedHirNode(ontol_hir::Kind::Var(*var), meta)
            } else {
                return None;
            }
        };

        Some(TypedHirNode(
            ontol_hir::Kind::Prop(
                ontol_hir::Optional(false),
                binder_var,
                *property_id,
                vec![ontol_hir::PropVariant {
                    dimension: ontol_hir::Dimension::Singular,
                    attr: ontol_hir::Attribute {
                        rel: Box::new(TypedHirNode(
                            ontol_hir::Kind::Unit,
                            Meta {
                                ty: unit_type,
                                span: SourceSpan::none(),
                            },
                        )),
                        val: Box::new(var_node),
                    },
                }],
            ),
            Meta {
                ty: object_ty,
                span: SourceSpan::none(),
            },
        ))
    } else {
        None
    }
}

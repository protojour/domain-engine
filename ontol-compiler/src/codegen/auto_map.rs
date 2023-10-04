use std::collections::HashMap;

use ontol_hir::VarAllocator;
use ontol_runtime::{DefId, PackageId};

use crate::{
    def::DefKind,
    map::MapKeyPair,
    relation::Constructor,
    text_patterns::TextPatternSegment,
    typed_hir::{
        IntoTypedHirData, Meta, TypedArena, TypedHir, TypedHirData, TypedRootNode, UNIT_META,
    },
    Compiler, NO_SPAN,
};

use super::task::ExplicitMapCodegenTask;

pub fn autogenerate_mapping<'m>(
    key_pair: MapKeyPair,
    package_id: PackageId,
    compiler: &mut Compiler<'m>,
) -> Option<ExplicitMapCodegenTask<'m>> {
    let first_def_id = key_pair[0].def_id;
    let second_def_id = key_pair[1].def_id;

    let first_properties = compiler.relations.properties_by_def_id(first_def_id)?;
    let second_properties = compiler.relations.properties_by_def_id(second_def_id)?;

    match (
        &first_properties.constructor,
        &second_properties.constructor,
    ) {
        (Constructor::TextFmt(first_fmt), Constructor::TextFmt(second_fmt)) => {
            let arms = autogenerate_fmt_to_fmt(
                (first_def_id, first_fmt),
                (second_def_id, second_fmt),
                compiler,
            )?;

            Some(ExplicitMapCodegenTask {
                def_id: compiler
                    .defs
                    .add_def(DefKind::AutoMapping, package_id, NO_SPAN),
                arms,
                span: NO_SPAN,
            })
        }
        _ => None,
    }
}

fn autogenerate_fmt_to_fmt<'m>(
    first: (DefId, &TextPatternSegment),
    second: (DefId, &TextPatternSegment),
    compiler: &Compiler<'m>,
) -> Option<[TypedRootNode<'m>; 2]> {
    let mut var_allocator = VarAllocator::default();
    let first_var = var_allocator.alloc();
    let second_var = var_allocator.alloc();
    let mut var_map = Default::default();

    let first_node = autogenerate_fmt_hir_struct(
        Some(&mut var_allocator),
        first.0,
        first_var,
        first.1,
        &mut var_map,
        compiler,
    )?;
    let second_node =
        autogenerate_fmt_hir_struct(None, second.0, second_var, second.1, &mut var_map, compiler)?;

    Some([first_node, second_node])
}

fn autogenerate_fmt_hir_struct<'m>(
    mut var_allocator: Option<&mut VarAllocator>,
    def_id: DefId,
    binder_var: ontol_hir::Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    compiler: &Compiler<'m>,
) -> Option<ontol_hir::RootNode<'m, TypedHir>> {
    let mut arena: TypedArena<'m> = Default::default();
    let mut nodes: Vec<ontol_hir::Node> = vec![];

    if let TextPatternSegment::Concat(segments) = segment {
        for child_segment in segments {
            if let Some(node) = autogenerate_fmt_segment_property(
                &mut var_allocator,
                binder_var,
                child_segment,
                var_map,
                compiler,
                &mut arena,
            ) {
                nodes.push(node);
            }
        }
    }

    let ty = compiler.def_types.table.get(&def_id)?;

    let struct_node = arena.add(TypedHirData(
        ontol_hir::Kind::Struct(
            ontol_hir::Binder { var: binder_var }.with_meta(Meta {
                ty: compiler.def_types.table.get(&def_id).unwrap(),
                span: NO_SPAN,
            }),
            ontol_hir::StructFlags::empty(),
            nodes.into(),
        ),
        Meta { ty, span: NO_SPAN },
    ));

    Some(ontol_hir::RootNode::new(struct_node, arena))
}

fn autogenerate_fmt_segment_property<'m>(
    mut var_allocator: &mut Option<&mut VarAllocator>,
    binder_var: ontol_hir::Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    compiler: &Compiler<'m>,
    arena: &mut TypedArena<'m>,
) -> Option<ontol_hir::Node> {
    if let TextPatternSegment::Property {
        property_id,
        type_def_id,
        segment: _,
    } = segment
    {
        let object_ty = compiler.def_types.table.get(type_def_id)?;
        let meta = Meta {
            ty: object_ty,
            span: NO_SPAN,
        };

        let var_node = if let Some(var_allocator) = &mut var_allocator {
            // first arm
            let var = var_allocator.alloc();
            var_map.insert(*type_def_id, var);
            arena.add(TypedHirData(ontol_hir::Kind::Var(var), meta))
        } else {
            // second arm
            if let Some(var) = var_map.get(type_def_id) {
                arena.add(TypedHirData(ontol_hir::Kind::Var(*var), meta))
            } else {
                return None;
            }
        };

        let rel = arena.add(TypedHirData(ontol_hir::Kind::Unit, UNIT_META));

        Some(
            arena.add(TypedHirData(
                ontol_hir::Kind::Prop(
                    ontol_hir::Optional(false),
                    binder_var,
                    *property_id,
                    [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                        rel,
                        val: var_node,
                    })]
                    .into(),
                ),
                Meta {
                    ty: object_ty,
                    span: NO_SPAN,
                },
            )),
        )
    } else {
        None
    }
}

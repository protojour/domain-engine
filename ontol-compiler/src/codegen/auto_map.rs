use std::collections::HashMap;

use ontol_hir::VarAllocator;
use ontol_runtime::DefId;

use crate::{
    mem::Intern,
    primitive::PrimitiveKind,
    relation::Constructor,
    text_patterns::TextPatternSegment,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef},
    Compiler, NO_SPAN,
};

use super::task::{ExplicitMapCodegenTask, MapArm, MapKeyPair};

pub fn autogenerate_mapping<'m>(
    key_pair: MapKeyPair,
    compiler: &mut Compiler<'m>,
) -> Option<ExplicitMapCodegenTask<'m>> {
    let first_def_id = key_pair.first().def_id;
    let second_def_id = key_pair.second().def_id;

    let unit_type = compiler
        .types
        .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()));

    let first_properties = compiler.relations.properties_by_def_id(first_def_id)?;
    let second_properties = compiler.relations.properties_by_def_id(second_def_id)?;

    match (
        &first_properties.constructor,
        &second_properties.constructor,
    ) {
        (Constructor::TextFmt(first_fmt), Constructor::TextFmt(second_fmt)) => {
            autogenerate_fmt_to_fmt(
                compiler,
                unit_type,
                (first_def_id, first_fmt),
                (second_def_id, second_fmt),
            )
        }
        _ => None,
    }
}

fn autogenerate_fmt_to_fmt<'m>(
    compiler: &Compiler<'m>,
    unit_type: TypeRef<'m>,
    first: (DefId, &TextPatternSegment),
    second: (DefId, &TextPatternSegment),
) -> Option<ExplicitMapCodegenTask<'m>> {
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
        unit_type,
        compiler,
    )?;
    let second_node = autogenerate_fmt_hir_struct(
        None,
        second.0,
        second_var,
        second.1,
        &mut var_map,
        unit_type,
        compiler,
    )?;

    Some(ExplicitMapCodegenTask {
        first: MapArm {
            is_match: false,
            node: first_node,
        },
        second: MapArm {
            is_match: false,
            node: second_node,
        },
        span: NO_SPAN,
    })
}

fn autogenerate_fmt_hir_struct<'m>(
    mut var_allocator: Option<&mut VarAllocator>,
    def_id: DefId,
    binder_var: ontol_hir::Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    unit_type: TypeRef<'m>,
    compiler: &Compiler<'m>,
) -> Option<TypedHirNode<'m>> {
    let mut nodes: Vec<TypedHirNode<'m>> = vec![];

    if let TextPatternSegment::Concat(segments) = segment {
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

    let ty = compiler.def_types.table.get(&def_id)?;

    Some(TypedHirNode(
        ontol_hir::Kind::Struct(
            TypedBinder {
                var: binder_var,
                meta: Meta {
                    ty: compiler.def_types.table.get(&def_id).unwrap(),
                    span: NO_SPAN,
                },
            },
            ontol_hir::StructFlags::empty(),
            nodes,
        ),
        Meta { ty, span: NO_SPAN },
    ))
}

fn autogenerate_fmt_segment_property<'m>(
    mut var_allocator: &mut Option<&mut VarAllocator>,
    binder_var: ontol_hir::Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, ontol_hir::Var>,
    unit_type: TypeRef<'m>,
    compiler: &Compiler<'m>,
) -> Option<TypedHirNode<'m>> {
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
                vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                    rel: Box::new(TypedHirNode(
                        ontol_hir::Kind::Unit,
                        Meta {
                            ty: unit_type,
                            span: NO_SPAN,
                        },
                    )),
                    val: Box::new(var_node),
                })],
            ),
            Meta {
                ty: object_ty,
                span: NO_SPAN,
            },
        ))
    } else {
        None
    }
}

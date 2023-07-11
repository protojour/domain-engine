use std::collections::HashMap;

use ontol_hir::VarAllocator;
use ontol_runtime::DefId;
use tracing::warn;

use crate::{
    def::{LookupRelationshipMeta, MapDirection},
    mem::Intern,
    patterns::StringPatternSegment,
    relation::Constructor,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef},
    Compiler, NO_SPAN,
};

use super::task::{ExplicitMapCodegenTask, MapKeyPair};

pub fn autogenerate_mapping<'m>(
    key_pair: MapKeyPair,
    compiler: &mut Compiler<'m>,
) -> Option<ExplicitMapCodegenTask<'m>> {
    let first_def_id = key_pair.first().def_id;
    let second_def_id = key_pair.second().def_id;

    let unit_type = compiler.types.intern(Type::Unit(DefId::unit()));

    let first_properties = compiler.relations.properties_by_def_id(first_def_id)?;
    let second_properties = compiler.relations.properties_by_def_id(second_def_id)?;

    match (
        &first_properties.constructor,
        &second_properties.constructor,
    ) {
        (Constructor::StringFmt(first_fmt), Constructor::StringFmt(second_fmt)) => {
            autogenerate_fmt_to_fmt(
                compiler,
                unit_type,
                (first_def_id, first_fmt),
                (second_def_id, second_fmt),
            )
        }
        (Constructor::Value(first_rel_id, ..), Constructor::Value(second_rel_id, ..)) => {
            let first_meta = compiler
                .defs
                .lookup_relationship_meta(*first_rel_id)
                .unwrap();
            let second_meta = compiler
                .defs
                .lookup_relationship_meta(*second_rel_id)
                .unwrap();

            let first_object = first_meta.relationship.object.0.def_id;
            let second_object = second_meta.relationship.object.0.def_id;

            if first_object == second_object {
                // This is trivial and just uses type punning
                None
            } else {
                warn!("TODO: Value to value - DIFFERENT OBJECTS");
                None
            }
        }
        _ => None,
    }
}

fn autogenerate_fmt_to_fmt<'m>(
    compiler: &Compiler<'m>,
    unit_type: TypeRef<'m>,
    first: (DefId, &StringPatternSegment),
    second: (DefId, &StringPatternSegment),
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
        direction: MapDirection::Omni,
        first: first_node,
        second: second_node,
        span: NO_SPAN,
    })
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

    let ty = compiler.def_types.table.get(&def_id)?;

    Some(TypedHirNode(
        ontol_hir::Kind::Struct(
            TypedBinder {
                var: binder_var,
                ty: compiler.def_types.table.get(&def_id).unwrap(),
            },
            nodes,
        ),
        Meta { ty, span: NO_SPAN },
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
                vec![ontol_hir::PropVariant {
                    dimension: ontol_hir::AttrDimension::Singular,
                    attr: ontol_hir::Attribute {
                        rel: Box::new(TypedHirNode(
                            ontol_hir::Kind::Unit,
                            Meta {
                                ty: unit_type,
                                span: NO_SPAN,
                            },
                        )),
                        val: Box::new(var_node),
                    },
                }],
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

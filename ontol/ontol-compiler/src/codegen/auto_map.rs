use std::collections::HashMap;

use fnv::FnvHashSet;
use ontol_runtime::{
    var::{Var, VarAllocator},
    DefId, PackageId,
};
use tracing::debug;

use crate::{
    def::DefKind,
    map::UndirectedMapKey,
    relation::Constructor,
    repr::repr_model::ReprKind,
    text_patterns::TextPatternSegment,
    thesaurus::TypeRelation,
    typed_hir::{IntoTypedHirData, Meta, TypedArena, TypedHir, TypedHirData, TypedRootNode},
    Compiler, NO_SPAN,
};

use super::task::{
    AbstractTemplate, AbstractTemplateApplication, ExplicitMapCodegenTask, OntolMap, OntolMapArms,
};

pub fn autogenerate_mapping<'m>(
    key_pair: UndirectedMapKey,
    package_id: PackageId,
    compiler: &mut Compiler<'m>,
) -> Option<ExplicitMapCodegenTask<'m>> {
    let first_def_id = key_pair[0].def_id;
    let second_def_id = key_pair[1].def_id;

    let first_properties = compiler.rel_ctx.properties_by_def_id(first_def_id)?;
    let second_properties = compiler.rel_ctx.properties_by_def_id(second_def_id)?;

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
                ontol_map: Some(OntolMap {
                    map_def_id: compiler
                        .defs
                        .add_def(DefKind::AutoMapping, package_id, NO_SPAN),
                    arms: OntolMapArms::Patterns(arms),
                    span: NO_SPAN,
                }),
                forward_extern: None,
                backward_extern: None,
            })
        }
        (Constructor::TextFmt(fmt), Constructor::Transparent) => {
            let [fmt_node, transparent_node] =
                autogenerate_fmt_to_transparent((first_def_id, fmt), second_def_id, compiler)?;

            Some(ExplicitMapCodegenTask {
                ontol_map: Some(OntolMap {
                    map_def_id: compiler
                        .defs
                        .add_def(DefKind::AutoMapping, package_id, NO_SPAN),
                    arms: OntolMapArms::Patterns([fmt_node, transparent_node]),
                    span: NO_SPAN,
                }),
                forward_extern: None,
                backward_extern: None,
            })
        }
        (Constructor::Transparent, Constructor::TextFmt(fmt)) => {
            let [fmt_node, transparent_node] =
                autogenerate_fmt_to_transparent((second_def_id, fmt), first_def_id, compiler)?;

            Some(ExplicitMapCodegenTask {
                ontol_map: Some(OntolMap {
                    map_def_id: compiler
                        .defs
                        .add_def(DefKind::AutoMapping, package_id, NO_SPAN),
                    arms: OntolMapArms::Patterns([transparent_node, fmt_node]),
                    span: NO_SPAN,
                }),
                forward_extern: None,
                backward_extern: None,
            })
        }
        (Constructor::Transparent, Constructor::Transparent) => {
            // search for abstract template
            let mut applicable_templates = vec![];

            let mut first_set: FnvHashSet<DefId> = Default::default();
            let mut second_set: FnvHashSet<DefId> = Default::default();

            for (first, _) in compiler.thesaurus.entries(first_def_id, &compiler.defs) {
                for (second, _) in compiler.thesaurus.entries(second_def_id, &compiler.defs) {
                    if matches!(
                        (&first.rel, &second.rel),
                        (TypeRelation::Super, TypeRelation::Super)
                    ) {
                        first_set.insert(first.def_id);
                        second_set.insert(second.def_id);

                        let undirected_key =
                            UndirectedMapKey::new([first.def_id.into(), second.def_id.into()]);

                        if let Some(template) =
                            compiler.code_ctx.abstract_templates.get(&undirected_key)
                        {
                            applicable_templates.push(AbstractTemplate {
                                pat_ids: template.pat_ids,
                                var_allocator: VarAllocator::from(
                                    *template.var_allocator.peek_next(),
                                ),
                            });
                        }
                    }
                }
            }

            if first_set == second_set && !first_set.is_empty() && !second_set.is_empty() {
                // Generate "pun" body
                let var = Var(0);
                let mut upper_arena: TypedArena<'m> = Default::default();
                let mut lower_arena: TypedArena<'m> = Default::default();

                fn pun_arm<'m>(
                    var: Var,
                    def_id: DefId,
                    arena: &mut TypedArena<'m>,
                    compiler: &mut Compiler<'m>,
                ) -> ontol_hir::Node {
                    let var = arena.add(TypedHirData(
                        ontol_hir::Kind::Var(var),
                        Meta {
                            ty: compiler.def_ty_ctx.def_table.get(&def_id).unwrap(),
                            span: NO_SPAN,
                        },
                    ));
                    arena.add(TypedHirData(
                        ontol_hir::Kind::Pun(var),
                        Meta {
                            ty: compiler.def_ty_ctx.def_table.get(&def_id).unwrap(),
                            span: NO_SPAN,
                        },
                    ))
                }

                let upper_node = pun_arm(var, first_def_id, &mut upper_arena, compiler);
                let lower_node = pun_arm(var, second_def_id, &mut lower_arena, compiler);

                return Some(ExplicitMapCodegenTask {
                    ontol_map: Some(OntolMap {
                        map_def_id: compiler.defs.add_def(
                            DefKind::AutoMapping,
                            package_id,
                            NO_SPAN,
                        ),
                        arms: OntolMapArms::Patterns([
                            ontol_hir::RootNode::new(upper_node, upper_arena),
                            ontol_hir::RootNode::new(lower_node, lower_arena),
                        ]),
                        span: NO_SPAN,
                    }),
                    forward_extern: None,
                    backward_extern: None,
                });
            }

            match applicable_templates.len() {
                0 => {
                    debug!("no applicable abstract template");
                    None
                }
                1 => {
                    let applied_template = applicable_templates.into_iter().next().unwrap();
                    let template_application = AbstractTemplateApplication {
                        pat_ids: applied_template.pat_ids,
                        def_ids: [first_def_id, second_def_id],
                        var_allocator: applied_template.var_allocator,
                    };

                    Some(ExplicitMapCodegenTask {
                        ontol_map: Some(OntolMap {
                            map_def_id: compiler.defs.add_def(
                                DefKind::AutoMapping,
                                package_id,
                                NO_SPAN,
                            ),
                            arms: OntolMapArms::Template(template_application),
                            span: NO_SPAN,
                        }),
                        forward_extern: None,
                        backward_extern: None,
                    })
                }
                _ => {
                    panic!();
                }
            }
        }
        _ => None,
    }
}

fn autogenerate_fmt_to_fmt<'m>(
    upper: (DefId, &TextPatternSegment),
    lower: (DefId, &TextPatternSegment),
    compiler: &Compiler<'m>,
) -> Option<[TypedRootNode<'m>; 2]> {
    let mut var_allocator = VarAllocator::default();
    let upper_var = var_allocator.alloc();
    let lower_var = var_allocator.alloc();
    let mut var_map = Default::default();

    let first_node = autogenerate_fmt_hir_struct(
        Some(&mut var_allocator),
        upper.0,
        upper_var,
        upper.1,
        &mut var_map,
        compiler,
    )?;
    let second_node =
        autogenerate_fmt_hir_struct(None, lower.0, lower_var, lower.1, &mut var_map, compiler)?;

    Some([first_node, second_node])
}

fn autogenerate_fmt_to_transparent<'m>(
    (fmt_def_id, segment): (DefId, &TextPatternSegment),
    transparent_def_id: DefId,
    compiler: &Compiler<'m>,
) -> Option<[TypedRootNode<'m>; 2]> {
    let mut var_allocator = VarAllocator::default();
    let var = var_allocator.alloc();
    let mut var_map = Default::default();
    let fmt_node = autogenerate_fmt_hir_struct(
        Some(&mut var_allocator),
        fmt_def_id,
        var,
        segment,
        &mut var_map,
        compiler,
    )?;

    let transparent_repr_kind = compiler.repr_ctx.get_repr_kind(&transparent_def_id)?;

    let transparent_var = match transparent_repr_kind {
        ReprKind::Scalar(scalar_def_id, ..) => *var_map.get(scalar_def_id)?,
        _ => *var_map.get(&transparent_def_id)?,
    };

    let transparent_node = {
        let mut arena: TypedArena<'m> = Default::default();
        let node = arena.add(TypedHirData(
            ontol_hir::Kind::Var(transparent_var),
            Meta::new(
                compiler.def_ty_ctx.def_table.get(&transparent_def_id)?,
                NO_SPAN,
            ),
        ));
        ontol_hir::RootNode::new(node, arena)
    };

    Some([fmt_node, transparent_node])
}

fn autogenerate_fmt_hir_struct<'m>(
    mut var_allocator: Option<&mut VarAllocator>,
    def_id: DefId,
    binder_var: Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, Var>,
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

    let ty = compiler.def_ty_ctx.def_table.get(&def_id)?;

    let struct_node = arena.add(TypedHirData(
        ontol_hir::Kind::Struct(
            ontol_hir::Binder { var: binder_var }.with_meta(Meta {
                ty: compiler.def_ty_ctx.def_table.get(&def_id).unwrap(),
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
    binder_var: Var,
    segment: &TextPatternSegment,
    var_map: &mut HashMap<DefId, Var>,
    compiler: &Compiler<'m>,
    arena: &mut TypedArena<'m>,
) -> Option<ontol_hir::Node> {
    if let TextPatternSegment::Property {
        rel_id,
        type_def_id,
        segment: _,
    } = segment
    {
        let object_ty = compiler.def_ty_ctx.def_table.get(type_def_id)?;
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

        Some(arena.add(TypedHirData(
            ontol_hir::Kind::Prop(
                ontol_hir::PropFlags::empty(),
                binder_var,
                *rel_id,
                ontol_hir::PropVariant::Unit(var_node),
            ),
            Meta::new(object_ty, NO_SPAN),
        )))
    } else {
        None
    }
}

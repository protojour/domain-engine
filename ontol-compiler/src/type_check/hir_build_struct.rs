use indexmap::IndexMap;
use ontol_hir::StructFlags;
use ontol_runtime::{
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    value::PropertyId,
    DefId, Role,
};
use tracing::debug;

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    mem::Intern,
    pattern::{PatternKind, StructPatternAttr, StructPatternModifier},
    primitive::PrimitiveKind,
    type_check::{hir_build::NodeInfo, repr::repr_model::ReprKind, TypeError},
    typed_hir::{Meta, TypedHir, TypedHirValue},
    types::{Type, TypeRef},
    CompileError, SourceSpan, NO_SPAN,
};

use super::{hir_build_ctx::HirBuildCtx, TypeCheck};

pub(super) struct StructInfo<'m> {
    pub struct_def_id: DefId,
    pub struct_ty: TypeRef<'m>,
    pub modifier: Option<StructPatternModifier>,
    pub parent_struct_flags: ontol_hir::StructFlags,
}

struct MatchAttribute {
    property_id: PropertyId,
    cardinality: Cardinality,
    rel_params_def: Option<DefId>,
    value_def: DefId,
    mentioned: bool,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn build_property_matcher(
        &mut self,
        StructInfo {
            struct_def_id,
            struct_ty,
            modifier,
            parent_struct_flags,
        }: StructInfo<'m>,
        pattern_attrs: &[StructPatternAttr],
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let properties = self.relations.properties_by_def_id(struct_def_id);

        let actual_struct_flags = match modifier {
            Some(StructPatternModifier::Match) => ontol_hir::StructFlags::MATCH,
            None => ontol_hir::StructFlags::empty(),
        } | parent_struct_flags;

        let struct_meta = Meta {
            ty: struct_ty,
            span,
        };

        match self.seal_ctx.get_repr_kind(&struct_def_id).unwrap() {
            ReprKind::Struct | ReprKind::Unit => {
                match properties.and_then(|props| props.table.as_ref()) {
                    Some(property_set) => {
                        let struct_binder: TypedHirValue<'_, ontol_hir::Binder> =
                            TypedHirValue(ctx.var_allocator.alloc().into(), struct_meta);

                        // The written attribute patterns should match against
                        // the relations defined on the type. Compute `MatchAttributes`:
                        let mut match_attributes = property_set
                            .iter()
                            .filter_map(|(property_id, _cardinality)| {
                                let meta = self.defs.relationship_meta(property_id.relationship_id);
                                let property_name = match property_id.role {
                                    Role::Subject => match meta.relation_def_kind.value {
                                        DefKind::TextLiteral(lit) => Some(*lit),
                                        _ => panic!("BUG: Expected named subject property"),
                                    },
                                    Role::Object => meta.relationship.object_prop,
                                };
                                let (_, owner_cardinality, _) =
                                    meta.relationship.by(property_id.role);
                                let (value_def_id, _, _) =
                                    meta.relationship.by(property_id.role.opposite());

                                property_name.map(|property_name| {
                                    (
                                        property_name,
                                        MatchAttribute {
                                            property_id: *property_id,
                                            cardinality: owner_cardinality,
                                            rel_params_def: match &meta.relationship.rel_params {
                                                RelParams::Type(def_id) => Some(*def_id),
                                                _ => None,
                                            },
                                            value_def: value_def_id,
                                            mentioned: false,
                                        },
                                    )
                                })
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut hir_props = Vec::with_capacity(pattern_attrs.len());

                        // Actually match the written attributes to the match attributes:
                        for pattern_attr in pattern_attrs {
                            let prop_node = self.build_struct_property_node(
                                struct_binder.value().var,
                                pattern_attr,
                                &mut match_attributes,
                                actual_struct_flags,
                                ctx,
                            );
                            if let Some(prop_node) = prop_node {
                                hir_props.push(prop_node);
                            }
                        }

                        // MatchAttributes that are not `mentioned` should be reported or auto-generated,
                        // Unless the pattern is a `match` pattern.
                        // A `match` pattern is only used for data extraction, so it doesn't
                        // matter if there are unmentioned properties.
                        if !actual_struct_flags.contains(ontol_hir::StructFlags::MATCH) {
                            self.handle_missing_struct_attributes(
                                struct_binder.value().var,
                                span,
                                match_attributes,
                                &mut hir_props,
                                ctx,
                            );
                        }

                        ctx.mk_node(
                            ontol_hir::Kind::Struct(
                                struct_binder,
                                actual_struct_flags,
                                hir_props.into(),
                            ),
                            struct_meta,
                        )
                    }
                    None => {
                        if !pattern_attrs.is_empty() {
                            return self.error_node(CompileError::NoPropertiesExpected, &span, ctx);
                        }
                        ctx.mk_node(ontol_hir::Kind::Unit, struct_meta)
                    }
                }
            }
            ReprKind::StructIntersection(members) => {
                let mut member_iter = members.iter();
                let single_def_id = match member_iter.next() {
                    Some((def_id, _span)) => {
                        if member_iter.next().is_some() {
                            todo!("More members");
                        }
                        *def_id
                    }
                    None => panic!("No members"),
                };

                let value_object_ty = self.check_def_sealed(single_def_id);
                debug!("value_object_ty: {value_object_ty:?}");

                match value_object_ty {
                    Type::Domain(def_id) => self.build_property_matcher(
                        StructInfo {
                            struct_def_id: *def_id,
                            struct_ty: value_object_ty,
                            modifier,
                            parent_struct_flags,
                        },
                        pattern_attrs,
                        span,
                        ctx,
                    ),
                    _ => {
                        let mut attributes = pattern_attrs.iter();
                        match attributes.next() {
                            Some(StructPatternAttr {
                                key: (def_id, _),
                                rel: _,
                                bind_option: _,
                                value,
                            }) if *def_id == DefId::unit() => {
                                let object_ty = self.check_def_sealed(single_def_id);
                                let inner_node = self.build_node(
                                    value,
                                    NodeInfo {
                                        expected_ty: Some(object_ty),
                                        parent_struct_flags,
                                    },
                                    ctx,
                                );

                                *ctx.hir_arena[inner_node].meta_mut() = struct_meta;

                                inner_node
                            }
                            _ => {
                                self.error_node(CompileError::ExpectedPatternAttribute, &span, ctx)
                            }
                        }
                    }
                }
            }
            ReprKind::Scalar(scalar_def_id, ..) => {
                let scalar_def_id = *scalar_def_id;

                let scalar_object_ty = self.check_def_sealed(scalar_def_id);
                debug!("scalar_object_ty: {struct_def_id:?}: {scalar_object_ty:?}");

                let mut attributes = pattern_attrs.iter();
                match attributes.next() {
                    Some(StructPatternAttr {
                        key: (def_id, _),
                        rel: _,
                        bind_option: _,
                        value,
                    }) if *def_id == DefId::unit() => {
                        let object_ty = self.check_def_sealed(scalar_def_id);
                        let inner_node = self.build_node(
                            value,
                            NodeInfo {
                                expected_ty: Some(object_ty),
                                parent_struct_flags,
                            },
                            ctx,
                        );

                        *ctx.hir_arena[inner_node].meta_mut() = struct_meta;

                        inner_node
                    }
                    _ => self.error_node(CompileError::ExpectedPatternAttribute, &span, ctx),
                }
            }
            ReprKind::Union(_) | ReprKind::StructUnion(_) => {
                self.error_node(CompileError::CannotMapUnion, &span, ctx)
            }
            kind => todo!("{kind:?}"),
        }
    }

    fn build_struct_property_node(
        &mut self,
        struct_binder_var: ontol_hir::Var,
        attr: &StructPatternAttr,
        match_attributes: &mut IndexMap<&'m str, MatchAttribute>,
        actual_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Option<ontol_hir::Node> {
        let StructPatternAttr {
            key: (def_id, prop_span),
            rel,
            bind_option,
            value,
        } = attr;

        let DefKind::TextLiteral(attr_prop) = self.defs.def_kind(*def_id) else {
            self.error(CompileError::NamedPropertyExpected, prop_span);
            return None;
        };
        let Some(match_property) = match_attributes.get_mut(attr_prop) else {
            self.error(CompileError::UnknownProperty, prop_span);
            return None;
        };
        if match_property.mentioned {
            self.error(CompileError::DuplicateProperty, prop_span);
            return None;
        }
        match_property.mentioned = true;

        let rel_params_ty = match match_property.rel_params_def {
            Some(rel_def_id) => self.check_def_sealed(rel_def_id),
            None => self.unit_type(),
        };
        debug!("rel_params_ty: {rel_params_ty:?}");

        let rel_node = match (rel_params_ty, rel) {
            (Type::Primitive(PrimitiveKind::Unit, _), Some(rel)) => {
                self.error_node(CompileError::NoRelationParametersExpected, &rel.span, ctx)
            }
            (ty @ Type::Primitive(PrimitiveKind::Unit, _), None) => ctx.mk_node(
                ontol_hir::Kind::Unit,
                Meta {
                    ty,
                    span: *prop_span,
                },
            ),
            (_, Some(rel)) => self.build_node(
                rel,
                NodeInfo {
                    expected_ty: Some(rel_params_ty),
                    parent_struct_flags: actual_struct_flags,
                },
                ctx,
            ),
            (ty @ Type::Anonymous(def_id), None) => {
                match self.relations.properties_by_def_id(*def_id) {
                    Some(_) => self.build_implicit_rel_node(ty, value, *prop_span, ctx),
                    // An anonymous type without properties, i.e. just "meta relationships" about the relationship itself:
                    None => ctx.mk_node(
                        ontol_hir::Kind::Unit,
                        Meta {
                            ty,
                            span: *prop_span,
                        },
                    ),
                }
            }
            (ty, None) => self.build_implicit_rel_node(ty, value, *prop_span, ctx),
        };

        let value_ty = self.check_def_sealed(match_property.value_def);
        debug!("value_ty: {value_ty:?}");

        let prop_variant = match match_property.cardinality.1 {
            ValueCardinality::One => {
                let val_node = self.build_node(
                    value,
                    NodeInfo {
                        expected_ty: Some(value_ty),
                        parent_struct_flags: actual_struct_flags,
                    },
                    ctx,
                );
                ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                    rel: rel_node,
                    val: val_node,
                })
            }
            ValueCardinality::Many => match &value.kind {
                PatternKind::Seq(aggr_pat_id, pat_elements) => {
                    let mut hir_elements = Vec::with_capacity(pat_elements.len());
                    for element in pat_elements {
                        let val_node = self.build_node(
                            &element.pattern,
                            NodeInfo {
                                expected_ty: Some(value_ty),
                                parent_struct_flags: actual_struct_flags,
                            },
                            ctx,
                        );
                        hir_elements.push((
                            ontol_hir::Iter(element.iter),
                            ontol_hir::Attribute {
                                rel: rel_node,
                                val: val_node,
                            },
                        ));
                    }

                    let label = *ctx.label_map.get(aggr_pat_id).unwrap();
                    let seq_ty = self.types.intern(Type::Seq(rel_params_ty, value_ty));

                    ontol_hir::PropVariant::Seq(ontol_hir::SeqPropertyVariant {
                        label: TypedHirValue(
                            label,
                            Meta {
                                ty: seq_ty,
                                span: NO_SPAN,
                            },
                        ),
                        has_default: ontol_hir::HasDefault(matches!(
                            match_property.property_id.role,
                            Role::Object
                        )),
                        elements: hir_elements.into(),
                    })
                }
                _ => {
                    self.type_error(
                        TypeError::VariableMustBeSequenceEnclosed(value_ty),
                        &value.span,
                    );
                    return None;
                }
            },
        };

        let optional = ontol_hir::Optional(matches!(
            match_property.cardinality.0,
            PropertyCardinality::Optional
        ));

        let prop_variants: Vec<ontol_hir::PropVariant<'_, TypedHir>> =
            match match_property.cardinality.0 {
                PropertyCardinality::Mandatory => {
                    vec![prop_variant]
                }
                PropertyCardinality::Optional => {
                    if *bind_option {
                        vec![prop_variant]
                    } else {
                        ctx.partial = true;
                        self.error(
                            CompileError::TODO("required to be optional?".into()),
                            &value.span,
                        );
                        vec![]
                    }
                }
            };

        Some(ctx.mk_node(
            ontol_hir::Kind::Prop(
                optional,
                struct_binder_var,
                match_property.property_id,
                prop_variants.into(),
            ),
            Meta {
                ty: self.unit_type(),
                span: *prop_span,
            },
        ))
    }

    fn handle_missing_struct_attributes(
        &mut self,
        struct_binder_var: ontol_hir::Var,
        struct_span: SourceSpan,
        match_attributes: IndexMap<&'m str, MatchAttribute>,
        hir_props: &mut Vec<ontol_hir::Node>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (name, match_property) in match_attributes {
            if match_property.mentioned {
                continue;
            }
            if matches!(match_property.property_id.role, Role::Object) {
                continue;
            }
            if matches!(match_property.cardinality.0, PropertyCardinality::Optional) {
                continue;
            }

            let relationship_id = match_property.property_id.relationship_id;

            if let Some(const_def_id) = self
                .relations
                .default_const_objects
                .get(&relationship_id)
                .cloned()
            {
                // Generate code for default value.
                let value_ty = self.check_def_sealed(const_def_id);

                let prop_node = {
                    let rel = self.unit_node_no_span(ctx);
                    let val = ctx.mk_node(
                        ontol_hir::Kind::Const(const_def_id),
                        Meta {
                            ty: value_ty,
                            span: NO_SPAN,
                        },
                    );
                    ctx.mk_node(
                        ontol_hir::Kind::Prop(
                            ontol_hir::Optional(false),
                            struct_binder_var,
                            match_property.property_id,
                            [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel,
                                val,
                            })]
                            .into(),
                        ),
                        Meta {
                            ty: self.unit_type(),
                            span: NO_SPAN,
                        },
                    )
                };
                hir_props.push(prop_node);
                continue;
            }

            if self
                .relations
                .value_generators
                .get(&relationship_id)
                .is_some()
            {
                // Value generators should be handled in data storage,
                // so leave these fields out when not mentioned.
                continue;
            }

            ctx.missing_properties
                .entry(ctx.arm)
                .or_default()
                .entry(struct_span)
                .or_default()
                .push(name.into());
        }
    }
}

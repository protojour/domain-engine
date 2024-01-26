use indexmap::IndexMap;
use ontol_hir::StructFlags;
use ontol_runtime::{
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    value::PropertyId,
    var::Var,
    DefId, Role,
};
use tracing::debug;

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    mem::Intern,
    pattern::{
        CompoundPatternAttr, CompoundPatternAttrKind, CompoundPatternModifier, Pattern,
        PatternKind, SetBinaryOperator,
    },
    primitive::PrimitiveKind,
    relation::Property,
    repr::repr_model::ReprKind,
    type_check::{ena_inference::Strength, hir_build::NodeInfo, TypeError},
    typed_hir::{Meta, TypedHir, TypedHirData, UNIT_META},
    types::{Type, TypeRef, UNIT_TYPE},
    CompileError, SourceSpan, NO_SPAN,
};

use super::{hir_build_ctx::HirBuildCtx, TypeCheck};

pub(super) struct UnpackerInfo<'m> {
    pub type_def_id: DefId,
    pub ty: TypeRef<'m>,
    pub modifier: Option<CompoundPatternModifier>,
    pub is_unit_binding: bool,
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
    /// Build an ontol_hir node that unpacks some higher level compound pattern
    pub(super) fn build_unpacker(
        &mut self,
        UnpackerInfo {
            type_def_id,
            ty,
            modifier,
            is_unit_binding,
            parent_struct_flags,
        }: UnpackerInfo<'m>,
        pattern_attrs: &[CompoundPatternAttr],
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let property_set = self.relations.properties_table_by_def_id(type_def_id);

        let actual_struct_flags = match modifier {
            Some(CompoundPatternModifier::Match) => ontol_hir::StructFlags::MATCH,
            None => ontol_hir::StructFlags::empty(),
        } | parent_struct_flags;

        let hir_meta = Meta { ty, span };

        match self.seal_ctx.get_repr_kind(&type_def_id).unwrap() {
            ReprKind::Struct | ReprKind::Unit => match property_set {
                Some(property_set) => {
                    let mut match_attributes = IndexMap::new();
                    self.collect_match_attributes(property_set, &mut match_attributes);

                    self.build_struct_node(
                        pattern_attrs,
                        hir_meta,
                        match_attributes,
                        actual_struct_flags,
                        span,
                        ctx,
                    )
                }
                None => {
                    if !pattern_attrs.is_empty() {
                        return self.error_node(CompileError::NoPropertiesExpected, &span, ctx);
                    }
                    ctx.mk_node(ontol_hir::Kind::Unit, hir_meta)
                }
            },
            ReprKind::StructIntersection(members) => {
                let mut match_attributes = IndexMap::new();

                if let Some(property_set) = property_set {
                    self.collect_match_attributes(property_set, &mut match_attributes);
                }

                for (member_def_id, _) in members {
                    if let Some(property_set) =
                        self.relations.properties_table_by_def_id(*member_def_id)
                    {
                        self.collect_match_attributes(property_set, &mut match_attributes);
                    }
                }

                self.build_struct_node(
                    pattern_attrs,
                    hir_meta,
                    match_attributes,
                    actual_struct_flags,
                    span,
                    ctx,
                )
            }
            ReprKind::Scalar(..) => {
                let mut attributes = pattern_attrs.iter();
                match attributes.next() {
                    Some(CompoundPatternAttr {
                        key: (attr_def_id, _),
                        bind_option: _,
                        kind: CompoundPatternAttrKind::Value { rel: _, val },
                    }) if is_unit_binding => {
                        assert!(*attr_def_id == DefId::unit());

                        let inner_node = self.build_node(
                            val,
                            NodeInfo {
                                expected_ty: Some((ty, Strength::Strong)),
                                parent_struct_flags,
                            },
                            ctx,
                        );

                        if ctx.hir_arena[inner_node].ty() != ty {
                            // The type of the inner node could be a built-in scalar (e.g. i64)
                            // as a result of being the value of a mathematical expression.
                            // But at the "unpack-level" here we need the type to be some domain-specific alias.
                            // So generate a `(map inner)` to type-pun the result.
                            ctx.mk_node(ontol_hir::Kind::Map(inner_node), Meta { ty, span })
                        } else {
                            inner_node
                        }
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

    /// The written attribute patterns should match against
    /// the relations defined on the type. Compute `MatchAttributes`:
    fn collect_match_attributes(
        &self,
        property_set: &IndexMap<PropertyId, Property>,
        match_attributes: &mut IndexMap<&'m str, MatchAttribute>,
    ) {
        for (property_id, _property) in property_set {
            let meta = self.defs.relationship_meta(property_id.relationship_id);
            let property_name = match property_id.role {
                Role::Subject => match meta.relation_def_kind.value {
                    DefKind::TextLiteral(lit) => Some(*lit),
                    _ => panic!("BUG: Expected named subject property"),
                },
                Role::Object => meta.relationship.object_prop,
            };
            let (_, owner_cardinality, _) = meta.relationship.by(property_id.role);
            let (value_def_id, _, _) = meta.relationship.by(property_id.role.opposite());

            if let Some(property_name) = property_name {
                match_attributes.insert(
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
                );
            }
        }
    }

    fn build_struct_node(
        &mut self,
        pattern_attrs: &[CompoundPatternAttr],
        hir_meta: Meta<'m>,
        mut match_attributes: IndexMap<&'m str, MatchAttribute>,
        actual_struct_flags: StructFlags,
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let struct_binder: TypedHirData<'_, ontol_hir::Binder> =
            TypedHirData(ctx.var_allocator.alloc().into(), hir_meta);

        let mut hir_props = Vec::with_capacity(pattern_attrs.len());

        // Actually match the written attributes to the match attributes:
        for pattern_attr in pattern_attrs {
            let prop_node = self.build_struct_property_node(
                struct_binder.hir().var,
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
                struct_binder.hir().var,
                span,
                match_attributes,
                &mut hir_props,
                ctx,
            );
        }

        ctx.mk_node(
            ontol_hir::Kind::Struct(struct_binder, actual_struct_flags, hir_props.into()),
            hir_meta,
        )
    }

    fn build_struct_property_node(
        &mut self,
        struct_binder_var: Var,
        attr: &CompoundPatternAttr,
        match_attributes: &mut IndexMap<&'m str, MatchAttribute>,
        actual_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Option<ontol_hir::Node> {
        let CompoundPatternAttr {
            key: (def_id, prop_span),
            bind_option,
            kind,
        } = attr;

        let DefKind::TextLiteral(attr_prop) = self.defs.def_kind(*def_id) else {
            self.error(CompileError::NamedPropertyExpected, prop_span);
            return None;
        };
        let Some(match_attribute) = match_attributes.get_mut(attr_prop) else {
            self.error(CompileError::UnknownProperty, prop_span);
            return None;
        };
        if match_attribute.mentioned {
            // TODO: This is probably allowed in match
            self.error(CompileError::DuplicateProperty, prop_span);
            return None;
        }
        match_attribute.mentioned = true;

        let rel_params_ty = match match_attribute.rel_params_def {
            Some(rel_def_id) => self.check_def_sealed(rel_def_id),
            None => &UNIT_TYPE,
        };

        debug!("rel_params_ty: {rel_params_ty:?}");

        match kind {
            CompoundPatternAttrKind::Value { rel, val } => {
                let value_ty = self.check_def_sealed(match_attribute.value_def);
                let rel_node = self.build_rel_node_from_option(
                    rel_params_ty,
                    rel.as_ref(),
                    val,
                    *prop_span,
                    actual_struct_flags,
                    ctx,
                );
                debug!("value_ty: {value_ty:?}");

                let prop_variant = match match_attribute.cardinality.1 {
                    ValueCardinality::One => {
                        let val_node = self.build_node(
                            val,
                            NodeInfo {
                                expected_ty: Some((value_ty, Strength::Strong)),
                                parent_struct_flags: actual_struct_flags,
                            },
                            ctx,
                        );
                        ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                            rel: rel_node,
                            val: val_node,
                        })
                    }
                    ValueCardinality::Many => match &val.kind {
                        PatternKind::Set { elements, .. } => {
                            let mut hir_elements = Vec::with_capacity(elements.len());
                            for element in elements {
                                let val_node = self.build_node(
                                    &element.val,
                                    NodeInfo {
                                        expected_ty: Some((value_ty, Strength::Strong)),
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

                            let label = *ctx.label_map.get(&val.id).unwrap();
                            let seq_ty = self.types.intern(Type::Seq(rel_params_ty, value_ty));

                            ontol_hir::PropVariant::Set(ontol_hir::SetPropertyVariant {
                                label: TypedHirData(
                                    label,
                                    Meta {
                                        ty: seq_ty,
                                        span: NO_SPAN,
                                    },
                                ),
                                has_default: ontol_hir::HasDefault(matches!(
                                    match_attribute.property_id.role,
                                    Role::Object
                                )),
                                elements: hir_elements.into(),
                            })
                        }
                        _ => {
                            self.type_error(
                                TypeError::VariableMustBeSequenceEnclosed(value_ty),
                                &val.span,
                            );
                            return None;
                        }
                    },
                };

                let mut optional = ontol_hir::Optional(matches!(
                    match_attribute.cardinality.0,
                    PropertyCardinality::Optional
                ));

                if self
                    .relations
                    .value_generators
                    .get(&match_attribute.property_id.relationship_id)
                    .is_some()
                {
                    optional = ontol_hir::Optional(true);
                }

                let prop_variants: Vec<ontol_hir::PropVariant<'_, TypedHir>> =
                    match match_attribute.cardinality.0 {
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
                                    &val.span,
                                );
                                vec![]
                            }
                        }
                    };

                Some(ctx.mk_node(
                    ontol_hir::Kind::Prop(
                        optional,
                        struct_binder_var,
                        match_attribute.property_id,
                        prop_variants.into(),
                    ),
                    Meta {
                        ty: &UNIT_TYPE,
                        span: *prop_span,
                    },
                ))
            }
            CompoundPatternAttrKind::ContainsElement { .. } => todo!(),
            CompoundPatternAttrKind::SetOperator { operator, elements } => {
                let value_ty = self.check_def_sealed(match_attribute.value_def);
                let set_node = self.build_hir_set_of(
                    elements,
                    rel_params_ty,
                    value_ty,
                    *prop_span,
                    actual_struct_flags,
                    ctx,
                );

                let prop_variants: Vec<ontol_hir::PropVariant<'m, TypedHir>> =
                    match (match_attribute.cardinality.1, operator) {
                        (ValueCardinality::One, SetBinaryOperator::ElementIn) => {
                            vec![ontol_hir::PropVariant::Predicate(
                                ontol_hir::PredicateClosure::ElementIn(set_node),
                            )]
                        }
                        (ValueCardinality::Many, SetBinaryOperator::ElementIn) => {
                            self.error(
                                CompileError::TODO("property must be a scalar".into()),
                                prop_span,
                            );
                            return None;
                        }
                        (ValueCardinality::Many, _) => {
                            vec![]
                        }
                        (ValueCardinality::One, _) => {
                            self.error(
                                CompileError::TODO("property must be a set".into()),
                                prop_span,
                            );
                            return None;
                        }
                    };

                Some(ctx.mk_node(
                    ontol_hir::Kind::Prop(
                        ontol_hir::Optional(true), // TODO
                        struct_binder_var,
                        match_attribute.property_id,
                        prop_variants.into(),
                    ),
                    Meta {
                        ty: &UNIT_TYPE,
                        span: *prop_span,
                    },
                ))
            }
        }
    }

    fn build_rel_node_from_option(
        &mut self,
        rel_params_ty: TypeRef<'m>,
        rel: Option<&Pattern>,
        val: &Pattern,
        prop_span: SourceSpan,
        actual_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        match (rel_params_ty, rel) {
            (Type::Primitive(PrimitiveKind::Unit, _), Some(rel)) => {
                self.error_node(CompileError::NoRelationParametersExpected, &rel.span, ctx)
            }
            (ty @ Type::Primitive(PrimitiveKind::Unit, _), None) => ctx.mk_node(
                ontol_hir::Kind::Unit,
                Meta {
                    ty,
                    span: prop_span,
                },
            ),
            (_, Some(rel)) => self.build_node(
                rel,
                NodeInfo {
                    expected_ty: Some((rel_params_ty, Strength::Strong)),
                    parent_struct_flags: actual_struct_flags,
                },
                ctx,
            ),
            (ty @ Type::Anonymous(def_id), None) => {
                match self.relations.properties_by_def_id(*def_id) {
                    Some(_) => self.build_implicit_rel_node(ty, val, prop_span, ctx),
                    // An anonymous type without properties, i.e. just "meta relationships" about the relationship itself:
                    None => ctx.mk_node(
                        ontol_hir::Kind::Unit,
                        Meta {
                            ty,
                            span: prop_span,
                        },
                    ),
                }
            }
            (ty, None) => self.build_implicit_rel_node(ty, val, prop_span, ctx),
        }
    }

    fn handle_missing_struct_attributes(
        &mut self,
        struct_binder_var: Var,
        struct_span: SourceSpan,
        match_attributes: IndexMap<&'m str, MatchAttribute>,
        hir_props: &mut Vec<ontol_hir::Node>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (name, match_attr) in match_attributes {
            if match_attr.mentioned {
                continue;
            }
            if matches!(match_attr.property_id.role, Role::Object) {
                continue;
            }
            if matches!(match_attr.cardinality.0, PropertyCardinality::Optional) {
                continue;
            }

            let relationship_id = match_attr.property_id.relationship_id;

            if let Some(const_def_id) = self
                .relations
                .default_const_objects
                .get(&relationship_id)
                .cloned()
            {
                // Generate code for default value.
                let value_ty = self.check_def_sealed(const_def_id);

                let prop_node = {
                    let rel = ctx.mk_unit_node_no_span();
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
                            match_attr.property_id,
                            [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel,
                                val,
                            })]
                            .into(),
                        ),
                        UNIT_META,
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

            {
                let meta = self.defs.relationship_meta(relationship_id);
                let (target_def_id, cardinality, _span) =
                    meta.relationship.by(match_attr.property_id.role.opposite());

                // Here we check that if the missing property refers to variably sized edges
                // to foreign entities, that property does not need to be mentioned for the map
                // to be valid. This is because an entity can always "stand on its own"
                // and be complete without looking at other entities.
                // FIXME: Does this work with unions?
                if let Some(target_properties) = self.relations.properties_by_def_id(target_def_id)
                {
                    if target_properties.identified_by.is_some()
                        && matches!(
                            cardinality,
                            (PropertyCardinality::Optional, _) | (_, ValueCardinality::Many)
                        )
                    {
                        continue;
                    }
                }
            }

            ctx.missing_properties
                .entry(ctx.current_arm)
                .or_default()
                .entry(struct_span)
                .or_default()
                .push(name.into());
        }
    }
}

use indexmap::IndexMap;
use ontol_hir::{PropFlags, PropVariant, StructFlags};
use ontol_parser::source::{NO_SPAN, SourceSpan};
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt, PropId,
    property::{Cardinality, PropertyCardinality, ValueCardinality},
    query::condition::SetOperator,
    var::Var,
};
use smallvec::smallvec;
use tracing::{debug, info};

use crate::{
    CompileError,
    def::DefKind,
    mem::Intern,
    pattern::{
        CompoundPatternAttr, CompoundPatternAttrKind, CompoundPatternModifier, Pattern,
        PatternKind, SetBinaryOperator,
    },
    primitive::PrimitiveKind,
    properties::Property,
    relation::{DefRelTag, RelId, RelParams, rel_def_meta},
    repr::repr_model::{ReprKind, ReprScalarKind},
    thesaurus::TypeRelation,
    type_check::{TypeError, ena_inference::Strength, hir_build::NodeInfo},
    typed_hir::{Meta, TypedHirData},
    types::{Type, TypeRef, UNIT_TYPE},
};

use super::{TypeCheck, hir_build_ctx::HirBuildCtx};

pub(super) struct UnpackerInfo<'m> {
    pub type_def_id: DefId,
    pub ty: TypeRef<'m>,
    pub modifier: Option<CompoundPatternModifier>,
    pub is_unit_binding: bool,
    pub parent_struct_flags: ontol_hir::StructFlags,
}

struct MatchAttribute {
    prop_id: PropId,
    rel_id: RelId,
    cardinality: Cardinality,
    rel_params_def: Option<DefId>,
    value_def: DefId,
    mentioned: bool,
}

#[derive(Eq, PartialEq, Hash)]
pub enum MatchAttributeKey<'m> {
    Named(&'m str),
    Def(DefId),
}

impl<'m> TypeCheck<'_, 'm> {
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
        let property_set = self.prop_ctx.properties_table_by_def_id(type_def_id);

        let actual_struct_flags = match modifier {
            Some(CompoundPatternModifier::Match) => ontol_hir::StructFlags::MATCH,
            None => ontol_hir::StructFlags::empty(),
        } | parent_struct_flags;

        let hir_meta = Meta { ty, span };

        let Some(repr_kind) = self.repr_ctx.get_repr_kind(&type_def_id) else {
            return self.error_node(CompileError::CannotMapAbstract.span(span), ctx);
        };

        match repr_kind {
            ReprKind::Struct | ReprKind::Unit | ReprKind::StructIntersection(_)
                if is_unit_binding =>
            {
                let attr = pattern_attrs
                    .iter()
                    .next()
                    .expect("there must be one pattern for unit binding");
                let CompoundPatternAttrKind::Value { val, .. } = &attr.kind else {
                    panic!("Must pass a value for unit binding");
                };

                let inner_node = self.build_node(
                    val,
                    NodeInfo {
                        // There is a potential mapping involved, send the known
                        // type as weak. If the argument attribute knows its own type,
                        // the weak type should be ignored:
                        expected_ty: Some((hir_meta.ty, Strength::Weak)),
                        parent_struct_flags: StructFlags::default(),
                    },
                    ctx,
                );

                ctx.mk_node(ontol_hir::Kind::Map(inner_node), hir_meta)
            }
            ReprKind::Struct | ReprKind::Unit => match property_set {
                Some(property_set) => {
                    let mut match_attributes = IndexMap::new();
                    self.collect_named_match_attributes(property_set, &mut match_attributes);
                    self.collect_membership_match_attributes(type_def_id, &mut match_attributes);

                    self.build_struct_node(
                        (type_def_id, actual_struct_flags),
                        pattern_attrs,
                        hir_meta,
                        match_attributes,
                        span,
                        ctx,
                    )
                }
                None => {
                    if !pattern_attrs.is_empty() {
                        return self.error_node(CompileError::NoPropertiesExpected.span(span), ctx);
                    }
                    ctx.mk_node(ontol_hir::Kind::Unit, hir_meta)
                }
            },
            ReprKind::StructIntersection(members) => {
                let mut match_attributes = IndexMap::new();

                if let Some(property_set) = property_set {
                    self.collect_named_match_attributes(property_set, &mut match_attributes);
                }

                for (member_def_id, _) in members {
                    if let Some(property_set) =
                        self.prop_ctx.properties_table_by_def_id(*member_def_id)
                    {
                        self.collect_named_match_attributes(property_set, &mut match_attributes);
                    }
                }

                self.collect_membership_match_attributes(type_def_id, &mut match_attributes);

                self.build_struct_node(
                    (type_def_id, actual_struct_flags),
                    pattern_attrs,
                    hir_meta,
                    match_attributes,
                    span,
                    ctx,
                )
            }
            ReprKind::Scalar(scalar_def_id, scalar_kind, _) => self.build_scalar_unpacker(
                UnpackerInfo {
                    type_def_id,
                    ty,
                    modifier,
                    is_unit_binding,
                    parent_struct_flags,
                },
                (*scalar_def_id, scalar_kind.clone()),
                pattern_attrs,
                span,
                ctx,
            ),
            ReprKind::FmtStruct(Some((_, attr_def_id))) => self.build_scalar_unpacker(
                UnpackerInfo {
                    type_def_id,
                    ty,
                    modifier,
                    is_unit_binding,
                    parent_struct_flags,
                },
                (*attr_def_id, ReprScalarKind::Text),
                pattern_attrs,
                span,
                ctx,
            ),
            ReprKind::Union(..) => self.error_node(CompileError::CannotMapUnion.span(span), ctx),
            kind => todo!("{kind:?}"),
        }
    }

    fn build_scalar_unpacker(
        &mut self,
        UnpackerInfo {
            ty,
            is_unit_binding,
            parent_struct_flags,
            ..
        }: UnpackerInfo<'m>,
        (scalar_def_id, scalar_kind): (DefId, ReprScalarKind),
        pattern_attrs: &[CompoundPatternAttr],
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let mut attributes = pattern_attrs.iter();

        let inner_node = match attributes.next() {
            Some(CompoundPatternAttr {
                key: (attr_def_id, _),
                bind_option: _,
                kind: CompoundPatternAttrKind::Value { rel: _, val },
            }) if is_unit_binding => {
                assert!(*attr_def_id == DefId::unit());

                self.build_node(
                    val,
                    NodeInfo {
                        expected_ty: Some((ty, Strength::Strong)),
                        parent_struct_flags,
                    },
                    ctx,
                )
            }
            _ => match (scalar_kind, self.defs.def_kind(scalar_def_id)) {
                (
                    ReprScalarKind::Text | ReprScalarKind::TextConstant(_),
                    DefKind::TextLiteral(lit),
                ) => {
                    // A symbol instantiation:
                    // Make a text constant with the `ty` DefId
                    ctx.mk_node(ontol_hir::Kind::Text((*lit).into()), Meta { ty, span })
                }
                _ => {
                    return self.error_node(CompileError::ExpectedPatternAttribute.span(span), ctx);
                }
            },
        };

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

    /// The written attribute patterns should match against
    /// the relations defined on the type. Compute `MatchAttributes`:
    fn collect_named_match_attributes(
        &self,
        property_set: &IndexMap<PropId, Property>,
        match_attributes: &mut IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
    ) {
        for (prop_id, property) in property_set {
            self.collect_match_attribute(*prop_id, property, match_attributes);
        }
    }

    fn collect_membership_match_attributes(
        &self,
        def_id: DefId,
        match_attributes: &mut IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
    ) {
        let Some(memberships) = self.thesaurus.reverse_table.get(&def_id) else {
            return;
        };

        for mem_def_id in memberships {
            let mesh = self.thesaurus.entries(*mem_def_id, self.defs);
            let Some((is, _)) = mesh.iter().find(|(is, _)| is.def_id == def_id) else {
                continue;
            };

            if matches!(is.rel, TypeRelation::SubVariant) {
                if let Some(property_set) = self.prop_ctx.properties_table_by_def_id(*mem_def_id) {
                    self.collect_named_match_attributes(property_set, match_attributes);
                }
            }
        }
    }

    fn collect_match_attribute(
        &self,
        prop_id: PropId,
        property: &Property,
        match_attributes: &mut IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
    ) {
        let meta = rel_def_meta(property.rel_id, self.rel_ctx, self.defs);
        let match_key = match meta.relation_def_kind.value {
            DefKind::TextLiteral(lit) => Some(MatchAttributeKey::Named(lit)),
            _ => Some(MatchAttributeKey::Def(meta.relationship.relation_def_id)),
        };
        let (_, owner_cardinality, _) = meta.relationship.subject();
        let (value_def_id, _, _) = meta.relationship.object();

        if let Some(match_key) = match_key {
            match_attributes.insert(
                match_key,
                MatchAttribute {
                    prop_id,
                    rel_id: property.rel_id,
                    cardinality: owner_cardinality,
                    rel_params_def: match &meta.relationship.rel_params {
                        RelParams::Def(def_id) => Some(*def_id),
                        _ => None,
                    },
                    value_def: value_def_id,
                    mentioned: false,
                },
            );
        }
    }

    fn build_struct_node(
        &mut self,
        (type_def_id, actual_struct_flags): (DefId, StructFlags),
        pattern_attrs: &[CompoundPatternAttr],
        hir_meta: Meta<'m>,
        mut match_attributes: IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let struct_binder: TypedHirData<'_, ontol_hir::Binder> =
            TypedHirData(ctx.var_allocator.alloc().into(), hir_meta);

        let mut hir_props = Vec::with_capacity(pattern_attrs.len());

        if self.prop_ctx.identified_by(type_def_id).is_some() {
            if let Some(order_union_def_id) = self.entity_ctx.order_union(&type_def_id) {
                match_attributes.insert(
                    MatchAttributeKey::Def(OntolDefTag::RelationOrder.def_id()),
                    MatchAttribute {
                        prop_id: OntolDefTag::RelationOrder.prop_id_0(),
                        rel_id: RelId(DefId::unit(), DefRelTag(0)),
                        cardinality: (PropertyCardinality::Optional, ValueCardinality::IndexSet),
                        rel_params_def: None,
                        value_def: order_union_def_id,
                        mentioned: false,
                    },
                );
            }

            match_attributes.insert(
                MatchAttributeKey::Def(OntolDefTag::RelationDirection.def_id()),
                MatchAttribute {
                    prop_id: OntolDefTag::RelationDirection.prop_id_0(),
                    rel_id: RelId(DefId::unit(), DefRelTag(0)),
                    cardinality: (PropertyCardinality::Optional, ValueCardinality::Unit),
                    rel_params_def: None,
                    value_def: OntolDefTag::UnionDirection.def_id(),
                    mentioned: false,
                },
            );
        }

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
        match_attributes: &mut IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
        actual_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Option<ontol_hir::Node> {
        let (def_id, prop_span) = attr.key;
        let bind_option = attr.bind_option;
        let kind = &attr.kind;

        let mut flags = PropFlags::empty();

        if bind_option.is_some() {
            flags.insert(PropFlags::PAT_OPTIONAL);
        }

        let match_attribute = match self.defs.def_kind(def_id) {
            DefKind::TextLiteral(name) => match_attributes.get_mut(&MatchAttributeKey::Named(name)),
            _ => match_attributes.get_mut(&MatchAttributeKey::Def(def_id)),
        };
        let Some(match_attribute) = match_attribute else {
            CompileError::UnknownProperty.span(prop_span).report(self);
            return None;
        };

        if match_attribute.mentioned {
            // TODO: This is probably allowed in match
            CompileError::DuplicateProperty.span(prop_span).report(self);
            return None;
        }
        match_attribute.mentioned = true;

        let rel_params_ty = match match_attribute.rel_params_def {
            Some(rel_def_id) => self.check_def(rel_def_id),
            None => &UNIT_TYPE,
        };

        debug!(?rel_params_ty, "property node");

        let node = match kind {
            CompoundPatternAttrKind::Value { rel, val } => {
                let value_ty = self.check_def(match_attribute.value_def);
                debug!(?value_ty, "property node");

                let prop_variant = match match_attribute.cardinality.1 {
                    ValueCardinality::Unit => {
                        let rel_node = self.build_rel_node_from_option(
                            rel_params_ty,
                            rel.as_deref(),
                            val,
                            prop_span,
                            actual_struct_flags,
                            ctx,
                        );

                        let val_node = self.build_node(
                            val,
                            NodeInfo {
                                expected_ty: Some((value_ty, Strength::Strong)),
                                parent_struct_flags: actual_struct_flags,
                            },
                            ctx,
                        );
                        if let Some(rel_node) = rel_node {
                            PropVariant::Tuple(smallvec![val_node, rel_node])
                        } else {
                            PropVariant::Unit(val_node)
                        }
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => {
                        match &val.kind {
                            PatternKind::Set { elements, .. } => {
                                let mut hir_matrix_rows = smallvec![];
                                let seq_ty = if rel_params_ty.is_unit() {
                                    let elements = self.type_ctx.intern(vec![value_ty]);
                                    self.type_ctx.intern(Type::Matrix(elements))
                                } else {
                                    let elements =
                                        self.type_ctx.intern(vec![value_ty, rel_params_ty]);
                                    self.type_ctx.intern(Type::Matrix(elements))
                                };

                                for element in elements.iter() {
                                    let val_node = self.build_node(
                                        &element.val,
                                        NodeInfo {
                                            expected_ty: Some((value_ty, Strength::Strong)),
                                            parent_struct_flags: actual_struct_flags,
                                        },
                                        ctx,
                                    );
                                    let rel_node = self.build_rel_node_from_option(
                                        rel_params_ty,
                                        element.rel.as_ref(),
                                        &element.val,
                                        prop_span,
                                        actual_struct_flags,
                                        ctx,
                                    );

                                    let mut elements = smallvec![val_node];
                                    if let Some(rel_node) = rel_node {
                                        elements.push(rel_node);
                                    }

                                    hir_matrix_rows.push(ontol_hir::MatrixRow(
                                        if element.is_iter {
                                            let Some(label) = ctx.label_map.get(&element.id) else {
                                                CompileError::TODO("unable to loop")
                                                    .span(prop_span)
                                                    .report(self);
                                                return None;
                                            };
                                            Some(TypedHirData(*label, Meta::new(seq_ty, prop_span)))
                                        } else {
                                            None
                                        },
                                        elements,
                                    ));
                                }

                                // if todo!("matches!(match_attribute.property_id.role, Role::Object)")
                                // {
                                //     flags.insert(PropFlags::REL_UP_OPTIONAL);
                                // }

                                ontol_hir::PropVariant::Unit(ctx.mk_node(
                                    ontol_hir::Kind::Matrix(hir_matrix_rows),
                                    Meta::new(seq_ty, prop_span),
                                ))
                            }
                            _ => {
                                let rel_node = self.build_rel_node_from_option(
                                    rel_params_ty,
                                    rel.as_deref(),
                                    val,
                                    prop_span,
                                    actual_struct_flags,
                                    ctx,
                                );

                                if actual_struct_flags.contains(StructFlags::MATCH) {
                                    // It's allowed to disregard the cardinality if a Match.
                                    let val_node = self.build_node(
                                        val,
                                        NodeInfo {
                                            expected_ty: Some((value_ty, Strength::Strong)),
                                            parent_struct_flags: actual_struct_flags,
                                        },
                                        ctx,
                                    );
                                    if let Some(rel_node) = rel_node {
                                        PropVariant::Tuple(smallvec![val_node, rel_node])
                                    } else {
                                        PropVariant::Unit(val_node)
                                    }
                                } else {
                                    TypeError::VariableMustBeSequenceEnclosed(value_ty)
                                        .report(val.span, self);
                                    return None;
                                }
                            }
                        }
                    }
                };

                if matches!(match_attribute.cardinality.0, PropertyCardinality::Optional) {
                    flags.insert(PropFlags::REL_DOWN_OPTIONAL | PropFlags::REL_UP_OPTIONAL);
                }

                if self
                    .misc_ctx
                    .value_generators
                    .contains_key(&match_attribute.rel_id)
                {
                    flags.insert(PropFlags::REL_DOWN_OPTIONAL);
                }

                if actual_struct_flags.contains(StructFlags::MATCH) && bind_option.is_some() {
                    // When in a MATCH context, the rel optionality always matches what is written in map
                    flags.insert(PropFlags::REL_DOWN_OPTIONAL | PropFlags::REL_UP_OPTIONAL);
                }

                if matches!(match_attribute.cardinality.0, PropertyCardinality::Optional)
                    && bind_option.is_none()
                {
                    // note: This is probably not necessary
                    ctx.partial = true;
                }

                if flags.rel_up_optional()
                    && !flags.pat_optional()
                    && !matches!(
                        match_attribute.cardinality.1,
                        ValueCardinality::IndexSet | ValueCardinality::List
                    )
                    && !actual_struct_flags.contains(StructFlags::MATCH)
                {
                    self.check_can_construct_default(rel_params_ty, prop_span);
                    self.check_can_construct_default(value_ty, prop_span);
                }

                Some(ctx.mk_node(
                    ontol_hir::Kind::Prop(
                        flags,
                        struct_binder_var,
                        match_attribute.prop_id,
                        prop_variant,
                    ),
                    Meta {
                        ty: &UNIT_TYPE,
                        span: prop_span,
                    },
                ))
            }
            CompoundPatternAttrKind::SetOperator { operator, elements } => {
                let value_ty = self.check_def(match_attribute.value_def);
                let set_node = self.build_hir_set_of(
                    elements,
                    rel_params_ty,
                    value_ty,
                    prop_span,
                    actual_struct_flags,
                    ctx,
                );

                let prop_variant: ontol_hir::PropVariant =
                    match (match_attribute.cardinality.1, operator) {
                        (ValueCardinality::Unit, SetBinaryOperator::ElementIn) => {
                            ontol_hir::PropVariant::Predicate(SetOperator::ElementIn, set_node)
                        }
                        (
                            ValueCardinality::IndexSet | ValueCardinality::List,
                            SetBinaryOperator::ElementIn,
                        ) => {
                            CompileError::TODO("property must be a scalar")
                                .span(prop_span)
                                .report(self);
                            return None;
                        }
                        (ValueCardinality::IndexSet, operator) => {
                            ontol_hir::PropVariant::Predicate(
                                match operator {
                                    SetBinaryOperator::ElementIn => unreachable!(),
                                    SetBinaryOperator::AllIn => SetOperator::SubsetOf,
                                    SetBinaryOperator::ContainsAll => SetOperator::SupersetOf,
                                    SetBinaryOperator::Intersects => SetOperator::SetIntersects,
                                    SetBinaryOperator::SetEquals => SetOperator::SetEquals,
                                },
                                set_node,
                            )
                        }
                        _ => {
                            CompileError::TODO("property must be a set")
                                .span(prop_span)
                                .report(self);
                            return None;
                        }
                    };

                Some(ctx.mk_node(
                    ontol_hir::Kind::Prop(
                        flags | PropFlags::REL_UP_OPTIONAL | PropFlags::REL_DOWN_OPTIONAL, // TODO
                        struct_binder_var,
                        match_attribute.prop_id,
                        prop_variant,
                    ),
                    Meta {
                        ty: &UNIT_TYPE,
                        span: prop_span,
                    },
                ))
            }
        }?;

        if let Some(option_span) = bind_option {
            if let ontol_hir::Kind::Prop(flags, ..) = ctx.hir_arena.node_ref(node).hir() {
                if !flags.contains(PropFlags::REL_UP_OPTIONAL) {
                    CompileError::PropertyNotOptional
                        .span(option_span)
                        .report(self);
                }
            }
        };

        Some(node)
    }

    fn build_rel_node_from_option(
        &mut self,
        rel_params_ty: TypeRef<'m>,
        rel: Option<&Pattern>,
        val: &Pattern,
        prop_span: SourceSpan,
        actual_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Option<ontol_hir::Node> {
        match (rel_params_ty, rel) {
            (Type::Primitive(PrimitiveKind::Unit, _), Some(rel)) => Some(self.error_node(
                CompileError::NoRelationParametersExpected.span(rel.span),
                ctx,
            )),
            (Type::Primitive(PrimitiveKind::Unit, _), None) => None,
            (_, Some(rel)) => Some(self.build_node(
                rel,
                NodeInfo {
                    expected_ty: Some((rel_params_ty, Strength::Strong)),
                    parent_struct_flags: actual_struct_flags,
                },
                ctx,
            )),
            (ty @ (Type::DomainDef(def_id) | Type::Anonymous(def_id)), None) => {
                match self.prop_ctx.properties_by_def_id(*def_id) {
                    Some(_) => {
                        if actual_struct_flags.contains(StructFlags::MATCH) {
                            Some(ctx.mk_node(
                                ontol_hir::Kind::Unit,
                                Meta {
                                    ty,
                                    span: prop_span,
                                },
                            ))
                        } else {
                            // need to produce something
                            Some(self.build_implicit_rel_node(ty, val, prop_span, ctx))
                        }
                    }
                    // An anonymous type without properties, i.e. just "meta relationships" about the relationship itself:
                    None => None,
                }
            }
            (ty, None) => Some(self.build_implicit_rel_node(ty, val, prop_span, ctx)),
        }
    }

    fn handle_missing_struct_attributes(
        &mut self,
        struct_binder_var: Var,
        struct_span: SourceSpan,
        match_attributes: IndexMap<MatchAttributeKey<'m>, MatchAttribute>,
        hir_props: &mut Vec<ontol_hir::Node>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (name, match_attr) in match_attributes {
            if match_attr.mentioned {
                continue;
            }
            if matches!(match_attr.cardinality.0, PropertyCardinality::Optional) {
                continue;
            }

            let relationship_id = match_attr.rel_id;

            if let Some(const_def_id) = self
                .misc_ctx
                .default_const_objects
                .get(&relationship_id)
                .cloned()
            {
                // Generate code for default value.
                let value_ty = self.check_def(const_def_id);

                let prop_node = {
                    let value = ctx.mk_node(
                        ontol_hir::Kind::Const(const_def_id),
                        Meta {
                            ty: value_ty,
                            span: NO_SPAN,
                        },
                    );
                    ctx.mk_node(
                        ontol_hir::Kind::Prop(
                            ontol_hir::PropFlags::empty(),
                            struct_binder_var,
                            match_attr.prop_id,
                            ontol_hir::PropVariant::Unit(value),
                        ),
                        Meta::unit(NO_SPAN),
                    )
                };
                hir_props.push(prop_node);
                continue;
            }

            if self
                .misc_ctx
                .value_generators
                .contains_key(&relationship_id)
            {
                // Value generators should be handled in data storage,
                // so leave these fields out when not mentioned.
                continue;
            }

            {
                let meta = rel_def_meta(relationship_id, self.rel_ctx, self.defs);
                let (target_def_id, cardinality, _span) = meta.relationship.object();

                // Here we check that if the missing property refers to variably sized edges
                // to foreign entities, that property does not need to be mentioned for the map
                // to be valid. This is because an entity can always "stand on its own"
                // and be complete without looking at other entities.
                // FIXME: Does this work with unions?
                if let Some(target_properties) = self.prop_ctx.properties_by_def_id(target_def_id) {
                    if target_properties.identified_by.is_some()
                        && matches!(
                            cardinality,
                            (PropertyCardinality::Optional, _) | (_, ValueCardinality::IndexSet)
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
                .push(name);
        }
    }

    fn check_can_construct_default(&mut self, ty: TypeRef<'m>, span: SourceSpan) {
        let Some(def_id) = ty.get_single_def_id() else {
            CompileError::TODO("Type not found")
                .span(NO_SPAN)
                .report(self);
            return;
        };

        if !self.check_can_construct_default_inner(def_id) {
            debug!(?ty, "cannot make default");
            CompileError::TODO("optional binding required, as a default value cannot be created")
                .span(span)
                .report(self);
        } else {
            info!(?ty, "CAN MAKE DEFAULT");
        }
    }

    fn check_can_construct_default_inner(&self, def_id: DefId) -> bool {
        match self.repr_ctx.get_repr_kind(&def_id) {
            Some(ReprKind::Struct) => self.check_relations_can_construct_default(def_id),
            Some(ReprKind::StructIntersection(members)) => {
                if !self.check_relations_can_construct_default(def_id) {
                    return false;
                }
                for (def_id, _) in members {
                    if !self.check_relations_can_construct_default(*def_id) {
                        return false;
                    }
                }
                true
            }
            Some(ReprKind::Unit) => true,
            Some(ReprKind::Scalar(scalar_def_id, ..)) => matches!(
                self.defs.def_kind(*scalar_def_id),
                DefKind::TextLiteral(_) | DefKind::NumberLiteral(_)
            ),
            Some(ReprKind::Union(members, _)) => members
                .iter()
                .all(|(def_id, _)| self.check_can_construct_default_inner(*def_id)),
            _ => false,
        }
    }

    fn check_relations_can_construct_default(&self, def_id: DefId) -> bool {
        if let Some(property_set) = self.prop_ctx.properties_table_by_def_id(def_id) {
            let mut match_attributes = Default::default();
            self.collect_named_match_attributes(property_set, &mut match_attributes);

            for (_, match_attribute) in match_attributes {
                if let (PropertyCardinality::Mandatory, ValueCardinality::Unit) =
                    match_attribute.cardinality
                {
                    return false;
                }
            }
        }

        true
    }
}

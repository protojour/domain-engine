use ontol_runtime::{property::PropertyCardinality, DefId, EdgeId, RelationshipId};
use tracing::debug;

use crate::{
    def::{BuiltinRelationKind, DefKind, FmtFinalState, Relationship, TypeDef},
    error::CompileError,
    mem::Intern,
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    relation::{Constructor, Properties, Property, TypeParam},
    sequence::Sequence,
    text_patterns::TextPatternSegment,
    thesaurus::TypeRelation,
    types::{FormatType, Type, TypeRef},
    SourceSpan,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_relationship(
        &mut self,
        def_id: DefId,
        relationship: &Relationship,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let relationship_id = RelationshipId(def_id);
        let relation_def_kind = &self.defs.def_kind(relationship.relation_def_id);

        match relation_def_kind {
            DefKind::TextLiteral(_) => {
                self.check_named_relation(relationship_id, None, relationship);
            }
            DefKind::Type(_) => {
                let edge_id = self
                    .edge_ctx
                    .edge_id_by_symbol(relationship.relation_def_id);

                self.check_named_relation(relationship_id, edge_id, relationship);
            }
            DefKind::BuiltinRelType(kind, _) => {
                self.check_builtin_relation(relationship_id, relationship, kind, span);
            }
            DefKind::FmtTransition(def_id, final_state) => {
                self.check_def(*def_id);
                self.check_fmt_relation(relationship_id, relationship, *def_id, final_state, span);
            }
            _ => {
                panic!()
            }
        }

        self.type_ctx.intern(Type::Tautology)
    }

    /// This defines a property on a compound type.
    /// The relation can be:
    /// 1. a text literal, which defines a named property
    /// 2. a domain-defined unit type, which defines a "flattened" property
    fn check_named_relation(
        &mut self,
        rel_id: RelationshipId,
        edge_id: Option<EdgeId>,
        relationship: &Relationship,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        let subject_ty = self.check_def(subject.0);
        let object_ty = self.check_def(object.0);

        if subject.0.package_id() == ONTOL_PKG {
            CompileError::SubjectMustBeDomainType
                .span(subject.1)
                .report(self);
            return object_ty;
        }

        self.check_subject_data_type(subject_ty, &subject.1);
        self.check_object_data_type(object_ty, &object.1);

        self.rel_ctx
            .properties_by_def_id_mut(subject.0)
            .table_mut()
            .insert(
                rel_id,
                Property {
                    cardinality: relationship.subject_cardinality,
                    is_entity_id: false,
                    is_edge_partial: false,
                },
            );

        // Ensure properties in object
        self.rel_ctx.properties_by_def_id_mut(object.0);

        if let Some(edge_id) = edge_id {
            let edge = self.edge_ctx.symbolic_edges.get_mut(&edge_id).unwrap();
            let slot = edge.symbols.get(&relationship.relation_def_id).unwrap();

            edge.variables
                .get_mut(&slot.left)
                .unwrap()
                .members
                .entry(subject.0)
                .or_default()
                .insert(rel_id);
            edge.variables
                .get_mut(&slot.right)
                .unwrap()
                .members
                .entry(object.0)
                .or_default()
                .insert(rel_id);
        }

        object_ty
    }

    fn check_builtin_relation(
        &mut self,
        relationship_id: RelationshipId,
        relationship: &Relationship,
        relation: &BuiltinRelationKind,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        // TODO: Check the object property is undefined

        match relation {
            BuiltinRelationKind::Is => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                // Ensure properties
                self.rel_ctx.properties_by_def_id_mut(subject.0);

                if !self.thesaurus.insert_domain_is(
                    subject.0,
                    match relationship.subject_cardinality.0 {
                        PropertyCardinality::Mandatory => TypeRelation::Super,
                        PropertyCardinality::Optional => TypeRelation::SubVariant,
                    },
                    object.0,
                    *span,
                ) {
                    CompileError::DuplicateAnonymousRelationship
                        .span(*span)
                        .report(self);
                }

                object_ty
            }
            BuiltinRelationKind::Identifies => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.rel_ctx.properties_by_def_id_mut(subject.0);

                if properties.identifies.is_some() {
                    return CompileError::AlreadyIdentifiesAType
                        .span(*span)
                        .report_ty(self);
                }
                if subject.0.package_id() != object.0.package_id() {
                    return CompileError::MustIdentifyWithinDomain
                        .span(*span)
                        .report_ty(self);
                }

                properties.identifies = Some(relationship_id);
                let object_properties = self.rel_ctx.properties_by_def_id_mut(object.0);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relationship.relation_def_id
                        );
                        return CompileError::AlreadyIdentified.span(*span).report_ty(self);
                    }
                    None => object_properties.identified_by = Some(relationship_id),
                }

                object_ty
            }
            BuiltinRelationKind::Id => {
                panic!("This should not have been lowered");
            }
            BuiltinRelationKind::StoreKey => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);

                let Type::TextConstant(def_id) = object_ty else {
                    return CompileError::TypeMismatch {
                        actual: format!(
                            "{}",
                            FormatType::new(object_ty, self.defs, self.primitives)
                        ),
                        expected: "text constant".to_string(),
                    }
                    .span(*span)
                    .report_ty(self);
                };

                let DefKind::TextLiteral(text_literal) = self.defs.def_kind(*def_id) else {
                    panic!("text literal expected to be registered not found!");
                };
                let text_constant = self.str_ctx.intern_constant(text_literal);
                self.edge_ctx.store_keys.insert(subject.0, text_constant);

                object_ty
            }
            BuiltinRelationKind::Indexed => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.rel_ctx.properties_by_def_id_mut(subject.0);
                match (&properties.table, &mut properties.constructor) {
                    (None, Constructor::Transparent) => {
                        let mut sequence = Sequence::default();

                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, relationship_id)
                        {
                            return error.span(*span).report_ty(self);
                        }

                        properties.constructor = Constructor::Sequence(sequence);
                    }
                    (None, Constructor::Sequence(sequence)) => {
                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, relationship_id)
                        {
                            return error.span(*span).report_ty(self);
                        }
                    }
                    _ => {
                        return CompileError::InvalidMixOfRelationshipTypeForSubject
                            .span(*span)
                            .report_ty(self);
                    }
                }

                object_ty
            }
            BuiltinRelationKind::Default => {
                let _subject_ty = self.check_def(subject.0);
                let subject_def_kind = self.defs.def_kind(subject.0);

                let DefKind::Type(TypeDef {
                    rel_type_for: Some(RelationshipId(outer_relationship_id)),
                    ..
                }) = subject_def_kind
                else {
                    return CompileError::TODO(
                        "default not supported here, must be on a relation type",
                    )
                    .span(*span)
                    .report_ty(self);
                };

                let DefKind::Relationship(Relationship {
                    object: outer_object,
                    ..
                }) = self.defs.def_kind(*outer_relationship_id)
                else {
                    unreachable!();
                };

                let Some(object_ty) = self.def_ty_ctx.def_table.get(&outer_object.0).cloned()
                else {
                    return CompileError::TODO(
                        "the type of the default relation has not been checked",
                    )
                    .span(*span)
                    .report_ty(self);
                };

                // just copy the type, type check done later
                self.expected_constant_types.insert(object.0, object_ty);

                let _object_ty = self.check_def(object.0);

                self.rel_ctx
                    .default_const_objects
                    .insert(RelationshipId(*outer_relationship_id), object.0);

                object_ty
            }
            BuiltinRelationKind::Gen => {
                let _subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                let subject_def_kind = self.defs.def_kind(subject.0);

                let Type::ValueGenerator(value_generator_def_id) = object_ty else {
                    return CompileError::TODO("Not a value generator")
                        .span(object.1)
                        .report_ty(self);
                };
                let DefKind::Type(TypeDef {
                    rel_type_for: Some(RelationshipId(outer_relationship_id)),
                    ..
                }) = subject_def_kind
                else {
                    return CompileError::TODO(
                        "gen not supported here, must be on a relation type",
                    )
                    .span(*span)
                    .report_ty(self);
                };
                let DefKind::Relationship(Relationship {
                    object: outer_object,
                    ..
                }) = self.defs.def_kind(*outer_relationship_id)
                else {
                    unreachable!();
                };

                let Some(_) = self.def_ty_ctx.def_table.get(&outer_object.0) else {
                    return CompileError::TODO("the type of the gen relation has not been checked")
                        .span(*span)
                        .report_ty(self);
                };

                self.rel_ctx.value_generators_unchecked.insert(
                    RelationshipId(*outer_relationship_id),
                    (*value_generator_def_id, *span),
                );
                object_ty
            }
            BuiltinRelationKind::Min | BuiltinRelationKind::Max | BuiltinRelationKind::Example => {
                let subject_ty = self.check_def(subject.0);
                let _ = self.check_def(object.0);

                self.rel_ctx
                    .type_params
                    .entry(subject.0)
                    .or_default()
                    .insert(
                        relationship.relation_def_id,
                        TypeParam {
                            definition_site: relationship_id.0.package_id(),
                            object: object.0,
                            span: *span,
                        },
                    );

                subject_ty
            }
            BuiltinRelationKind::Order => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                // This will be checked post-seal in check_entity.rs
                self.rel_ctx
                    .order_relationships
                    .entry(subject.0)
                    .or_default()
                    .push(relationship_id);

                object_ty
            }
            BuiltinRelationKind::Direction => {
                let _subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                if self
                    .rel_ctx
                    .direction_relationships
                    .insert(subject.0, (relationship_id, object.0))
                    .is_some()
                {
                    return CompileError::TODO("duplicate `direction` relationship")
                        .span(*span)
                        .report_ty(self);
                }

                object_ty
            }
        }
    }

    fn check_fmt_relation(
        &mut self,
        relationship_id: RelationshipId,
        relationship: &Relationship,
        relation_def_id: DefId,
        final_state: &FmtFinalState,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        let subject_ty = self.check_def(subject.0);
        let object_ty = self.check_def(object.0);

        match subject_ty {
            Type::TextConstant(subject_def_id) if *subject_def_id == self.primitives.empty_text => {
                if let Err(err) = self.extend_text_pattern_fmt_constructor(
                    relation_def_id,
                    (relationship_id, relationship),
                    object.0,
                    object_ty,
                    TextPatternSegment::EmptyString,
                    *final_state,
                    span,
                ) {
                    return err;
                }
            }
            Type::Anonymous(_) => {
                debug!("Fmt subject anonymous object: {:?}", subject.0);
                let subject_constructor = self
                    .rel_ctx
                    .properties_by_def_id(subject.0)
                    .map(|props| &props.constructor);

                let Some(Constructor::TextFmt(subject_pattern)) = subject_constructor else {
                    return CompileError::ConstructorMismatch
                        .span(*span)
                        .report_ty(self);
                };

                if let Err(err) = self.extend_text_pattern_fmt_constructor(
                    relation_def_id,
                    (relationship_id, relationship),
                    object.0,
                    object_ty,
                    subject_pattern.clone(),
                    *final_state,
                    span,
                ) {
                    return err;
                }
            }
            _ => {
                return CompileError::ConstructorMismatch
                    .span(*span)
                    .report_ty(self);
            }
        }

        subject_ty
    }

    fn check_subject_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        let Some(def_id) = ty.get_single_def_id() else {
            CompileError::SubjectMustBeDomainType
                .span(*span)
                .report_ty(self);
            return;
        };

        match self.defs.def_kind(def_id) {
            DefKind::Primitive(..) | DefKind::Type(_) | DefKind::Extern(_) => {
                self.check_not_sealed(ty, span);
            }
            _ => {
                CompileError::SubjectMustBeDomainType
                    .span(*span)
                    .report(self);
            }
        }
    }

    fn check_object_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Tautology
            | Type::BuiltinRelation
            | Type::Function(_)
            | Type::Package
            | Type::Infer(_)
            | Type::ValueGenerator(_)
            | Type::Error => {
                CompileError::ObjectMustBeDataType.span(*span).report(self);
            }
            _ => {}
        }
    }

    fn check_not_sealed(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        if let Some(def_id) = ty.get_single_def_id() {
            if self.seal_ctx.is_sealed(def_id) {
                CompileError::MutationOfSealedDef.span(*span).report(self);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn extend_text_pattern_fmt_constructor(
        &mut self,
        relation_def_id: DefId,
        relationship: (RelationshipId, &Relationship),
        object_def: DefId,
        object_ty: TypeRef<'m>,
        origin: TextPatternSegment,
        final_state: FmtFinalState,
        span: &SourceSpan,
    ) -> Result<(), TypeRef<'m>> {
        let appendee = match self.defs.def_kind(relation_def_id) {
            DefKind::Primitive(PrimitiveKind::Text, _) => TextPatternSegment::AnyString,
            DefKind::TextLiteral(str) => TextPatternSegment::new_literal(str),
            DefKind::Regex(_) => TextPatternSegment::Regex(
                self.defs
                    .literal_regex_meta_table
                    .get(&relation_def_id)
                    .expect("regex hir not found for literal regex")
                    .hir
                    .clone(),
            ),
            DefKind::Primitive(PrimitiveKind::Serial, _) => TextPatternSegment::Property {
                rel_id: relationship.0,
                type_def_id: relation_def_id,
                segment: Box::new(TextPatternSegment::Serial { radix: 10 }),
            },
            _ => {
                let constructor = self
                    .rel_ctx
                    .properties_by_def_id(relation_def_id)
                    .map(Properties::constructor);

                match constructor {
                    Some(Constructor::TextFmt(rel_segment)) => TextPatternSegment::Property {
                        rel_id: relationship.0,
                        type_def_id: relation_def_id,
                        segment: Box::new(rel_segment.clone()),
                    },
                    _ => {
                        return Err(CompileError::CannotConcatenateStringPattern
                            .span(*span)
                            .report_ty(self))
                    }
                }
            }
        };

        let object_properties = self.rel_ctx.properties_by_def_id_mut(object_def);

        if !matches!(&object_properties.constructor, Constructor::Transparent) {
            return Err(CompileError::ConstructorMismatch
                .span(*span)
                .report_ty(self));
        }

        object_properties.constructor =
            Constructor::TextFmt(TextPatternSegment::concat([origin, appendee]));

        if final_state.0 || !object_ty.is_anonymous() {
            // constructors of unnamable types do not need to be processed..
            // Register pattern processing for later:
            self.rel_ctx.text_pattern_constructors.insert(object_def);
        }

        Ok(())
    }
}

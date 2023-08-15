use indexmap::map::Entry;
use ontol_runtime::{
    ontology::PropertyCardinality, smart_format, value::PropertyId, DefId, RelationshipId,
};
use tracing::debug;

use crate::{
    def::{BuiltinRelationKind, DefKind, FmtFinalState, Relationship, TypeDef},
    error::CompileError,
    mem::Intern,
    patterns::StringPatternSegment,
    primitive::PrimitiveKind,
    relation::{Constructor, Is, Properties, Property, TypeParam, TypeRelation},
    sequence::Sequence,
    types::{Type, TypeRef},
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
            DefKind::StringLiteral(_) => {
                self.check_string_literal_relation(relationship_id, relationship, span);
            }
            DefKind::BuiltinRelType(builtin) => {
                self.check_builtin_relation(relationship_id, relationship, builtin, span);
            }
            DefKind::FmtTransition(def_reference, final_state) => {
                self.check_def_shallow(def_reference.def_id);
                self.check_fmt_relation(
                    relationship_id,
                    relationship,
                    def_reference.def_id,
                    final_state,
                    span,
                );
            }
            _ => {
                panic!()
            }
        }

        self.types.intern(Type::Tautology)
    }

    /// This defines a property on a compound type
    fn check_string_literal_relation(
        &mut self,
        relationship_id: RelationshipId,
        relationship: &Relationship,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        let subject_ty = self.check_def_shallow(subject.0.def_id);
        let object_ty = self.check_def_shallow(object.0.def_id);

        self.check_subject_data_type(subject_ty, &subject.1);
        self.check_object_data_type(object_ty, &object.1);

        let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);
        match &mut properties.table {
            None => {
                properties.table = Some(
                    [(
                        PropertyId::subject(relationship_id),
                        Property {
                            cardinality: relationship.subject_cardinality,
                            is_entity_id: false,
                        },
                    )]
                    .into(),
                );
            }
            Some(map) => {
                map.insert(
                    PropertyId::subject(relationship_id),
                    Property {
                        cardinality: relationship.subject_cardinality,
                        is_entity_id: false,
                    },
                );
            }
        }

        // Ensure properties in object
        self.relations.properties_by_def_id_mut(object.0.def_id);

        match (&relationship.object_prop, object_ty) {
            (Some(_), Type::Domain(_)) => {
                self.check_not_sealed(object_ty, &object.1);

                if self
                    .relations
                    .properties_by_def_id_mut(object.0.def_id)
                    .table_mut()
                    .insert(
                        PropertyId::object(relationship_id),
                        Property {
                            cardinality: relationship.object_cardinality,
                            is_entity_id: false,
                        },
                    )
                    .is_some()
                {
                    return self.error(CompileError::UnionInNamedRelationshipNotSupported, span);
                }
            }
            (Some(_), _) => {
                // non-domain type in object
                return self.error(CompileError::NonEntityInReverseRelationship, span);
            }
            (None, _) => {}
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
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                // Ensure properties
                self.relations.properties_by_def_id_mut(subject.0.def_id);

                let prev_entry = self
                    .relations
                    .ontology_mesh
                    .entry(subject.0.def_id)
                    .or_default()
                    .entry(Is {
                        def_id: object.0.def_id,
                        rel: match relationship.subject_cardinality.0 {
                            PropertyCardinality::Mandatory => TypeRelation::Super,
                            PropertyCardinality::Optional => TypeRelation::SubVariant,
                        },
                    });

                match prev_entry {
                    Entry::Vacant(vacant) => {
                        vacant.insert(*span);
                    }
                    Entry::Occupied(_) => {
                        self.error(CompileError::DuplicateAnonymousRelationship, span);
                    }
                }

                object_ty
            }
            BuiltinRelationKind::Identifies => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);

                if properties.identifies.is_some() {
                    return self.error(CompileError::AlreadyIdentifiesAType, span);
                }
                if subject.0.def_id.package_id() != object.0.def_id.package_id() {
                    return self.error(CompileError::MustIdentifyWithinDomain, span);
                }

                properties.identifies = Some(relationship_id);
                let object_properties = self.relations.properties_by_def_id_mut(object.0.def_id);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relationship.relation_def_id
                        );
                        return self.error(CompileError::AlreadyIdentified, span);
                    }
                    None => object_properties.identified_by = Some(relationship_id),
                }

                object_ty
            }
            BuiltinRelationKind::Id => {
                panic!("This should not have been lowered");
            }
            BuiltinRelationKind::Indexed => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);
                match (&properties.table, &mut properties.constructor) {
                    (None, Constructor::Transparent) => {
                        let mut sequence = Sequence::default();

                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, relationship_id)
                        {
                            return self.error(error, span);
                        }

                        properties.constructor = Constructor::Sequence(sequence);
                    }
                    (None, Constructor::Sequence(sequence)) => {
                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, relationship_id)
                        {
                            return self.error(error, span);
                        }
                    }
                    _ => {
                        return self
                            .error(CompileError::InvalidMixOfRelationshipTypeForSubject, span)
                    }
                }

                object_ty
            }
            BuiltinRelationKind::Route => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_package_data_type(subject_ty, &subject.1);
                self.check_package_data_type(object_ty, &object.1);

                object_ty
            }
            BuiltinRelationKind::Default => {
                let _subject_ty = self.check_def_shallow(subject.0.def_id);
                let subject_def_kind = self.defs.def_kind(subject.0.def_id);

                match subject_def_kind {
                    DefKind::Type(TypeDef {
                        rel_type_for: Some(RelationshipId(outer_relationship_id)),
                        ..
                    }) => match self.defs.def_kind(*outer_relationship_id) {
                        DefKind::Relationship(Relationship {
                            object: outer_object,
                            ..
                        }) => match self.def_types.table.get(&outer_object.0.def_id).cloned() {
                            Some(object_ty) => {
                                // just copy the type, type check done later
                                self.expected_constant_types
                                    .insert(object.0.def_id, object_ty);

                                let _object_ty = self.check_def_shallow(object.0.def_id);

                                self.relations.default_const_objects.insert(
                                    RelationshipId(*outer_relationship_id),
                                    object.0.def_id,
                                );

                                object_ty
                            }
                            None => self.error(
                                CompileError::TODO(smart_format!(
                                    "the type of the default relation has not been checked"
                                )),
                                span,
                            ),
                        },
                        _ => unreachable!(),
                    },
                    _ => self.error(
                        CompileError::TODO(smart_format!(
                            "default not supported here, must be on a relation type"
                        )),
                        span,
                    ),
                }
            }
            BuiltinRelationKind::Gen => {
                let _subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                let subject_def_kind = self.defs.def_kind(subject.0.def_id);

                let value_generator_def_id = match object_ty {
                    Type::ValueGenerator(def_id) => *def_id,
                    _ => {
                        return self.error(
                            CompileError::TODO(smart_format!("Not a value generator")),
                            &object.1,
                        )
                    }
                };

                match subject_def_kind {
                    DefKind::Type(TypeDef {
                        rel_type_for: Some(RelationshipId(outer_relationship_id)),
                        ..
                    }) => match self.defs.def_kind(*outer_relationship_id) {
                        DefKind::Relationship(Relationship {
                            object: outer_object,
                            ..
                        }) => match self.def_types.table.get(&outer_object.0.def_id).cloned() {
                            Some(_outer_object_ty) => {
                                self.relations.value_generators_unchecked.insert(
                                    RelationshipId(*outer_relationship_id),
                                    (value_generator_def_id, *span),
                                );
                                object_ty
                            }
                            None => self.error(
                                CompileError::TODO(smart_format!(
                                    "the type of the gen relation has not been checked"
                                )),
                                span,
                            ),
                        },
                        _ => unreachable!(),
                    },
                    _ => self.error(
                        CompileError::TODO(smart_format!(
                            "gen not supported here, must be on a relation type"
                        )),
                        span,
                    ),
                }
            }
            BuiltinRelationKind::Min
            | BuiltinRelationKind::Max
            | BuiltinRelationKind::Doc
            | BuiltinRelationKind::Example => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let _ = self.check_def_shallow(object.0.def_id);

                self.relations
                    .type_params
                    .entry(subject.0.def_id)
                    .or_default()
                    .insert(
                        relationship.relation_def_id,
                        TypeParam {
                            object: object.0.def_id,
                            span: *span,
                        },
                    );

                subject_ty
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

        let subject_ty = self.check_def_shallow(subject.0.def_id);
        let object_ty = self.check_def_shallow(object.0.def_id);

        match subject_ty {
            Type::StringConstant(subject_def_id)
                if *subject_def_id == self.primitives.empty_string =>
            {
                if let Err(e) = self.extend_string_pattern_fmt_constructor(
                    relation_def_id,
                    (relationship_id, relationship),
                    object.0.def_id,
                    object_ty,
                    StringPatternSegment::EmptyString,
                    *final_state,
                    span,
                ) {
                    return e;
                }
            }
            Type::Anonymous(_) => {
                debug!("Fmt subject anonymous object: {:?}", subject.0.def_id);
                let subject_constructor = self
                    .relations
                    .properties_by_def_id(subject.0.def_id)
                    .map(|props| &props.constructor);

                match subject_constructor {
                    Some(Constructor::StringFmt(subject_pattern)) => {
                        if let Err(e) = self.extend_string_pattern_fmt_constructor(
                            relation_def_id,
                            (relationship_id, relationship),
                            object.0.def_id,
                            object_ty,
                            subject_pattern.clone(),
                            *final_state,
                            span,
                        ) {
                            return e;
                        }
                    }
                    _ => {
                        return self.error(CompileError::ConstructorMismatch, span);
                    }
                }
            }
            _ => {
                return self.error(CompileError::ConstructorMismatch, span);
            }
        }

        subject_ty
    }

    fn check_subject_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        let def_id = match ty.get_single_def_id() {
            Some(def_id) => def_id,
            None => {
                self.error(CompileError::SubjectMustBeDomainType, span);
                return;
            }
        };

        match self.defs.def_kind(def_id) {
            DefKind::Primitive(_) | DefKind::Type(_) => {
                self.check_not_sealed(ty, span);
            }
            _ => {
                self.error(CompileError::SubjectMustBeDomainType, span);
            }
        }
    }

    fn check_object_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Tautology
            | Type::BuiltinRelation
            | Type::Function { .. }
            | Type::Package
            | Type::Infer(_)
            | Type::ValueGenerator(_)
            | Type::Error => {
                self.error(CompileError::ObjectMustBeDataType, span);
            }
            _ => {}
        }
    }

    fn check_not_sealed(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        if let Some(def_id) = ty.get_single_def_id() {
            if self.seal_ctx.is_sealed(def_id) {
                self.error(CompileError::MutationOfSealedType, span);
            }
        }
    }

    fn check_package_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Package => {}
            _ => {
                self.error(CompileError::SubjectMustBeDomainType, span);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn extend_string_pattern_fmt_constructor(
        &mut self,
        relation_def_id: DefId,
        relationship: (RelationshipId, &Relationship),
        object_def: DefId,
        object_ty: TypeRef<'m>,
        origin: StringPatternSegment,
        final_state: FmtFinalState,
        span: &SourceSpan,
    ) -> Result<(), TypeRef<'m>> {
        let appendee = match self.defs.def_kind(relation_def_id) {
            DefKind::Primitive(PrimitiveKind::String) => StringPatternSegment::AllStrings,
            DefKind::StringLiteral(str) => StringPatternSegment::new_literal(str),
            DefKind::Regex(_) => StringPatternSegment::Regex(
                self.defs
                    .literal_regex_hirs
                    .get(&relation_def_id)
                    .expect("regex hir not found for literal regex")
                    .clone(),
            ),
            _ => {
                match self
                    .relations
                    .properties_by_def_id(relation_def_id)
                    .map(Properties::constructor)
                {
                    Some(Constructor::StringFmt(rel_segment)) => StringPatternSegment::Property {
                        property_id: PropertyId::subject(relationship.0),
                        type_def_id: relation_def_id,
                        segment: Box::new(rel_segment.clone()),
                    },
                    _ => {
                        return Err(self.error(CompileError::CannotConcatenateStringPattern, span));
                    }
                }
            }
        };

        let object_properties = self.relations.properties_by_def_id_mut(object_def);

        match &mut object_properties.constructor {
            Constructor::Transparent => {
                object_properties.constructor =
                    Constructor::StringFmt(StringPatternSegment::concat([origin, appendee]));

                if final_state.0 || !object_ty.is_anonymous() {
                    // constructors of unnamable types do not need to be processed..
                    // Register pattern processing for later:
                    self.relations
                        .string_pattern_constructors
                        .insert(object_def);
                }
            }
            _ => return Err(self.error(CompileError::ConstructorMismatch, span)),
        }

        Ok(())
    }
}

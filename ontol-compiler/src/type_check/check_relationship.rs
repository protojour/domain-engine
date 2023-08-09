use indexmap::map::Entry;
use ontol_runtime::{
    ontology::PropertyCardinality, smart_format, value::PropertyId, DefId, RelationshipId,
};
use tracing::debug;

use crate::{
    def::{
        BuiltinRelationKind, Def, DefKind, DefReference, FmtFinalState, Relation, RelationId,
        RelationKind, Relationship, TypeDef,
    },
    error::CompileError,
    mem::Intern,
    patterns::StringPatternSegment,
    primitive::PrimitiveKind,
    relation::{Constructor, Is, Properties, Property},
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
        let relation = match self.defs.table.get(&relationship.relation_id.0) {
            Some(Def {
                kind: DefKind::Relation(relation),
                ..
            }) => relation,
            other => panic!("TODO: relation not found, got {other:?}"),
        };

        match &relation.kind {
            RelationKind::Named(def) | RelationKind::FmtTransition(def, _) => {
                self.check_def_shallow(def.def_id);
            }
            _ => {}
        };

        self.check_subject_property(
            (RelationshipId(def_id), relationship),
            (relationship.relation_id, relation),
            &relationship.subject,
            &relationship.object,
            span,
        );
        self.check_object_property(
            (RelationshipId(def_id), relationship),
            (relationship.relation_id, relation),
            &relationship.object,
            &relationship.subject,
            span,
        );

        self.types.intern(Type::Tautology)
    }

    fn check_subject_property(
        &mut self,
        relationship: (RelationshipId, &Relationship),
        relation: (RelationId, &Relation),
        subject: &(DefReference, SourceSpan),
        object: &(DefReference, SourceSpan),
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        match &relation.1.kind {
            RelationKind::Builtin(BuiltinRelationKind::Is) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                let prev_entry = self
                    .relations
                    .ontology_mesh
                    .entry(subject.0.def_id)
                    .or_default()
                    .entry(Is {
                        def_id: object.0.def_id,
                        cardinality: relationship.1.subject_cardinality.0,
                        is_ontol_alias: false,
                    });

                match prev_entry {
                    Entry::Vacant(vacant) => {
                        vacant.insert(*span);
                    }
                    Entry::Occupied(_) => {
                        self.error(CompileError::DuplicateAnonymousRelationship, span);
                    }
                }

                let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);

                match (
                    relationship.1.subject_cardinality.0,
                    &mut properties.constructor,
                ) {
                    (PropertyCardinality::Mandatory, Constructor::Transparent) => {
                        properties.constructor = Constructor::Value(
                            relationship.0,
                            *span,
                            relationship.1.subject_cardinality,
                        );
                    }
                    (PropertyCardinality::Mandatory, _) => {}
                    (PropertyCardinality::Optional, Constructor::Transparent) => {}
                    _ => return self.error(CompileError::ConstructorMismatch, span),
                }

                object_ty
            }
            RelationKind::Builtin(BuiltinRelationKind::Identifies) => {
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

                properties.identifies = Some(relationship.0);
                let object_properties = self.relations.properties_by_def_id_mut(object.0.def_id);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relation.0
                        );
                        return self.error(CompileError::AlreadyIdentified, span);
                    }
                    None => object_properties.identified_by = Some(relationship.0),
                }

                object_ty
            }
            RelationKind::Builtin(BuiltinRelationKind::Id) => {
                panic!("This should not have been lowered");
            }
            RelationKind::Builtin(BuiltinRelationKind::Indexed) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);
                match (&properties.table, &mut properties.constructor) {
                    (None, Constructor::Transparent) => {
                        let mut sequence = Sequence::default();

                        if let Err(error) =
                            sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                        {
                            return self.error(error, span);
                        }

                        properties.constructor = Constructor::Sequence(sequence);
                    }
                    (None, Constructor::Sequence(sequence)) => {
                        if let Err(error) =
                            sequence.define_relationship(&relationship.1.rel_params, relationship.0)
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
            RelationKind::Named(_) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                let properties = self.relations.properties_by_def_id_mut(subject.0.def_id);
                match &mut properties.table {
                    None => {
                        properties.table = Some(
                            [(
                                PropertyId::subject(relationship.0),
                                Property {
                                    cardinality: relationship.1.subject_cardinality,
                                    is_entity_id: false,
                                },
                            )]
                            .into(),
                        );
                    }
                    Some(map) => {
                        map.insert(
                            PropertyId::subject(relationship.0),
                            Property {
                                cardinality: relationship.1.subject_cardinality,
                                is_entity_id: false,
                            },
                        );
                    }
                }

                object_ty
            }
            RelationKind::FmtTransition(_, _) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let _ = self.check_def_shallow(object.0.def_id);

                subject_ty
            }
            RelationKind::Builtin(BuiltinRelationKind::Route) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                self.check_package_data_type(subject_ty, &subject.1);
                self.check_package_data_type(object_ty, &object.1);

                object_ty
            }
            RelationKind::Builtin(BuiltinRelationKind::Default) => {
                let _subject_ty = self.check_def_shallow(subject.0.def_id);
                let subject_def_kind = self.defs.get_def_kind(subject.0.def_id).unwrap();

                match subject_def_kind {
                    DefKind::Type(TypeDef {
                        rel_type_for: Some(RelationshipId(outer_relationship_id)),
                        ..
                    }) => match self.defs.get_def_kind(*outer_relationship_id) {
                        Some(DefKind::Relationship(Relationship {
                            object: outer_object,
                            ..
                        })) => match self.def_types.table.get(&outer_object.0.def_id).cloned() {
                            Some(object_ty) => {
                                // just copy the type, type check done later
                                self.expected_constant_types
                                    .insert(object.0.def_id, object_ty);

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
            RelationKind::Builtin(BuiltinRelationKind::Gen) => {
                let _subject_ty = self.check_def_shallow(subject.0.def_id);
                let object_ty = self.check_def_shallow(object.0.def_id);

                let subject_def_kind = self.defs.get_def_kind(subject.0.def_id).unwrap();

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
                    }) => match self.defs.get_def_kind(*outer_relationship_id) {
                        Some(DefKind::Relationship(Relationship {
                            object: outer_object,
                            ..
                        })) => match self.def_types.table.get(&outer_object.0.def_id).cloned() {
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
            RelationKind::Builtin(
                BuiltinRelationKind::Min
                | BuiltinRelationKind::Max
                | BuiltinRelationKind::Doc
                | BuiltinRelationKind::Example,
            ) => {
                let subject_ty = self.check_def_shallow(subject.0.def_id);
                let _ = self.check_def_shallow(object.0.def_id);

                subject_ty
            }
        }
    }

    /// Check object property, the inverse of a subject property
    fn check_object_property(
        &mut self,
        relationship: (RelationshipId, &Relationship),
        relation: (RelationId, &Relation),
        object: &(DefReference, SourceSpan),
        subject: &(DefReference, SourceSpan),
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let object_ty = self.check_def_shallow(object.0.def_id);
        let subject_ty = self.check_def_shallow(subject.0.def_id);

        if let RelationKind::FmtTransition(_, final_state) = &relation.1.kind {
            match subject_ty {
                Type::StringConstant(subject_def_id)
                    if *subject_def_id == self.primitives.empty_string =>
                {
                    if let Err(e) = self.extend_string_pattern_fmt_constructor(
                        relation,
                        relationship,
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
                    debug!("Fmt subject anonymous object: {:?}", object.0.def_id);
                    let subject_constructor = self
                        .relations
                        .properties_by_def_id(subject.0.def_id)
                        .map(|props| &props.constructor);

                    match subject_constructor {
                        Some(Constructor::StringFmt(subject_pattern)) => {
                            if let Err(e) = self.extend_string_pattern_fmt_constructor(
                                relation,
                                relationship,
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
        } else {
            let object_properties = self.relations.properties_by_def_id_mut(object.0.def_id);

            match (
                &relationship.1.object_prop,
                object_ty,
                &mut object_properties.table,
            ) {
                (Some(_), Type::Domain(_), None) => {
                    object_properties.table = Some(
                        [(
                            PropertyId::object(relationship.0),
                            Property {
                                cardinality: relationship.1.object_cardinality,
                                is_entity_id: false,
                            },
                        )]
                        .into(),
                    );
                }
                (Some(_), Type::Domain(_), Some(table)) => {
                    if table
                        .insert(
                            PropertyId::object(relationship.0),
                            Property {
                                cardinality: relationship.1.object_cardinality,
                                is_entity_id: false,
                            },
                        )
                        .is_some()
                    {
                        return self
                            .error(CompileError::UnionInNamedRelationshipNotSupported, span);
                    }
                }
                (Some(_), _, _) => {
                    // non-domain type in object
                    return self.error(CompileError::NonEntityInReverseRelationship, span);
                }
                (None, _, _) => {}
            }
        }

        subject_ty
    }

    fn check_subject_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Domain(_) | Type::Anonymous(_) => {}
            _ => {
                self.error(CompileError::SubjectMustBeDomainType, span);
            }
        }
        self.check_not_sealed(ty, span);
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
            if self.sealed_defs.sealed_set.contains(&def_id) {
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
        relation: (RelationId, &Relation),
        relationship: (RelationshipId, &Relationship),
        object_def: DefId,
        object_ty: TypeRef<'m>,
        origin: StringPatternSegment,
        final_state: FmtFinalState,
        span: &SourceSpan,
    ) -> Result<(), TypeRef<'m>> {
        let rel_def = match &relation.1.kind {
            RelationKind::FmtTransition(def, _) | RelationKind::Named(def) => def,
            _ => todo!(),
        };

        let appendee = match self.defs.get_def_kind(rel_def.def_id) {
            Some(DefKind::Primitive(PrimitiveKind::String)) => StringPatternSegment::AllStrings,
            Some(DefKind::StringLiteral(str)) => StringPatternSegment::new_literal(str),
            Some(DefKind::Regex(_)) => StringPatternSegment::Regex(
                self.defs
                    .literal_regex_hirs
                    .get(&rel_def.def_id)
                    .expect("regex hir not found for literal regex")
                    .clone(),
            ),
            _ => {
                match self
                    .relations
                    .properties_by_def_id(rel_def.def_id)
                    .map(Properties::constructor)
                {
                    Some(Constructor::StringFmt(rel_segment)) => StringPatternSegment::Property {
                        property_id: PropertyId::subject(relationship.0),
                        type_def_id: rel_def.def_id,
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

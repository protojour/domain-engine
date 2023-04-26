use ontol_runtime::{value::PropertyId, DefId, RelationId};
use tracing::debug;

use crate::{
    def::{
        BuiltinRelationKind, Def, DefKind, DefReference, PropertyCardinality, Relation,
        RelationKind, Relationship,
    },
    error::CompileError,
    mem::Intern,
    patterns::StringPatternSegment,
    primitive::PrimitiveKind,
    relation::{Constructor, Properties, Property, RelationshipId},
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
        let relation = match self.defs.map.get(&relationship.relation_id.0) {
            Some(Def {
                kind: DefKind::Relation(relation),
                ..
            }) => relation,
            other => panic!("TODO: relation not found, got {other:?}"),
        };

        match &relation.kind {
            RelationKind::Named(def) | RelationKind::FmtTransition(def) => {
                self.check_def(def.def_id);
            }
            _ => {}
        };

        self.relations.relationships_by_subject.insert(
            (relationship.subject.0.def_id, relationship.relation_id),
            RelationshipId(def_id),
        );
        self.relations.relationships_by_object.insert(
            (relationship.object.0.def_id, relationship.relation_id),
            RelationshipId(def_id),
        );

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
        let subject_ty = self.check_def(subject.0.def_id);
        let object_ty = self.check_def(object.0.def_id);

        match &relation.1.kind {
            RelationKind::Builtin(BuiltinRelationKind::Is) => {
                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_type_mut(subject.0.def_id);

                match (
                    relationship.1.subject_cardinality.0,
                    &mut properties.constructor,
                ) {
                    (PropertyCardinality::Mandatory, Constructor::Struct) => {
                        properties.constructor = Constructor::Value(
                            relationship.0,
                            *span,
                            relationship.1.subject_cardinality,
                        );
                    }
                    (
                        PropertyCardinality::Mandatory,
                        Constructor::Value(
                            existing_relationship_id,
                            existing_span,
                            existing_cardinality,
                        ),
                    ) => {
                        properties.constructor = Constructor::Intersection(
                            [
                                (
                                    *existing_relationship_id,
                                    *existing_span,
                                    *existing_cardinality,
                                ),
                                (relationship.0, *span, relationship.1.subject_cardinality),
                            ]
                            .into(),
                        );
                    }
                    (PropertyCardinality::Mandatory, Constructor::Intersection(intersection)) => {
                        intersection.push((
                            relationship.0,
                            *span,
                            relationship.1.subject_cardinality,
                        ));
                    }
                    (PropertyCardinality::Optional, Constructor::Struct) => {
                        properties.constructor =
                            Constructor::Union([(relationship.0, *span)].into());
                        // Register union for check later
                        self.relations.value_unions.insert(subject.0.def_id);
                    }
                    (PropertyCardinality::Optional, Constructor::Union(variants)) => {
                        variants.push((relationship.0, *span));
                    }
                    _ => return self.error(CompileError::ConstructorMismatch, span),
                }
            }
            RelationKind::Builtin(BuiltinRelationKind::Identifies) => {
                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_type_mut(subject.0.def_id);

                if properties.identifies.is_some() {
                    return self.error(CompileError::AlreadyIdentifiesAType, span);
                }
                if subject.0.def_id.package_id() != object.0.def_id.package_id() {
                    return self.error(CompileError::MustIdentifyWithinDomain, span);
                }

                properties.identifies = Some(RelationId(self.primitives.identifies_relation));
                let object_properties = self.relations.properties_by_type_mut(object.0.def_id);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relation.0
                        );
                        return self.error(CompileError::AlreadyIdentified, span);
                    }
                    None => {
                        object_properties.identified_by =
                            Some(RelationId(self.primitives.identifies_relation))
                    }
                }
            }
            RelationKind::Builtin(BuiltinRelationKind::Indexed) => {
                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.relations.properties_by_type_mut(subject.0.def_id);
                match (&properties.map, &mut properties.constructor) {
                    (None, Constructor::Struct) => {
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
            }
            RelationKind::Named(_) => {
                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                let properties = self.relations.properties_by_type_mut(subject.0.def_id);
                match &mut properties.map {
                    None => {
                        properties.map = Some(
                            [(
                                PropertyId::subject(relation.0),
                                Property {
                                    cardinality: relationship.1.subject_cardinality,
                                    is_entity_id: false,
                                },
                            )]
                            .into(),
                        );
                    }
                    Some(map) => {
                        let prev = map.insert(
                            PropertyId::subject(relation.0),
                            Property {
                                cardinality: relationship.1.subject_cardinality,
                                is_entity_id: false,
                            },
                        );
                        if prev.is_some() {
                            return self
                                .error(CompileError::UnionInNamedRelationshipNotSupported, span);
                        }
                    }
                }
            }
            RelationKind::FmtTransition(_) => return subject_ty,
            RelationKind::Builtin(BuiltinRelationKind::Route) => {
                self.check_package_data_type(subject_ty, &subject.1);
                self.check_package_data_type(object_ty, &object.1);
            }
            RelationKind::Builtin(BuiltinRelationKind::Doc | BuiltinRelationKind::Example) => {
                return subject_ty
            }
        };

        object_ty
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
        let object_ty = self.check_def(object.0.def_id);
        let subject_ty = self.check_def(subject.0.def_id);

        if matches!(&relation.1.kind, RelationKind::FmtTransition(_)) {
            match subject_ty {
                Type::StringConstant(subject_def_id)
                    if *subject_def_id == self.primitives.empty_string =>
                {
                    if let Err(e) = self.extend_string_pattern_fmt_constructor(
                        relation,
                        object.0.def_id,
                        object_ty,
                        StringPatternSegment::EmptyString,
                        span,
                    ) {
                        return e;
                    }
                }
                Type::Anonymous(_) => {
                    let subject_constructor = self
                        .relations
                        .properties_by_type(subject.0.def_id)
                        .map(|props| &props.constructor);

                    match subject_constructor {
                        Some(Constructor::StringFmt(subject_pattern)) => {
                            if let Err(e) = self.extend_string_pattern_fmt_constructor(
                                relation,
                                object.0.def_id,
                                object_ty,
                                subject_pattern.clone(),
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
            let object_properties = self.relations.properties_by_type_mut(object.0.def_id);

            match (
                &relation.1.object_prop,
                object_ty,
                &mut object_properties.map,
            ) {
                (Some(_), Type::Domain(_), None) => {
                    object_properties.map = Some(
                        [(
                            PropertyId::object(relation.0),
                            Property {
                                cardinality: relationship.1.object_cardinality,
                                is_entity_id: false,
                            },
                        )]
                        .into(),
                    );
                }
                (Some(_), Type::Domain(_), Some(map)) => {
                    if map
                        .insert(
                            PropertyId::object(relation.0),
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
    }

    fn check_object_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Tautology
            | Type::BuiltinRelation
            | Type::Function { .. }
            | Type::Package
            | Type::Infer(_)
            | Type::Error => {
                self.error(CompileError::ObjectMustBeDataType, span);
            }
            _ => {}
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

    fn extend_string_pattern_fmt_constructor(
        &mut self,
        relation: (RelationId, &Relation),
        object_def: DefId,
        object_ty: TypeRef<'m>,
        origin: StringPatternSegment,
        span: &SourceSpan,
    ) -> Result<(), TypeRef<'m>> {
        let rel_def = match &relation.1.kind {
            RelationKind::FmtTransition(def) | RelationKind::Named(def) => def,
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
                    .properties_by_type(rel_def.def_id)
                    .map(Properties::constructor)
                {
                    Some(Constructor::StringFmt(rel_segment)) => StringPatternSegment::Property {
                        property_id: PropertyId::subject(relation.0),
                        type_def_id: rel_def.def_id,
                        segment: Box::new(rel_segment.clone()),
                    },
                    _ => {
                        return Err(self.error(CompileError::CannotConcatenateStringPattern, span));
                    }
                }
            }
        };

        let object_properties = self.relations.properties_by_type_mut(object_def);

        match &mut object_properties.constructor {
            Constructor::Struct => {
                object_properties.constructor =
                    Constructor::StringFmt(StringPatternSegment::concat([origin, appendee]));

                if !object_ty.is_anonymous() {
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

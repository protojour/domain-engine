use ontol_runtime::{value::PropertyId, DefId, RelationId};
use tracing::debug;

use crate::{
    def::{
        Def, DefKind, DefReference, Primitive, PropertyCardinality, Relation, RelationKind,
        Relationship,
    },
    error::CompileError,
    mem::Intern,
    patterns::StringPatternSegment,
    relation::{Constructor, Properties, RelationshipId},
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
            RelationKind::Named(def) | RelationKind::Transition(def) => {
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

        if matches!(&relation.1.kind, RelationKind::Transition(_)) {
            return subject_ty;
        }

        match subject_ty {
            Type::EmptySequence(_) => return object_ty,
            Type::StringConstant(def_id) if *def_id == self.defs.empty_string() => {
                return object_ty;
            }
            Type::Unit(_) | Type::StringConstant(_) => {
                return self.error(CompileError::SubjectMustBeDomainType, &subject.1)
            }
            _ => {}
        }

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(subject.0.def_id);

        match (
            &relation.1.kind,
            &mut properties.map,
            &mut properties.constructor,
        ) {
            (RelationKind::Is, _, constructor) => {
                match (relationship.1.subject_cardinality.0, constructor) {
                    (PropertyCardinality::Mandatory, Constructor::Identity) => {
                        properties.constructor = Constructor::Value(
                            relationship.0,
                            *span,
                            relationship.1.subject_cardinality,
                        );
                    }
                    (PropertyCardinality::Optional, Constructor::Identity) => {
                        properties.constructor =
                            Constructor::ValueUnion([(relationship.0, *span)].into());
                        // Register union for check later
                        self.relations.value_unions.insert(subject.0.def_id);
                    }
                    (PropertyCardinality::Optional, Constructor::ValueUnion(variants)) => {
                        variants.push((relationship.0, *span));
                    }
                    _ => return self.error(CompileError::ConstructorMismatch, span),
                }
            }
            (RelationKind::Identifies, _, _) => {
                if properties.identifies.is_some() {
                    return self.error(CompileError::AlreadyIdentifiesAType, span);
                }
                if subject.0.def_id.package_id() != object.0.def_id.package_id() {
                    return self.error(CompileError::MustIdentifyWithinDomain, span);
                }

                properties.identifies = Some(RelationId(self.defs.identifies_relation()));
                let object_properties = self.relations.properties_by_type_mut(object.0.def_id);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relation.0
                        );
                        todo!("Object is already identified, report error")
                    }
                    None => {
                        object_properties.identified_by =
                            Some(RelationId(self.defs.identifies_relation()))
                    }
                }
            }
            (RelationKind::Indexed, None, Constructor::Identity) => {
                let mut sequence = Sequence::default();

                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }

                properties.constructor = Constructor::Sequence(sequence);
            }
            (RelationKind::Indexed, None, Constructor::Sequence(sequence)) => {
                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }
            }
            (RelationKind::Named(_), None, Constructor::Identity) => {
                properties.map = Some(
                    [(
                        PropertyId::subject(relation.0),
                        relationship.1.subject_cardinality,
                    )]
                    .into(),
                );
            }
            (RelationKind::Named(_), Some(map), Constructor::Identity) => {
                if map
                    .insert(
                        PropertyId::subject(relation.0),
                        relationship.1.subject_cardinality,
                    )
                    .is_some()
                {
                    return self.error(CompileError::UnionInNamedRelationshipNotSupported, span);
                }
            }
            (RelationKind::Transition(_), None, Constructor::StringPattern(_)) => {
                debug!("should concatenate string pattern");
            }
            _ => return self.error(CompileError::InvalidMixOfRelationshipTypeForSubject, span),
        }

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

        if matches!(&relation.1.kind, RelationKind::Transition(_)) {
            match subject_ty {
                Type::StringConstant(subject_def_id)
                    if *subject_def_id == self.defs.empty_string() =>
                {
                    if let Err(e) = self.extend_string_pattern_constructor(
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
                        Some(Constructor::StringPattern(subject_pattern)) => {
                            if let Err(e) = self.extend_string_pattern_constructor(
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
                            relationship.1.object_cardinality,
                        )]
                        .into(),
                    );
                }
                (Some(_), Type::Domain(_), Some(map)) => {
                    if map
                        .insert(
                            PropertyId::object(relation.0),
                            relationship.1.object_cardinality,
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
                (None, _, _) => {
                    debug!(
                        "ignored object property with constructor {:?}",
                        object_properties.constructor
                    );
                }
            }
        }

        subject_ty
    }

    fn extend_string_pattern_constructor(
        &mut self,
        relation: (RelationId, &Relation),
        object_def: DefId,
        object_ty: TypeRef<'m>,
        origin: StringPatternSegment,
        span: &SourceSpan,
    ) -> Result<(), TypeRef<'m>> {
        let rel_def = match &relation.1.kind {
            RelationKind::Transition(def) | RelationKind::Named(def) => def,
            _ => todo!(),
        };

        let appendee = match self.defs.get_def_kind(rel_def.def_id) {
            Some(DefKind::Primitive(Primitive::String)) => StringPatternSegment::AllStrings,
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
                    Some(Constructor::StringPattern(rel_segment)) => {
                        StringPatternSegment::Property {
                            property_id: PropertyId::subject(relation.0),
                            type_def_id: rel_def.def_id,
                            segment: Box::new(rel_segment.clone()),
                        }
                    }
                    _ => {
                        return Err(self.error(CompileError::CannotConcatenateStringPattern, span));
                    }
                }
            }
        };

        let object_properties = self.relations.properties_by_type_mut(object_def);

        match &mut object_properties.constructor {
            Constructor::Identity => {
                object_properties.constructor =
                    Constructor::StringPattern(StringPatternSegment::concat([origin, appendee]));

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

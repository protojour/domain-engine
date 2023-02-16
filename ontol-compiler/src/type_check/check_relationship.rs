use ontol_runtime::{string_types::StringLikeType, value::PropertyId, DefId, RelationId};
use tracing::debug;

use crate::{
    def::{
        Cardinality, Def, DefKind, Primitive, PropertyCardinality, Relation, RelationIdent,
        Relationship, ValueCardinality,
    },
    error::CompileError,
    mem::Intern,
    patterns::StringPatternSegment,
    relation::{Constructor, MapProperties, Properties, RelationshipId},
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

        match relation.ident {
            RelationIdent::Named(def_id) | RelationIdent::Typed(def_id) => {
                self.check_def(def_id);
            }
            _ => {}
        };

        self.relations.relationships_by_subject.insert(
            (relationship.subject.0, relationship.relation_id),
            RelationshipId(def_id),
        );
        self.relations.relationships_by_object.insert(
            (relationship.object.0, relationship.relation_id),
            RelationshipId(def_id),
        );

        self.check_subject_property(
            (RelationshipId(def_id), relationship),
            (relationship.relation_id, relation),
            relationship.subject,
            relationship.object,
            span,
        );
        self.check_object_property(
            (RelationshipId(def_id), relationship),
            (relationship.relation_id, relation),
            relationship.object,
            relationship.subject,
            span,
        );

        self.types.intern(Type::Tautology)
    }

    fn check_subject_property(
        &mut self,
        relationship: (RelationshipId, &Relationship),
        relation: (RelationId, &Relation),
        subject: (DefId, SourceSpan),
        object: (DefId, SourceSpan),
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let subject_ty = self.check_def(subject.0);
        let object_ty = self.check_def(object.0);
        let adapter = CardinalityAdapter::new(subject_ty, object_ty);

        match subject_ty {
            Type::Unit(_) | Type::EmptySequence(_) => return object_ty,
            Type::StringConstant(def_id) if *def_id == self.defs.empty_string() => {
                return object_ty;
            }
            Type::StringConstant(_) => {
                return self.error(CompileError::InvalidSubjectType, &subject.1)
            }
            _ => {}
        }

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(subject.0);

        match (
            &relation.1.ident,
            &mut properties.map,
            &mut properties.constructor,
        ) {
            (RelationIdent::Indexed, MapProperties::Empty, Constructor::Identity) => {
                let mut sequence = Sequence::default();

                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }

                properties.constructor = Constructor::Sequence(sequence);
            }
            (RelationIdent::Indexed, MapProperties::Empty, Constructor::Sequence(sequence)) => {
                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }
            }
            (RelationIdent::Named(_), MapProperties::Empty, Constructor::Identity) => {
                properties.map = MapProperties::Map(
                    [(
                        PropertyId::subject(relation.0),
                        adapter.adapt(relationship.1.subject_cardinality),
                    )]
                    .into(),
                );
            }
            (RelationIdent::Named(_), MapProperties::Map(map), Constructor::Identity) => {
                if map
                    .insert(
                        PropertyId::subject(relation.0),
                        adapter.adapt(relationship.1.subject_cardinality),
                    )
                    .is_some()
                {
                    return self.error(CompileError::UnionInNamedRelationshipNotSupported, span);
                }
            }
            (
                RelationIdent::Named(_) | RelationIdent::Typed(_),
                MapProperties::Empty,
                Constructor::StringPattern(_),
            ) => {
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
        object: (DefId, SourceSpan),
        subject: (DefId, SourceSpan),
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let object_ty = self.check_def(object.0);
        let subject_ty = self.check_def(subject.0);
        let adapter = CardinalityAdapter::new(object_ty, subject_ty);

        match subject_ty {
            Type::Unit(_) => {
                let object_properties = self.relations.properties_by_type_mut(object.0);

                match &mut object_properties.constructor {
                    Constructor::Identity => {
                        object_properties.constructor = Constructor::Value(
                            relationship.0,
                            *span,
                            relationship.1.subject_cardinality,
                        );
                    }
                    Constructor::Value(existing_relationship_id, existing_span, cardinality) => {
                        match (relationship.1.subject_cardinality, cardinality) {
                            (
                                (PropertyCardinality::Mandatory, ValueCardinality::One),
                                (PropertyCardinality::Mandatory, ValueCardinality::One),
                            ) => {
                                object_properties.constructor = Constructor::ValueUnion(
                                    [
                                        (*existing_relationship_id, *existing_span),
                                        (relationship.0, *span),
                                    ]
                                    .into(),
                                );

                                // Register union for check later
                                self.relations.value_unions.insert(object.0);
                            }
                            _ => {
                                return self.error(
                                    CompileError::InvalidCardinaltyCombinationInUnion,
                                    span,
                                );
                            }
                        }
                    }
                    Constructor::ValueUnion(properties) => {
                        properties.push((relationship.0, *span));
                    }
                    _ => return self.error(CompileError::ConstructorMismatch, span),
                }
            }
            Type::StringConstant(subject_def_id) if *subject_def_id == self.defs.empty_string() => {
                if let Err(e) = self.extend_string_pattern_constructor(
                    relation,
                    object.0,
                    object_ty,
                    StringPatternSegment::Empty,
                    span,
                ) {
                    return e;
                }
            }
            Type::Anonymous(_) => {
                let subject_constructor = self
                    .relations
                    .properties_by_type(subject.0)
                    .map(|props| &props.constructor);

                match subject_constructor {
                    Some(Constructor::StringPattern(subject_pattern)) => {
                        if let Err(e) = self.extend_string_pattern_constructor(
                            relation,
                            object.0,
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
                let object_properties = self.relations.properties_by_type_mut(object.0);

                match (
                    &relation.1.object_prop,
                    object_ty,
                    &mut object_properties.map,
                ) {
                    (Some(_), Type::DomainEntity(_), MapProperties::Empty) => {
                        object_properties.map = MapProperties::Map(
                            [(
                                PropertyId::object(relation.0),
                                adapter.adapt(relationship.1.object_cardinality),
                            )]
                            .into(),
                        );
                    }
                    (Some(_), Type::DomainEntity(_), MapProperties::Map(map)) => {
                        if map
                            .insert(
                                PropertyId::object(relation.0),
                                adapter.adapt(relationship.1.object_cardinality),
                            )
                            .is_some()
                        {
                            return self
                                .error(CompileError::UnionInNamedRelationshipNotSupported, span);
                        }
                    }
                    (Some(_), _, _) => {
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
        let rel_def_id = match relation.1.ident {
            RelationIdent::Typed(def_id) | RelationIdent::Named(def_id) => def_id,
            _ => todo!(),
        };

        let appendee = match self.defs.get_def_kind(rel_def_id) {
            Some(DefKind::StringLiteral(str)) => StringPatternSegment::literal(str),
            Some(DefKind::Regex(_)) => StringPatternSegment::Regex(
                self.defs
                    .literal_regex_hirs
                    .get(&rel_def_id)
                    .expect("regex hir not found for literal regex")
                    .clone(),
            ),
            def_kind => {
                match self
                    .relations
                    .properties_by_type(rel_def_id)
                    .map(Properties::constructor)
                {
                    Some(Constructor::StringPattern(rel_segment)) => {
                        StringPatternSegment::Property {
                            property_id: PropertyId::subject(relation.0),
                            type_def_id: rel_def_id,
                            // FIXME: Should be a common place to compute this:
                            string_like_type: match def_kind {
                                Some(DefKind::Primitive(Primitive::Uuid)) => {
                                    Some(StringLikeType::Uuid)
                                }
                                _ => None,
                            },
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
                    self.relations.string_patterns.insert(object_def);
                }
            }
            _ => return Err(self.error(CompileError::ConstructorMismatch, span)),
        }

        Ok(())
    }
}

struct CardinalityAdapter<'m> {
    domain_ty: TypeRef<'m>,
    codomain_ty: TypeRef<'m>,
}

impl<'m> CardinalityAdapter<'m> {
    fn new(domain_ty: TypeRef<'m>, codomain_ty: TypeRef<'m>) -> Self {
        Self {
            domain_ty,
            codomain_ty,
        }
    }

    /// Relationships between _entities_ behave differently than relationships between values.
    ///
    /// Entity relationships are usually optional.
    fn adapt(&self, cardinality: Cardinality) -> Cardinality {
        match (self.domain_ty, self.codomain_ty, cardinality.1) {
            (Type::DomainEntity(_), Type::DomainEntity(_), ValueCardinality::Many) => {
                (PropertyCardinality::Optional, cardinality.1)
            }
            _ => cardinality,
        }
    }
}

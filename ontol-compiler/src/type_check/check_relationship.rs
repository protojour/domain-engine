use ontol_runtime::{value::PropertyId, DefId, RelationId};

use crate::{
    def::{
        Cardinality, Def, DefKind, PropertyCardinality, Relation, RelationIdent, Relationship,
        ValueCardinality,
    },
    error::CompileError,
    mem::Intern,
    relation::{Constructor, MapProperties, RelationshipId},
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

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(object.0);

        match subject_ty {
            Type::Unit(_) => match &mut properties.constructor {
                Constructor::Identity => {
                    properties.constructor = Constructor::Value(
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
                            properties.constructor = Constructor::ValueUnion(
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
                            return self
                                .error(CompileError::InvalidCardinaltyCombinationInUnion, span);
                        }
                    }
                }
                Constructor::ValueUnion(properties) => {
                    properties.push((relationship.0, *span));
                }
                _ => return self.error(CompileError::ConstructorMismatch, span),
            },
            _ => match (&relation.1.object_prop, object_ty, &mut properties.map) {
                (Some(_), Type::DomainEntity(_), MapProperties::Empty) => {
                    properties.map = MapProperties::Map(
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
                (None, _, _) => {}
            },
        }

        subject_ty
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

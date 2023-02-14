use ontol_runtime::{DefId, RelationId};

use crate::{
    def::{
        Cardinality, Def, DefKind, PropertyCardinality, Relation, RelationIdent, Relationship,
        ValueCardinality,
    },
    error::CompileError,
    mem::Intern,
    relation::{ObjectProperties, RelationshipId, SubjectProperties},
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

        self.relations.relationships_by_subject.insert(
            (relationship.subject, relationship.relation_id),
            RelationshipId(def_id),
        );
        self.relations.relationships_by_object.insert(
            (relationship.object, relationship.relation_id),
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
        subject_def: DefId,
        object_def: DefId,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let domain_ty = self.check_def(subject_def);
        let codomain_ty = self.check_def(object_def);
        let adapter = CardinalityAdapter::new(domain_ty, codomain_ty);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(subject_def);

        match (&relation.1.ident, &mut properties.subject) {
            (RelationIdent::Anonymous, SubjectProperties::Empty) => {
                properties.subject = SubjectProperties::Value(
                    relationship.0,
                    *span,
                    relationship.1.subject_cardinality,
                );
            }
            (
                RelationIdent::Anonymous,
                SubjectProperties::Value(existing_relationship_id, existing_span, cardinality),
            ) => {
                match (relationship.1.subject_cardinality, cardinality) {
                    (
                        (PropertyCardinality::Mandatory, ValueCardinality::One),
                        (PropertyCardinality::Mandatory, ValueCardinality::One),
                    ) => {
                        properties.subject = SubjectProperties::ValueUnion(
                            [
                                (*existing_relationship_id, *existing_span),
                                (relationship.0, *span),
                            ]
                            .into(),
                        );

                        // Register union for check later
                        self.relations.value_unions.insert(subject_def);
                    }
                    _ => {
                        return self.error(CompileError::InvalidCardinaltyCombinationInUnion, span);
                    }
                }
            }
            (RelationIdent::Anonymous, SubjectProperties::ValueUnion(properties)) => {
                properties.push((relationship.0, *span));
            }
            (RelationIdent::Indexed, SubjectProperties::Empty) => {
                let mut sequence = Sequence::default();

                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }

                properties.subject = SubjectProperties::Sequence(sequence);
            }
            (RelationIdent::Indexed, SubjectProperties::Sequence(sequence)) => {
                if let Err(error) =
                    sequence.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }
            }
            (RelationIdent::Named(_), SubjectProperties::Empty) => {
                properties.subject = SubjectProperties::Map(
                    [(
                        relation.0,
                        adapter.adapt(relationship.1.subject_cardinality),
                    )]
                    .into(),
                );
            }
            (RelationIdent::Named(_), SubjectProperties::Map(properties)) => {
                if properties
                    .insert(
                        relation.0,
                        adapter.adapt(relationship.1.subject_cardinality),
                    )
                    .is_some()
                {
                    return self.error(CompileError::UnionInNamedRelationshipNotSupported, span);
                }
            }
            _ => return self.error(CompileError::InvalidMixOfRelationshipTypeForSubject, span),
        }

        codomain_ty
    }

    /// Check object property, the inverse of a subject property
    fn check_object_property(
        &mut self,
        relationship: (RelationshipId, &Relationship),
        relation: (RelationId, &Relation),
        object_def: DefId,
        subject_def: DefId,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let domain_ty = self.check_def(object_def);
        let codomain_ty = self.check_def(subject_def);
        let adapter = CardinalityAdapter::new(domain_ty, codomain_ty);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(object_def);

        match (&relation.1.object_prop, domain_ty, &mut properties.object) {
            (Some(_), Type::DomainEntity(_), ObjectProperties::Empty) => {
                properties.object = ObjectProperties::Map(
                    [(relation.0, adapter.adapt(relationship.1.object_cardinality))].into(),
                );
            }
            (Some(_), Type::DomainEntity(_), ObjectProperties::Map(properties)) => {
                if properties
                    .insert(relation.0, adapter.adapt(relationship.1.object_cardinality))
                    .is_some()
                {
                    return self.error(CompileError::UnionInNamedRelationshipNotSupported, span);
                }
            }
            (Some(_), _, _) => {
                return self.error(CompileError::NonEntityInReverseRelationship, span);
            }
            (None, _, _) => {}
        };

        codomain_ty
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

use ontol_runtime::{DefId, RelationId};

use crate::{
    codegen::{CodegenTask, EqCodegenTask},
    def::{
        Cardinality, Def, DefKind, Primitive, PropertyCardinality, Relation, RelationIdent,
        Relationship, ValueCardinality,
    },
    error::CompileError,
    mem::Intern,
    relation::{ObjectProperties, RelationshipId, SubjectProperties},
    sequence::Sequence,
    typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{check_expr::CheckExprContext, inference::Inference, TypeCheck};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_def(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_types.map.get(&def_id) {
            return type_ref;
        }

        let def = self
            .defs
            .map
            .get(&def_id)
            .expect("BUG: definition not found");

        match &def.kind {
            DefKind::DomainType(_) => {
                let ty = self.types.intern(Type::Domain(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::DomainEntity(_) => {
                let ty = self.types.intern(Type::DomainEntity(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::StringLiteral(_) => {
                let ty = self.types.intern(Type::StringConstant(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Tuple(element_defs) => {
                let element_types = element_defs
                    .iter()
                    .map(|e| self.check_def(*e))
                    .collect::<Vec<_>>();
                let element_types = self.types.intern(element_types);
                let ty = self.types.intern(Type::Tuple(element_types));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Relationship(relationship) => {
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
                    &def.span,
                );
                self.check_object_property(
                    (RelationshipId(def_id), relationship),
                    (relationship.relation_id, relation),
                    relationship.object,
                    relationship.subject,
                    &def.span,
                );

                self.types.intern(Type::Tautology)
            }
            DefKind::Primitive(Primitive::Int) => self.types.intern(Type::Int(def_id)),
            DefKind::Primitive(Primitive::Number) => self.types.intern(Type::Number(def_id)),
            DefKind::Equation(variables, first_id, second_id) => {
                let mut ctx = CheckExprContext {
                    inference: Inference::new(),
                    typed_expr_table: TypedExprTable::default(),
                    bound_variables: Default::default(),
                };

                for (index, (variable_expr_id, variable_span)) in variables.0.iter().enumerate() {
                    let var_ref = ctx.typed_expr_table.add_expr(TypedExpr {
                        ty: self.types.intern(Type::Tautology),
                        kind: TypedExprKind::Variable(SyntaxVar(index as u32)),
                        span: *variable_span,
                    });
                    ctx.bound_variables.insert(*variable_expr_id, var_ref);
                }

                let (_, node_a) = self.check_expr_id(*first_id, &mut ctx);
                let (_, node_b) = self.check_expr_id(*second_id, &mut ctx);

                self.codegen_tasks.push(CodegenTask::Eq(EqCodegenTask {
                    typed_expr_table: ctx.typed_expr_table.seal(),
                    node_a,
                    node_b,
                    span: def.span,
                }));

                self.types.intern(Type::Tautology)
            }
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
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
                let mut tuple = Sequence::default();

                if let Err(error) =
                    tuple.define_relationship(&relationship.1.rel_params, relationship.0)
                {
                    return self.error(error, span);
                }

                properties.subject = SubjectProperties::Sequence(tuple);
            }
            (RelationIdent::Indexed, SubjectProperties::Sequence(tuple)) => {
                if let Err(error) =
                    tuple.define_relationship(&relationship.1.rel_params, relationship.0)
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

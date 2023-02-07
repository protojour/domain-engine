use ontol_runtime::{DefId, RelationId};

use crate::{
    codegen::{CodegenTask, EqCodegenTask},
    def::{Cardinality, Def, DefKind, Primitive, Relation, Relationship},
    error::CompileError,
    mem::Intern,
    relation::{RelationshipId, Role, SubjectProperties},
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

                self.check_property(
                    (RelationshipId(def_id), relationship),
                    (relationship.relation_id, relation),
                    Role::Subject,
                    (relationship.subject, relationship.object),
                    &def.span,
                );
                self.check_property(
                    (RelationshipId(def_id), relationship),
                    (relationship.relation_id, relation),
                    Role::Object,
                    (relationship.object, relationship.subject),
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

    fn check_property(
        &mut self,
        relationship: (RelationshipId, &Relationship),
        relation: (RelationId, &Relation),
        primary_role: Role,
        role_defs: (DefId, DefId),
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let property_codomain_ty = self.check_def(role_defs.1);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(role_defs.0);
        match primary_role {
            Role::Subject => match (&relation.1.ident, &mut properties.subject) {
                (None, SubjectProperties::Empty) => {
                    properties.subject =
                        SubjectProperties::Value(relationship.0, *span, relationship.1.cardinality);
                }
                (
                    None,
                    SubjectProperties::Value(existing_relationship_id, existing_span, cardinality),
                ) => {
                    match (relationship.1.cardinality, cardinality) {
                        (Cardinality::One, Cardinality::One) => {
                            properties.subject = SubjectProperties::ValueUnion(
                                [
                                    (*existing_relationship_id, *existing_span),
                                    (relationship.0, *span),
                                ]
                                .into(),
                            );

                            // Register union for check later
                            self.relations.value_unions.insert(role_defs.0);
                        }
                        _ => {
                            return self
                                .error(CompileError::InvalidCardinaltyCombinationInUnion, span);
                        }
                    }
                }
                (None, SubjectProperties::ValueUnion(properties)) => {
                    properties.push((relationship.0, *span));
                }
                (Some(_), SubjectProperties::Empty) => {
                    properties.subject =
                        SubjectProperties::Map([(relation.0, relationship.1.cardinality)].into());
                }
                (Some(_), SubjectProperties::Map(properties)) => {
                    if properties
                        .insert(relation.0, relationship.1.cardinality)
                        .is_some()
                    {
                        return self
                            .error(CompileError::UnionInNamedRelationshipNotSupported, span);
                    }
                }
                (None, SubjectProperties::Map(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Value(_, _, _) | SubjectProperties::ValueUnion(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
            },
            Role::Object => {
                properties.object.insert(relation.0);
            }
        }

        property_codomain_ty
    }
}

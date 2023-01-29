use ontol_runtime::DefId;

use crate::{
    codegen::{CodegenTask, EqArm, EqCodegenTask},
    def::{Def, DefKind, Primitive, Relation},
    error::CompileError,
    mem::Intern,
    relation::{Role, SubjectProperties},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{inference::Inference, TypeCheck};

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
            DefKind::Type(_) => {
                let ty = self.types.intern(Type::Domain(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Relationship(relationship) => {
                let relation = match self.defs.map.get(&relationship.relation_def_id) {
                    Some(Def {
                        kind: DefKind::Relation(relation),
                        ..
                    }) => relation,
                    other => panic!("TODO: relation not found, got {other:?}"),
                };

                self.check_property(
                    def_id,
                    relation,
                    relationship.subject,
                    Role::Subject,
                    relationship.object,
                    &def.span,
                );
                self.check_property(
                    def_id,
                    relation,
                    relationship.object,
                    Role::Object,
                    relationship.subject,
                    &def.span,
                );

                self.types.intern(Type::Tautology)
            }
            DefKind::Primitive(Primitive::Number) => self.types.intern(Type::Number),
            DefKind::Equivalence(first_id, second_id) => {
                let mut inf = Inference::new();
                let first = self.check_expr_id(*first_id, &mut inf);
                let second = self.check_expr_id(*second_id, &mut inf);

                self.codegen_tasks.push(CodegenTask::Eq(EqCodegenTask {
                    arm1: EqArm {
                        expr_id: *first_id,
                        ty: first,
                    },
                    arm2: EqArm {
                        expr_id: *second_id,
                        ty: second,
                    },
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
        relationship_id: DefId,
        relation: &Relation,
        role_def_id: DefId,
        role: Role,
        inverse_role_def_id: DefId,
        span: &SourceSpan,
    ) -> TypeRef<'m> {
        let property_codomain = self.check_def(inverse_role_def_id);
        let property_id = self.relations.new_property(relationship_id, role);

        // Type of the property value/the property "range" / "co-domain":
        let properties = self.relations.properties_by_type_mut(role_def_id);
        match role {
            Role::Subject => match (&relation.ident, &mut properties.subject) {
                (None, SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Value(property_id);
                }
                (None, SubjectProperties::Value(_)) => {
                    return self.error(CompileError::DuplicateAnonymousRelation, span);
                }
                (None, SubjectProperties::Map(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Unit) => {
                    properties.subject = SubjectProperties::Map([property_id].into());
                }
                (Some(_), SubjectProperties::Value(_)) => {
                    return self.error(CompileError::CannotMixNamedAndAnonymousRelations, span);
                }
                (Some(_), SubjectProperties::Map(properties)) => {
                    properties.insert(property_id);
                }
            },
            Role::Object => {
                properties.object.insert(property_id);
            }
        }

        property_codomain
    }
}

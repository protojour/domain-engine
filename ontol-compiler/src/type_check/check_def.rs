use ontol_runtime::DefId;

use crate::{
    codegen::{CodegenTask, EqCodegenTask},
    def::{Def, DefKind, Primitive, Relation},
    error::CompileError,
    mem::Intern,
    relation::{Role, SubjectProperties},
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
            DefKind::Equivalence(variables, first_id, second_id) => {
                let mut ctx = CheckExprContext {
                    inference: Inference::new(),
                    typed_expr_table: TypedExprTable::default(),
                    variable_nodes: Default::default(),
                };

                for (index, variable_expr_id) in variables.0.iter().enumerate() {
                    let node_id = ctx.typed_expr_table.add_expr(TypedExpr {
                        ty: self.types.intern(Type::Tautology),
                        kind: TypedExprKind::Variable(SyntaxVar(index as u32)),
                    });
                    ctx.variable_nodes.insert(*variable_expr_id, node_id);
                }

                let (_, node_a) = self.check_expr_id(*first_id, &mut ctx);
                let (_, node_b) = self.check_expr_id(*second_id, &mut ctx);

                self.codegen_tasks.push(CodegenTask::Eq(EqCodegenTask {
                    typed_expr_table: ctx.typed_expr_table,
                    node_a,
                    node_b,
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

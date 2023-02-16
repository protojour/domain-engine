use ontol_runtime::DefId;

use crate::{
    codegen::{CodegenTask, EqCodegenTask},
    def::{DefKind, Primitive},
    mem::Intern,
    typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
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
            DefKind::DomainType(Some(_ident)) => {
                let ty = self.types.intern(Type::Domain(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::DomainType(None) => {
                let ty = self.types.intern(Type::Anonymous(def_id));
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
            DefKind::Regex(_) => {
                let ty = self.types.intern(Type::Regex(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Relationship(relationship) => {
                self.check_relationship(def_id, relationship, &def.span)
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
}

use ontol_runtime::DefId;

use crate::{
    codegen::{CodegenTask, MapCodegenTask},
    def::{DefKind, TypeDef},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_expr::{TypedExpr, TypedExprKind},
    types::{Type, TypeRef},
};

use super::{check_expr::CheckExprContext, TypeCheck};

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
            DefKind::Type(TypeDef {
                ident: Some(_ident),
                ..
            }) => {
                let ty = self.types.intern(Type::Domain(def_id));
                self.def_types.map.insert(def_id, ty);
                ty
            }
            DefKind::Type(TypeDef { ident: None, .. }) => {
                let ty = self.types.intern(Type::Anonymous(def_id));
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
            DefKind::Primitive(PrimitiveKind::Int) => self.types.intern(Type::Int(def_id)),
            DefKind::Primitive(PrimitiveKind::Number) => self.types.intern(Type::Number(def_id)),
            DefKind::Mapping(variables, first_id, second_id) => {
                let mut ctx = CheckExprContext::new();

                for (index, (variable_expr_id, variable_span)) in variables.0.iter().enumerate() {
                    let var_ref = ctx.expressions.add(TypedExpr {
                        ty: self.types.intern(Type::Tautology),
                        kind: TypedExprKind::Variable(ctx.syntax_var(index as u16)),
                        span: *variable_span,
                    });
                    ctx.bound_variables.insert(*variable_expr_id, var_ref);
                }

                let (_, node_a) = self.check_expr_id(*first_id, &mut ctx);
                let (_, node_b) = self.check_expr_id(*second_id, &mut ctx);

                self.codegen_tasks.push(CodegenTask::Map(MapCodegenTask {
                    expressions: ctx.expressions,
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

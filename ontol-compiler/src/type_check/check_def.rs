use ontol_runtime::{smart_format, DefId};
use tracing::debug;

use crate::{
    codegen::{CodegenTask, ConstCodegenTask},
    def::{DefKind, TypeDef},
    error::CompileError,
    mem::Intern,
    primitive::PrimitiveKind,
    type_check::check_expr::CheckExprContext,
    types::{Type, TypeRef},
};

use super::TypeCheck;

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
                match self.check_map(def, variables, *first_id, *second_id) {
                    Ok(ty) => ty,
                    Err(error) => {
                        debug!("Aggregation group error: {error:?}");
                        self.types.intern(Type::Error)
                    }
                }
            }
            DefKind::Constant(expr_id) => {
                let mut expr_root = self.consume_expr(*expr_id);
                expr_root.expected_ty = match self.expected_constant_types.remove(&def_id) {
                    None => {
                        self.error(
                            CompileError::TODO(smart_format!("No expected type for constant")),
                            &def.span,
                        );
                        return self.types.intern(Type::Error);
                    }
                    Some(ty) => Some(ty),
                };

                let mut ctx = CheckExprContext::new();
                let (ty, hir_idx) = self.check_expr_root(expr_root, &mut ctx);

                self.codegen_tasks
                    .push(CodegenTask::Const(ConstCodegenTask {
                        def_id,
                        nodes: ctx.nodes,
                        root: hir_idx,
                        span: def.span,
                    }));

                ty
            }
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }
}

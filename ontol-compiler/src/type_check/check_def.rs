use ontol_runtime::DefId;
use tracing::debug;

use crate::{
    codegen::task::{CodegenTask, ConstCodegenTask},
    def::{DefKind, TypeDef},
    mem::Intern,
    primitive::PrimitiveKind,
    type_check::hir_build_ctx::HirBuildCtx,
    types::{Type, TypeRef},
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_def(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_types.map.get(&def_id) {
            return type_ref;
        }
        let ty = self.check_def_inner(def_id);
        self.def_types.map.insert(def_id, ty);
        ty
    }

    fn check_def_inner(&mut self, def_id: DefId) -> TypeRef<'m> {
        let def = self
            .defs
            .map
            .get(&def_id)
            .expect("BUG: definition not found");

        match &def.kind {
            DefKind::Type(TypeDef {
                ident: Some(_ident),
                ..
            }) => self.types.intern(Type::Domain(def_id)),
            DefKind::Type(TypeDef { ident: None, .. }) => {
                self.types.intern(Type::Anonymous(def_id))
            }
            DefKind::StringLiteral(_) => self.types.intern(Type::StringConstant(def_id)),
            DefKind::Regex(_) => self.types.intern(Type::Regex(def_id)),
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
                let expr = self.expressions.map.remove(expr_id).unwrap();
                let ty = match self.expected_constant_types.remove(&def_id) {
                    None => return self.types.intern(Type::Error),
                    Some(ty) => ty,
                };

                let mut ctx = HirBuildCtx::new();
                let node = self.build_node(&expr, Some(ty), &mut ctx);

                self.codegen_tasks
                    .push(CodegenTask::Const(ConstCodegenTask { def_id, node }));

                ty
            }
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }
}

use std::str::FromStr;

use ontol_runtime::DefId;
use ordered_float::NotNan;
use tracing::debug;

use crate::{
    codegen::task::ConstCodegenTask,
    def::{DefKind, MapDirection, TypeDef},
    mem::Intern,
    type_check::hir_build_ctx::HirBuildCtx,
    types::{Type, TypeRef},
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    /// Do type check of a type and then seal it.
    ///
    /// The type must be complete at the first usage site, e.g. in a map expression.
    pub fn check_def_sealed(&mut self, def_id: DefId) -> TypeRef<'m> {
        let ty = self.check_def_shallow(def_id);
        self.seal_def(def_id);
        ty
    }

    /// Compute the immediate type of a definition.
    /// Does not perform deep type check.
    ///
    /// This function is called for every definition in a loop.
    pub fn check_def_shallow(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_types.table.get(&def_id) {
            return type_ref;
        }
        let ty = self.check_def_inner(def_id);
        self.def_types.table.insert(def_id, ty);
        ty
    }

    fn check_def_inner(&mut self, def_id: DefId) -> TypeRef<'m> {
        let def = self
            .defs
            .table
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
            DefKind::Primitive(kind, _ident) => self.types.intern(Type::Primitive(*kind, def_id)),
            DefKind::Mapping(direction, variables, first_id, second_id) => {
                match self.check_map(def, *direction, variables, *first_id, *second_id) {
                    Ok(ty) => ty,
                    Err(error) => {
                        debug!("Aggregation group error: {error:?}");
                        self.types.intern(Type::Error)
                    }
                }
            }
            DefKind::Constant(expr_id) => {
                let expr = self.expressions.table.remove(expr_id).unwrap();
                let ty = match self.expected_constant_types.remove(&def_id) {
                    None => self.types.intern(Type::Error),
                    Some(ty) => ty,
                };

                let mut ctx = HirBuildCtx::new(expr.span, MapDirection::Omni);
                let node = self.build_node(&expr, Some(ty), &mut ctx);

                self.codegen_tasks
                    .add_const_task(ConstCodegenTask { def_id, node });

                ty
            }
            DefKind::NumberLiteral(lit) => {
                if lit.contains('.') {
                    match NotNan::<f64>::from_str(lit) {
                        Ok(num) => self.types.intern(Type::FloatConstant(num)),
                        Err(_) => self.types.intern(Type::Error),
                    }
                } else {
                    match i64::from_str(lit) {
                        Ok(num) => self.types.intern(Type::IntConstant(num)),
                        Err(_) => self.types.intern(Type::Error),
                    }
                }
            }
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }
}

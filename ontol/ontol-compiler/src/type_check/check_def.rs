use std::str::FromStr;

use ontol_runtime::{var::VarAllocator, DefId};
use ordered_float::NotNan;

use crate::{
    codegen::task::ConstCodegenTask,
    def::{DefKind, TypeDef},
    mem::Intern,
    type_check::hir_build_ctx::HirBuildCtx,
    types::{Type, TypeRef},
};

use super::{ena_inference::Strength, hir_build::NodeInfo, TypeCheck};

impl<'c, 'm> TypeCheck<'c, 'm> {
    /// Compute the immediate type of a definition.
    /// Performs relationship analysis for relationship definitions (rel statements).
    pub fn check_def(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_ty_ctx.table.get(&def_id) {
            return type_ref;
        }
        let ty = self.check_def_inner(def_id);
        self.def_ty_ctx.table.insert(def_id, ty);
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
            DefKind::TextLiteral(_) => self.types.intern(Type::TextConstant(def_id)),
            DefKind::Regex(_) => self.types.intern(Type::Regex(def_id)),
            DefKind::Relationship(relationship) => {
                self.check_relationship(def_id, relationship, &def.span)
            }
            DefKind::Edge => self.types.intern(Type::Tautology),
            DefKind::Primitive(kind, _ident) => self.types.intern(Type::Primitive(*kind, def_id)),
            DefKind::Mapping { .. } => self.types.intern(Type::Tautology),
            DefKind::Constant(pat_id) => {
                let pattern = self.patterns.table.remove(pat_id).unwrap();
                let ty = match self.expected_constant_types.remove(&def_id) {
                    None => self.types.intern(Type::Error),
                    Some(ty) => ty,
                };

                let mut ctx = HirBuildCtx::new(pattern.span, VarAllocator::default());
                let node = self.build_node(
                    &pattern,
                    NodeInfo {
                        expected_ty: Some((ty, Strength::Strong)),
                        parent_struct_flags: Default::default(),
                    },
                    &mut ctx,
                );

                self.code_ctx.add_const_task(ConstCodegenTask {
                    def_id,
                    node: ontol_hir::RootNode::new(node, ctx.hir_arena),
                });

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
            DefKind::Extern(_) => self.types.intern(Type::Extern(def_id)),
            DefKind::BuiltinRelType(..) => self.types.intern(Type::Tautology),
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }
}

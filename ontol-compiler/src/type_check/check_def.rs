use ontol_runtime::{smart_format, DefId};
use tracing::debug;

use crate::{
    def::{DefKind, TypeDef},
    error::CompileError,
    mem::Intern,
    primitive::PrimitiveKind,
    types::{Type, TypeRef},
};

use super::{TypeCheck, TypeError};

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
            DefKind::NumberLiteral(lit) => match self.expected_constant_types.get(&def_id) {
                None => {
                    self.error(
                        CompileError::TODO(smart_format!("No expected type for constant")),
                        &def.span,
                    );
                    self.types.intern(Type::Error)
                }
                Some(Type::Int(_)) => match lit.parse::<i64>() {
                    Ok(number) => {
                        let ty = self.types.intern(Type::IntConstant(number));
                        self.def_types.map.insert(def_id, ty);
                        ty
                    }
                    Err(_) => {
                        self.error(CompileError::InvalidInteger, &def.span);
                        self.types.intern(Type::Error)
                    }
                },
                Some(ty) => self.type_error(TypeError::NotConvertibleFromNumber(ty), &def.span),
            },
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }
}

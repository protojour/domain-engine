use fnv::FnvHashMap;
use ontol_hir::{kind::NodeKind, visitor::HirMutVisitor};
use ontol_runtime::smart_format;

use crate::{
    error::CompileError,
    typed_hir::{Meta, TypedHir, TypedHirNode},
    types::Types,
    CompileErrors,
};

use super::{
    hir_build_ctx::{Arm, VariableMapping},
    inference::{Infer, TypeVar},
    TypeError,
};

pub(super) struct HirArmTypeInference<'c, 'm> {
    pub(super) types: &'c mut Types<'m>,
    pub(super) eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
    pub(super) errors: &'c mut CompileErrors,
}

impl<'c, 'm> HirMutVisitor<'m, TypedHir> for HirArmTypeInference<'c, 'm> {
    fn visit_node(&mut self, index: usize, node: &mut <TypedHir as ontol_hir::Lang>::Node<'m>) {
        let mut infer = Infer {
            types: self.types,
            eq_relations: self.eq_relations,
        };
        match infer.infer_recursive(node.meta.ty) {
            Ok(ty) => node.meta.ty = ty,
            Err(TypeError::Propagated) => {}
            Err(TypeError::NotEnoughInformation) => {
                self.errors.push(
                    CompileError::TODO(smart_format!("Not enough type information"))
                        .spanned(&node.meta.span),
                );
            }
            _ => panic!("Unexpected inference error"),
        }

        self.visit_kind(index, &mut node.kind);
    }
}

pub(super) struct HirVariableMapper<'c, 'm> {
    pub variable_mapping: &'c FnvHashMap<ontol_hir::Variable, VariableMapping<'m>>,
    pub arm: Arm,
}

impl<'c, 'm> HirMutVisitor<'m, TypedHir> for HirVariableMapper<'c, 'm> {
    fn visit_node(&mut self, index: usize, node: &mut <TypedHir as ontol_hir::Lang>::Node<'m>) {
        self.visit_kind(index, &mut node.kind);

        if let NodeKind::VariableRef(var) = &node.kind {
            if let Some(var_mapping) = self.variable_mapping.get(var) {
                let arm = self.arm;
                let mapped_type = match arm {
                    Arm::First => var_mapping.second_arm_type,
                    Arm::Second => var_mapping.first_arm_type,
                };

                let variable_ref = TypedHirNode {
                    kind: NodeKind::VariableRef(*var),
                    meta: Meta {
                        ty: mapped_type,
                        span: node.meta.span,
                    },
                };
                let map = TypedHirNode {
                    kind: NodeKind::Map(Box::new(variable_ref)),
                    meta: node.meta,
                };
                *node = map;
            }
        }
    }
}

use fnv::FnvHashMap;
use ontol_runtime::smart_format;

use crate::{
    error::CompileError,
    typed_hir::{Meta, TypedArena, TypedHir, TypedHirData},
    types::Types,
    CompileErrors,
};

use super::{
    hir_build_ctx::{Arm, VariableMapping},
    inference::{Infer, TypeVar},
    TypeError,
};

/// Perform type inference limited to within one "map arm"
pub(super) struct HirArmTypeInference<'c, 'm> {
    pub(super) types: &'c mut Types<'m>,
    pub(super) eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
    pub(super) errors: &'c mut CompileErrors,
}

impl<'c, 'm> HirArmTypeInference<'c, 'm> {
    pub fn infer_node(&mut self, node: &mut TypedHirData<'m, ontol_hir::Kind<'m, TypedHir>>) {
        let mut infer = Infer {
            types: self.types,
            eq_relations: self.eq_relations,
        };
        match infer.infer_recursive(node.ty()) {
            Ok(ty) => node.meta_mut().ty = ty,
            Err(TypeError::Propagated) => {}
            Err(TypeError::NotEnoughInformation) => {
                self.errors.error(
                    CompileError::TODO(smart_format!("Not enough type information")),
                    &node.span(),
                );
            }
            _ => panic!("Unexpected inference error"),
        }
    }
}

/// Unify the types of variables in both map arms.
/// If the types do not match, the variable expression gets changed to a map expression.
pub(super) struct HirVariableMapper<'c, 'm> {
    pub variable_mapping: &'c FnvHashMap<ontol_hir::Var, VariableMapping<'m>>,
    pub arm: Arm,
}

impl<'c, 'm> HirVariableMapper<'c, 'm> {
    pub fn map_vars(&mut self, arena: &mut TypedArena<'m>) {
        let mut alloc = arena.pre_allocator();
        let mut new_var_nodes: Vec<TypedHirData<'m, ontol_hir::Kind<'m, TypedHir>>> = vec![];

        for data in arena.iter_mut() {
            if let ontol_hir::Kind::Var(var) = data.hir() {
                if let Some(var_mapping) = self.variable_mapping.get(var) {
                    let arm = self.arm;
                    let mapped_type = match arm {
                        Arm::First => var_mapping.second_arm_type,
                        Arm::Second => var_mapping.first_arm_type,
                    };

                    // Make a new node which is the new var reference
                    new_var_nodes.push(TypedHirData(
                        ontol_hir::Kind::Var(*var),
                        Meta {
                            ty: mapped_type,
                            span: data.span(),
                        },
                    ));

                    // Replace the old node with a Map expression referencing the new var node
                    *data.hir_mut() = ontol_hir::Kind::Map(alloc.prealloc_node());
                }
            }
        }

        // Actually append the new variable nodes to the arena
        for new_var_node in new_var_nodes {
            arena.add(new_var_node);
        }
    }
}

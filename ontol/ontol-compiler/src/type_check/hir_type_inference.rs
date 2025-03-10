use fnv::FnvHashMap;
use ontol_runtime::var::Var;

use crate::{
    CompileErrors,
    error::CompileError,
    typed_hir::{Meta, TypedArena, TypedHir, TypedHirData},
    types::TypeCtx,
};

use super::{
    TypeError,
    ena_inference::{Infer, TypeVar},
    hir_build_ctx::{Arm, VariableMapping},
};

/// Perform type inference limited to within one "map arm"
pub(super) struct HirArmTypeInference<'c, 'm> {
    pub(super) types: &'c mut TypeCtx<'m>,
    pub(super) eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
    pub(super) errors: &'c mut CompileErrors,
}

impl<'m> HirArmTypeInference<'_, 'm> {
    pub fn infer(&mut self, data: &mut TypedHirData<'m, ontol_hir::Kind<'m, TypedHir>>) {
        let mut infer = Infer {
            types: self.types,
            eq_relations: self.eq_relations,
        };
        match infer.infer_recursive(data.ty()) {
            Ok(ty) => data.meta_mut().ty = ty,
            Err(TypeError::Propagated) => {}
            Err(TypeError::NotEnoughInformation) => {
                CompileError::TODO("Not enough type information")
                    .span(data.span())
                    .report(&mut self.errors);
            }
            _ => panic!("Unexpected inference error"),
        }
    }
}

/// Unify the types of variables in both map arms.
/// If the types do not match, the variable expression gets changed to a map expression.
pub(super) struct HirVariableMapper<'c, 'm> {
    pub variable_mapping: &'c FnvHashMap<Var, VariableMapping<'m>>,
    pub arm: Arm,
}

impl<'m> HirVariableMapper<'_, 'm> {
    pub fn map_vars(&mut self, arena: &mut TypedArena<'m>) {
        let mut alloc = arena.pre_allocator();
        let mut new_var_nodes: Vec<TypedHirData<'m, ontol_hir::Kind<'m, TypedHir>>> = vec![];

        for data in arena.iter_data_mut() {
            if let ontol_hir::Kind::Var(var) = data.hir() {
                match self.variable_mapping.get(var) {
                    Some(VariableMapping::Mapping(types)) => {
                        let arm = self.arm;
                        let mapped_type = match arm {
                            Arm::Upper => types[1],
                            Arm::Lower => types[0],
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
                    Some(VariableMapping::Overwrite(ty)) => {
                        // Write the type from a strong binding into a weak binding,
                        // for correctness. The unifier can see the proper type of each variable use.
                        data.meta_mut().ty = ty;
                    }
                    _ => {}
                }
            }
        }

        // Actually append the new variable nodes to the arena
        for new_var_node in new_var_nodes {
            arena.add(new_var_node);
        }
    }
}

use ontol_hir::Label;
use ontol_runtime::var::VarAllocator;

use crate::{
    codegen::{
        task::{ExplicitMapCodegenTask, MapCodegenTask},
        type_mapper::TypeMapper,
    },
    def::Def,
    error::CompileError,
    map::UndirectedMapKey,
    mem::Intern,
    pattern::PatId,
    repr::repr_model::ReprKind,
    type_check::hir_build_ctx::{Arm, VariableMapping},
    typed_hir::TypedRootNode,
    types::{Type, TypeRef},
    Note, SpannedNote,
};

use super::{
    ena_inference::{KnownType, Strength},
    hir_build_ctx::{HirBuildCtx, ARMS},
    hir_type_inference::{HirArmTypeInference, HirVariableMapper},
    map_arm_analyze::PreAnalyzer,
    TypeCheck, TypeEquation, TypeError,
};

pub enum MapOutputClass {
    /// The output is interpreted as a function of the opposing arm.
    /// All output data can be derived and computed from from input data.
    Data,
    /// The output is interpreted as match on some entity storage, meant to produce one value.
    /// Requires some form of runtime datastore to be computed.
    FindMatch,
    /// The output is interpreted as a match on some entity store meant to produce several values.
    /// Requires some form of runtime datastore to be computed.
    FilterMatch,
}

#[derive(Debug)]
pub enum CheckMapError {
    MutualArmInference,
    ArmNotInferrable,
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<Label>),
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_map(
        &mut self,
        def: &Def,
        var_allocator: &VarAllocator,
        pat_ids: [PatId; 2],
    ) -> Result<TypeRef<'m>, CheckMapError> {
        let mut ctx = HirBuildCtx::new(def.span, VarAllocator::from(*var_allocator.peek_next()));

        let mut analyzer = PreAnalyzer {
            errors: self.errors,
        };
        for (pat_id, arm) in pat_ids.iter().zip(ARMS) {
            let _entered = arm.tracing_debug_span().entered();

            ctx.current_arm = arm;
            analyzer.analyze_arm(self.patterns.table.get(pat_id).unwrap(), None, &mut ctx)?;
        }

        self.build_typed_ontol_hir_arms(
            def,
            [(pat_ids[0], Arm::First), (pat_ids[1], Arm::Second)],
            &mut ctx,
        )?;

        self.report_missing_prop_errors(&mut ctx);

        Ok(self.types.intern(Type::Tautology))
    }

    fn build_typed_ontol_hir_arms(
        &mut self,
        def: &Def,
        input: [(PatId, Arm); 2],
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<(), CheckMapError> {
        let mut arm_nodes = input.map(|(pat_id, arm)| {
            let _entered = arm.tracing_debug_span().entered();

            ctx.current_arm = arm;
            let mut root_node = self.build_root_pattern(pat_id, ctx);
            self.infer_hir_arm_types(&mut root_node, ctx);
            root_node
        });

        // unify the type of variables on either side:
        self.infer_hir_unify_arms(def, &mut arm_nodes, ctx);

        if let Some(key_pair) = TypeMapper::new(self.relations, self.defs, self.seal_ctx)
            .find_map_key_pair([arm_nodes[0].data().ty(), arm_nodes[1].data().ty()])
        {
            self.codegen_tasks.add_map_task(
                key_pair,
                MapCodegenTask::Explicit(ExplicitMapCodegenTask {
                    def_id: def.id,
                    arms: arm_nodes,
                    span: def.span,
                }),
            );
        }

        Ok(())
    }

    fn infer_hir_arm_types(
        &mut self,
        hir_root_node: &mut TypedRootNode<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) {
        let mut inference = HirArmTypeInference {
            types: self.types,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        for data in hir_root_node.arena_mut().iter_data_mut() {
            inference.infer(data);
        }
    }

    fn infer_hir_unify_arms(
        &mut self,
        def: &Def,
        arm_nodes: &mut [TypedRootNode<'m>; 2],
        ctx: &mut HirBuildCtx<'m>,
    ) {
        for (var, explicit_var) in &mut ctx.pattern_variables {
            let var_arms = match ARMS.map(|arm| explicit_var.hir_arms.get(&arm)) {
                [Some(first), Some(second)] => [first, second],
                _ => continue,
            };
            let type_vars = var_arms.map(|var_arm| ctx.inference.new_type_variable(var_arm.pat_id));

            let Err(type_error) = ctx
                .inference
                .eq_relations
                .unify_var_var(type_vars[0], type_vars[1])
            else {
                continue;
            };
            let TypeError::Mismatch(TypeEquation { actual, expected }) = type_error else {
                unreachable!();
            };

            match (actual.0.get_single_def_id(), expected.0.get_single_def_id()) {
                (Some(first_def_id), Some(second_def_id))
                    if actual.0.is_domain_specific() || expected.0.is_domain_specific() =>
                {
                    if let Some(ty) = self
                        .get_strong_type_for_distinct_weakly_repr_compatible_types(actual, expected)
                    {
                        ctx.variable_mapping
                            .insert(*var, VariableMapping::Overwrite(ty));
                    } else {
                        ctx.variable_mapping
                            .insert(*var, VariableMapping::Mapping([actual.0, expected.0]));

                        self.codegen_tasks.add_map_task(
                            UndirectedMapKey::new([first_def_id.into(), second_def_id.into()]),
                            MapCodegenTask::Auto(def.id.package_id()),
                        );
                    }
                }
                _ => {
                    self.type_error(
                        TypeError::Mismatch(TypeEquation { actual, expected }),
                        &var_arms[1].span,
                    );
                }
            }
        }

        for (node, arm) in arm_nodes.iter_mut().zip(ARMS) {
            HirVariableMapper {
                variable_mapping: &ctx.variable_mapping,
                arm,
            }
            .map_vars(node.arena_mut());
        }
    }

    /// Computes whether two scalar types are compatible when one has
    /// a weak type constraint and the other has a strong strong type constraint.
    /// In that case, the variables do not need to be mapped.
    /// If successfull, returns the strong type.
    fn get_strong_type_for_distinct_weakly_repr_compatible_types(
        &self,
        (first_ty, first_strength): KnownType<'m>,
        (second_ty, second_strength): KnownType<'m>,
    ) -> Option<TypeRef<'m>> {
        if first_strength == second_strength {
            return None;
        }

        let first_def_id = first_ty.get_single_def_id().unwrap();
        let second_def_id = second_ty.get_single_def_id().unwrap();
        let first_repr = self.seal_ctx.get_repr_kind(&first_def_id).unwrap();
        let second_repr = self.seal_ctx.get_repr_kind(&second_def_id).unwrap();

        match (first_repr, second_repr) {
            (
                ReprKind::Scalar(first_sc_def_id, first_kind, _),
                ReprKind::Scalar(second_sc_def_id, second_kind, _),
            ) => {
                if first_kind != second_kind {
                    return None;
                }

                if first_sc_def_id == second_sc_def_id {
                    if first_strength == Strength::Strong {
                        Some(first_ty)
                    } else {
                        Some(second_ty)
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn report_missing_prop_errors(&mut self, ctx: &mut HirBuildCtx<'m>) {
        for (_arm, missing) in std::mem::take(&mut ctx.missing_properties) {
            for (span, properties) in missing {
                let error = CompileError::MissingProperties(properties);
                self.error_with_notes(
                    error,
                    &span,
                    vec![SpannedNote {
                        note: Note::ConsiderUsingMatch,
                        span,
                    }],
                );
            }
        }
    }
}

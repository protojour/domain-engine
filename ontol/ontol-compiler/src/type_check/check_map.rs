use ontol_hir::Label;
use ontol_parser::source::SourceSpan;
use ontol_runtime::{DefId, var::VarAllocator};

use crate::{
    MissingProperties, Note, arm_span,
    codegen::{
        task::{AbstractTemplate, MapCodegenRequest, OntolMap, OntolMapArms},
        type_mapper::TypeMapper,
    },
    def::DefKind,
    error::CompileError,
    map::UndirectedMapKey,
    mem::Intern,
    pattern::{PatId, PatternKind, TypePath},
    repr::repr_model::ReprKind,
    type_check::hir_build_ctx::{Arm, VariableMapping},
    typed_hir::TypedRootNode,
    types::{Type, TypeRef},
};

use super::{
    MapArmsKind, TypeCheck, TypeEquation, TypeError,
    ena_inference::{KnownType, Strength},
    hir_build_ctx::{ARMS, HirBuildCtx},
    hir_build_props::MatchAttributeKey,
    hir_type_inference::{HirArmTypeInference, HirVariableMapper},
    map_arm_analyze::PreAnalyzer,
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

#[expect(unused)]
#[derive(Debug)]
pub enum CheckMapError {
    DepthExceeded,
    RootCount(usize),
    NoLeaves,
    TooManyLeaves(Vec<Label>),
}

impl<'m> TypeCheck<'_, 'm> {
    pub fn check_map(
        &mut self,
        map_def: (DefId, SourceSpan),
        var_allocator: &VarAllocator,
        pat_ids: [PatId; 2],
        arms_kind: MapArmsKind,
    ) -> Result<TypeRef<'m>, CheckMapError> {
        if matches!(arms_kind, MapArmsKind::Abstract) {
            let upper = self.expect_pattern_abstract_def_id(pat_ids[0]);
            let lower = self.expect_pattern_abstract_def_id(pat_ids[1]);

            if let (Some(upper), Some(lower)) = (upper, lower) {
                let key_pair = UndirectedMapKey::new([upper.into(), lower.into()]);

                self.code_ctx.add_map_task(
                    key_pair,
                    MapCodegenRequest::ExplicitOntol(OntolMap {
                        map_def_id: map_def.0,
                        arms: OntolMapArms::Abstract([upper, lower]),
                        span: map_def.1,
                    }),
                    self.defs,
                    self.errors,
                );

                self.code_ctx.abstract_templates.insert(
                    key_pair,
                    AbstractTemplate {
                        directed_def_ids: [upper, lower],
                        pat_ids,
                        var_allocator: VarAllocator::from(*var_allocator.peek_next()),
                    },
                );
            }
        } else {
            let mut ctx =
                HirBuildCtx::new(map_def.1, VarAllocator::from(*var_allocator.peek_next()));

            let mut analyzer = PreAnalyzer {
                errors: self.errors,
            };
            for (pat_id, arm) in pat_ids.iter().zip(ARMS) {
                let _entered = arm_span!(arm).entered();

                ctx.current_arm = arm;

                let pattern = self.patterns.table.get(pat_id).unwrap();
                analyzer.analyze_arm(pattern, None, &mut ctx)?;
            }

            self.build_typed_ontol_hir_arms(
                map_def,
                [(pat_ids[0], Arm::Upper), (pat_ids[1], Arm::Lower)],
                arms_kind,
                &mut ctx,
            )?;

            self.report_missing_prop_errors(&mut ctx);
        }

        Ok(self.type_ctx.intern(Type::Tautology))
    }

    fn build_typed_ontol_hir_arms(
        &mut self,
        map_def: (DefId, SourceSpan),
        input: [(PatId, Arm); 2],
        arms_kind: MapArmsKind,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Result<(), CheckMapError> {
        let mut arm_nodes = input.map(|(pat_id, arm)| {
            let _entered = arm_span!(arm).entered();

            ctx.current_arm = arm;
            let mut root_node = self.build_root_pattern(pat_id, arms_kind.clone(), ctx);
            self.infer_hir_arm_types(&mut root_node, ctx);
            root_node
        });

        // unify the type of variables on either side:
        self.infer_hir_unify_arms(map_def, &mut arm_nodes, ctx);

        if let Some(key_pair) = TypeMapper::new(self.rel_ctx, self.defs, self.repr_ctx)
            .find_map_key_pair([arm_nodes[0].data().ty(), arm_nodes[1].data().ty()])
        {
            self.code_ctx.add_map_task(
                key_pair,
                MapCodegenRequest::ExplicitOntol(OntolMap {
                    map_def_id: map_def.0,
                    arms: OntolMapArms::Patterns(arm_nodes),
                    span: map_def.1,
                }),
                self.defs,
                self.errors,
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
            types: self.type_ctx,
            eq_relations: &mut ctx.inference.eq_relations,
            errors: self.errors,
        };
        for data in hir_root_node.arena_mut().iter_data_mut() {
            inference.infer(data);
        }
    }

    fn infer_hir_unify_arms(
        &mut self,
        map_def: (DefId, SourceSpan),
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

                        self.code_ctx.add_map_task(
                            UndirectedMapKey::new([first_def_id.into(), second_def_id.into()]),
                            MapCodegenRequest::Auto(map_def.0.domain_index()),
                            self.defs,
                            self.errors,
                        );
                    }
                }
                _ => {
                    TypeError::Mismatch(TypeEquation { actual, expected })
                        .report(var_arms[1].span, self);
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
        let first_repr = self.repr_ctx.get_repr_kind(&first_def_id).unwrap();
        let second_repr = self.repr_ctx.get_repr_kind(&second_def_id).unwrap();

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
            for (span, attr_keys) in missing {
                let mut formatted_properties = vec![];
                for attr_key in attr_keys {
                    match attr_key {
                        MatchAttributeKey::Named(name) => {
                            formatted_properties.push(format!("`{name}`"))
                        }
                        MatchAttributeKey::Def(def_id) => match self.defs.def_kind(def_id) {
                            DefKind::Type(type_def) => {
                                if let Some(ident) = type_def.ident.as_ref() {
                                    formatted_properties.push((*ident).into());
                                } else {
                                    formatted_properties.push("<anonymous>".into());
                                }
                            }
                            _ => {
                                formatted_properties.push("<anonymous>".into());
                            }
                        },
                    }
                }

                let error =
                    CompileError::MissingProperties(MissingProperties(formatted_properties));
                error
                    .span(span)
                    .with_note(Note::ConsiderUsingMatch.span(span))
                    .report(self);
            }
        }
    }

    fn expect_pattern_abstract_def_id(&mut self, pat_id: PatId) -> Option<DefId> {
        self.get_pattern_abstract_def_id(pat_id).or_else(|| {
            let pat = self.patterns.table.get(&pat_id).unwrap();
            CompileError::TODO("must be named compound pattern")
                .span(pat.span)
                .report(self);

            None
        })
    }

    fn get_pattern_abstract_def_id(&self, pat_id: PatId) -> Option<DefId> {
        match &self.patterns.table.get(&pat_id)?.kind {
            PatternKind::Compound { type_path, .. } => match type_path {
                TypePath::Specified { def_id, .. } => Some(*def_id),
                TypePath::Inferred { .. } => None,
                TypePath::RelContextual => None,
            },
            _ => None,
        }
    }
}

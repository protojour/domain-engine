use indexmap::IndexMap;
use ontol_hir::Label;
use ontol_runtime::{
    var::{Var, VarSet},
    DefId,
};
use tracing::debug;

use crate::{
    hir_unify::{
        expr,
        flat_scope::{self, OutputVar},
        flat_unifier::StructuralOrigin,
        flat_unifier_impl::{apply_lateral_scope, unify_scope_structural},
        seq_type_infer::SeqTypeInfer,
    },
    typed_hir::{self, IntoTypedHirData, TypedHir, TypedHirData, UNIT_META},
    NO_SPAN,
};

use super::{
    flat_level_builder::LevelBuilder,
    flat_scope::ScopeVar,
    flat_unifier::{FlatUnifier, Level, MainScope},
    flat_unifier_table::{ExprSelector, Table},
    UnifierResult,
};

pub(super) fn unify_regex<'m>(
    (main_scope, selector, level): (MainScope, ExprSelector, Level),
    (index, scope_var): (usize, ScopeVar),
    (opt_seq_label, regex_def_id, regex_hir_meta): (Option<Label>, DefId, typed_hir::Meta<'m>),
    in_scope: &VarSet,
    (builder, next_indexes): (&mut LevelBuilder, &mut Vec<usize>),
    table: &mut Table<'m>,
    unifier: &mut FlatUnifier<'_, 'm>,
) -> UnifierResult<()> {
    let next_in_scope = in_scope.union_one(scope_var.0);

    let capture_scope_union = {
        let mut union = VarSet::default();
        for alt_idx in table.dependees(Some(scope_var)) {
            let alt_scope_var = table.scope_maps[alt_idx].scope.meta().scope_var;

            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                let cap_scope_map = &mut table.scope_maps[cap_scope_idx];
                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                union.insert(cap_scope_var.0);
            }
        }
        union
    };

    let scope_map = &mut table.scope_maps[index];

    if !in_scope.0.is_disjoint(&capture_scope_union.0) {
        builder.output.extend(apply_lateral_scope(
            main_scope,
            scope_map.select_assignments(selector),
            &|| in_scope.clone(),
            table,
            unifier,
            level.next(),
        )?);
        next_indexes.extend(table.dependees(Some(scope_var)));
    } else if let Some(seq_label) = opt_seq_label {
        debug!("{level}looping regex");
        // looping regex
        let mut match_arms: Vec<ontol_hir::CaptureMatchArm<'m, TypedHir>> = vec![];

        let mut seq_type_inferers: IndexMap<ontol_hir::Label, SeqTypeInfer<'m>> =
            Default::default();

        let mut hir_props: Vec<ontol_hir::Node> = vec![];

        // Analyze which sequences are under scrutiny and allocate output variables
        for assignment in std::mem::take(&mut scope_map.assignments) {
            if let expr::Expr(expr::Kind::Prop(prop), meta) = assignment.expr {
                if let expr::PropVariant::Seq { label, .. } = &prop.variant {
                    let inferer = seq_type_inferers.entry(*label).or_insert_with(|| {
                        SeqTypeInfer::new(OutputVar(unifier.var_allocator.alloc()))
                    });

                    let rel = unifier.mk_node(ontol_hir::Kind::Unit, UNIT_META);
                    let val = unifier.mk_node(
                        ontol_hir::Kind::Var(inferer.output_seq_var.0),
                        meta.hir_meta,
                    );

                    hir_props.push(
                        unifier.mk_node(
                            ontol_hir::Kind::Prop(
                                prop.optional,
                                prop.struct_var,
                                prop.prop_id,
                                [ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                    rel,
                                    val,
                                })]
                                .into(),
                            ),
                            meta.hir_meta,
                        ),
                    );
                }
            }
        }

        // Input for sequence type "inference"
        for scope_map in &mut table.scope_maps {
            for assignment in &scope_map.assignments {
                if let expr::Kind::SeqItem(label, _, _, attr) = assignment.expr.kind() {
                    if let Some(infer) = seq_type_inferers.get_mut(label) {
                        infer
                            .types
                            .push((attr.rel.meta().hir_meta.ty, attr.val.meta().hir_meta.ty));
                    }
                }
            }
        }

        // alternations:
        for alt_idx in table.dependees(Some(scope_var)) {
            let alt_scope_var = table.scope_maps[alt_idx].scope.meta().scope_var;

            let mut match_arm = ontol_hir::CaptureMatchArm {
                capture_groups: vec![],
                nodes: Default::default(),
            };
            let mut captured_scope = VarSet::default();

            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                let cap_scope_map = &mut table.scope_maps[cap_scope_idx];
                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                let flat_scope::Kind::RegexCapture(cap_index) = cap_scope_map.scope.kind() else {
                    panic!("Expected regex capture");
                };

                match_arm.capture_groups.push(ontol_hir::CaptureGroup {
                    index: *cap_index,
                    binder: ontol_hir::Binder {
                        var: cap_scope_var.0,
                    }
                    .with_meta(cap_scope_map.scope.meta().hir_meta),
                });
                captured_scope.insert(cap_scope_var.0);
            }

            match_arm.nodes.extend(apply_lateral_scope(
                MainScope::MultiSequence(&seq_type_inferers),
                table.scope_maps[alt_idx].take_assignments(),
                &|| next_in_scope.union(&captured_scope),
                table,
                unifier,
                level.next(),
            )?);

            match_arm.nodes.extend(unify_scope_structural(
                (main_scope, ExprSelector::SeqItem(seq_label), level.next()),
                StructuralOrigin::DependeesOf(alt_scope_var),
                captured_scope,
                table,
                unifier,
            )?);

            match_arms.push(match_arm);
        }

        let mut nodes: ontol_hir::Nodes = Default::default();
        nodes.push(unifier.mk_node(
            ontol_hir::Kind::MatchRegex(
                ontol_hir::Iter(true),
                scope_var.0,
                regex_def_id,
                match_arms,
            ),
            regex_hir_meta,
        ));
        nodes.extend(hir_props);

        // wrap everything in sequence binders
        for (label, seq_type_infer) in seq_type_inferers {
            let output_seq_var = seq_type_infer.output_seq_var;
            let sequence_ty = seq_type_infer.infer(unifier.types);
            let sequence_meta = typed_hir::Meta {
                ty: sequence_ty,
                // FIXME: SPAN
                span: NO_SPAN,
            };

            let let_def = unifier.mk_node(
                ontol_hir::Kind::Sequence(
                    ontol_hir::Binder { var: Var(label.0) }.with_meta(sequence_meta),
                    ontol_hir::Nodes::default(),
                ),
                sequence_meta,
            );

            let let_node = unifier.mk_node(
                ontol_hir::Kind::Let(
                    ontol_hir::Binder {
                        var: output_seq_var.0,
                    }
                    .with_meta(UNIT_META),
                    let_def,
                    nodes,
                ),
                UNIT_META,
            );

            nodes = Default::default();
            nodes.push(let_node);
        }

        builder.output.extend(nodes);
    } else {
        // normal, one-shot regex
        let mut match_arms: Vec<ontol_hir::CaptureMatchArm<'m, TypedHir>> = vec![];

        // alternations:
        for alt_idx in table.dependees(Some(scope_var)) {
            let alt_scope_map = &mut table.scope_maps[alt_idx];
            let alt_scope_var = alt_scope_map.scope.meta().scope_var;

            let mut match_arm = ontol_hir::CaptureMatchArm {
                capture_groups: vec![],
                nodes: ontol_hir::Nodes::default(),
            };
            let mut captured_scope = VarSet::default();

            for cap_scope_idx in table.dependees(Some(alt_scope_var)) {
                let cap_scope_map = &mut table.scope_maps[cap_scope_idx];
                let cap_scope_var = cap_scope_map.scope.meta().scope_var;

                let flat_scope::Kind::RegexCapture(cap_index) = cap_scope_map.scope.kind() else {
                    panic!("Expected regex capture");
                };

                match_arm.capture_groups.push(ontol_hir::CaptureGroup {
                    index: *cap_index,
                    binder: TypedHirData(
                        ontol_hir::Binder {
                            var: cap_scope_var.0,
                        },
                        cap_scope_map.scope.meta().hir_meta,
                    ),
                });
                captured_scope.insert(cap_scope_var.0);
            }

            match_arm.nodes.extend(unify_scope_structural(
                (main_scope, selector, level.next()),
                StructuralOrigin::DependeesOf(alt_scope_var),
                captured_scope,
                table,
                unifier,
            )?);

            match_arms.push(match_arm);
        }

        builder.output.push(unifier.mk_node(
            ontol_hir::Kind::MatchRegex(
                ontol_hir::Iter(false),
                scope_var.0,
                regex_def_id,
                match_arms,
            ),
            regex_hir_meta,
        ));
    }

    Ok(())
}

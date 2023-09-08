use indexmap::IndexMap;
use ontol_runtime::value::PropertyId;

use crate::{
    hir_unify::flat_scope,
    typed_hir::{self, TypedBinder, TypedHir, TypedHirNode},
};

use super::{flat_scope::ScopeVar, flat_unifier::FlatUnifier, flat_unifier_table::Table};

#[derive(Default)]
pub(super) struct LevelBuilder<'m> {
    pub output: Vec<TypedHirNode<'m>>,
    merged_match_arms_table: IndexMap<(ontol_hir::Var, PropertyId), MergedMatchArms<'m>>,
}

#[derive(Default)]
pub(super) struct MergedMatchArms<'m> {
    optional: ontol_hir::Optional,
    match_arms: Vec<ontol_hir::PropMatchArm<'m, TypedHir>>,
}

impl<'m> LevelBuilder<'m> {
    pub fn build<'a>(mut self, unifier: &mut FlatUnifier<'a, 'm>) -> Vec<TypedHirNode<'m>> {
        for ((struct_var, property_id), mut merged_match_arms) in self.merged_match_arms_table {
            if !merged_match_arms.match_arms.is_empty() {
                if merged_match_arms.optional.0 {
                    merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                        pattern: ontol_hir::PropPattern::Absent,
                        nodes: vec![],
                    });
                }

                self.output.push(TypedHirNode(
                    ontol_hir::Kind::MatchProp(
                        struct_var,
                        property_id,
                        merged_match_arms.match_arms,
                    ),
                    unifier.unit_meta(),
                ));
            }
        }

        self.output
    }

    pub fn add_prop_variant_scope(
        &mut self,
        scope_var: ScopeVar,
        (optional, struct_var, property_id): (ontol_hir::Optional, ontol_hir::Var, PropertyId),
        body: Vec<TypedHirNode<'m>>,
        table: &mut Table<'m>,
    ) {
        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        if !body.is_empty() {
            let (rel_binding, val_binding) = table.rel_val_bindings(scope_var);
            merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                pattern: ontol_hir::PropPattern::Attr(rel_binding, val_binding),
                nodes: body,
            });
        }
    }

    pub fn add_seq_prop_variant_scope(
        &mut self,
        scope_var: ScopeVar,
        (optional, struct_var, property_id): (ontol_hir::Optional, ontol_hir::Var, PropertyId),
        body: Vec<TypedHirNode<'m>>,
        table: &mut Table<'m>,
    ) {
        let scope_map = table
            .table_mut()
            .iter()
            .find(|scope_map| scope_map.scope.meta().scope_var == scope_var)
            .unwrap();

        let flat_scope::Kind::SeqPropVariant(label, ..) = scope_map.scope.kind() else {
            panic!();
        };

        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        if !body.is_empty() {
            let label_binding = ontol_hir::Binding::Binder(TypedBinder {
                var: scope_var.0,
                meta: typed_hir::Meta {
                    ty: label.ty,
                    span: scope_map.scope.meta().hir_meta.span,
                },
            });

            merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                pattern: ontol_hir::PropPattern::Seq(label_binding, ontol_hir::HasDefault(false)),
                nodes: body,
            });
        }
    }
}

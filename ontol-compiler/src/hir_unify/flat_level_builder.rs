use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, var::Var};

use crate::{
    hir_unify::flat_scope,
    typed_hir::{self, IntoTypedHirData, TypedHir},
    types::UNIT_TYPE,
    NO_SPAN,
};

use super::{
    flat_scope::ScopeVar,
    flat_unifier::FlatUnifier,
    flat_unifier_table::{RelValBindings, Table},
};

#[derive(Default)]
pub(super) struct LevelBuilder<'m> {
    pub output: Vec<ontol_hir::Node>,
    merged_match_arms_table: IndexMap<(Var, PropertyId), MergedMatchArms<'m>>,
}

#[derive(Default)]
pub(super) struct MergedMatchArms<'m> {
    optional: ontol_hir::Optional,
    match_arms: Vec<(ontol_hir::PropPattern<'m, TypedHir>, ontol_hir::Nodes)>,
}

impl<'m> LevelBuilder<'m> {
    pub fn build<'a>(mut self, unifier: &mut FlatUnifier<'a, 'm>) -> Vec<ontol_hir::Node> {
        for ((struct_var, property_id), mut merged_match_arms) in self.merged_match_arms_table {
            if !merged_match_arms.match_arms.is_empty() {
                if merged_match_arms.optional.0 {
                    merged_match_arms
                        .match_arms
                        .push((ontol_hir::PropPattern::Absent, ontol_hir::Nodes::default()));
                }

                let mut ty = &UNIT_TYPE;

                // Compute the type of the match-prop expression.
                // FIXME: Handle the case where there are multiple arms
                // with differing type.
                for (_, nodes) in &merged_match_arms.match_arms {
                    if let Some(node) = nodes.last() {
                        ty = unifier.hir_arena[*node].meta().ty;
                    }
                }

                self.output.push(unifier.mk_node(
                    ontol_hir::Kind::MatchProp(
                        struct_var,
                        property_id,
                        merged_match_arms.match_arms.into(),
                    ),
                    typed_hir::Meta { ty, span: NO_SPAN },
                ));
            }
        }

        self.output
    }

    pub fn add_prop_variant_scope(
        &mut self,
        (optional, struct_var, property_id): (ontol_hir::Optional, Var, PropertyId),
        bindings: RelValBindings<'m>,
        body: Vec<ontol_hir::Node>,
    ) {
        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        if !body.is_empty() {
            merged_match_arms.match_arms.push((
                ontol_hir::PropPattern::Attr(bindings.rel, bindings.val),
                body.into(),
            ));
        }
    }

    pub fn add_seq_prop_variant_scope(
        &mut self,
        scope_var: ScopeVar,
        (optional, has_default, struct_var, property_id): (
            ontol_hir::Optional,
            ontol_hir::HasDefault,
            Var,
            PropertyId,
        ),
        body: Vec<ontol_hir::Node>,
        table: &mut Table<'m>,
    ) {
        let scope_map = table
            .scope_maps
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
            let label_binding = ontol_hir::Binding::Binder(
                ontol_hir::Binder { var: scope_var.0 }.with_meta(typed_hir::Meta {
                    ty: label.ty(),
                    span: scope_map.scope.meta().hir_meta.span,
                }),
            );

            merged_match_arms.match_arms.push((
                ontol_hir::PropPattern::Set(label_binding, has_default),
                body.into(),
            ));
        }
    }
}

use indexmap::IndexMap;
use ontol_runtime::value::PropertyId;

use crate::{
    hir_unify::flat_scope,
    typed_hir::{self, TypedBinder, TypedHir, TypedHirNode},
    types::Types,
};

use super::{flat_unifier::unit_meta, flat_unifier_table::Table};

#[derive(Default)]
pub(super) struct MergedMatchArms<'m> {
    optional: ontol_hir::Optional,
    match_arms: Vec<ontol_hir::PropMatchArm<'m, TypedHir>>,
}

#[derive(Default)]
pub(super) struct LevelBuilder<'m> {
    pub output: Vec<TypedHirNode<'m>>,
    merged_match_arms_table: IndexMap<(ontol_hir::Var, PropertyId), MergedMatchArms<'m>>,
}

impl<'m> LevelBuilder<'m> {
    pub fn build(mut self, types: &mut Types<'m>) -> Vec<TypedHirNode<'m>> {
        for ((struct_var, property_id), mut merged_match_arms) in self.merged_match_arms_table {
            if merged_match_arms.optional.0 {
                merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                    pattern: ontol_hir::PropPattern::Absent,
                    nodes: vec![],
                });
            }

            let meta = unit_meta(types);
            self.output.push(TypedHirNode(
                ontol_hir::Kind::MatchProp(struct_var, property_id, merged_match_arms.match_arms),
                meta,
            ));
        }

        self.output
    }

    pub fn add_prop_variant_scope(
        &mut self,
        scope_var: ontol_hir::Var,
        (optional, struct_var, property_id): (ontol_hir::Optional, ontol_hir::Var, PropertyId),
        body: Vec<TypedHirNode<'m>>,
        table: &mut Table<'m>,
    ) {
        let var_attribute = table.scope_prop_variant_bindings(scope_var);

        fn make_binding<'m>(
            scope_node: Option<&flat_scope::ScopeNode<'m>>,
        ) -> ontol_hir::Binding<'m, TypedHir> {
            match scope_node {
                Some(scope_node) => ontol_hir::Binding::Binder(TypedBinder {
                    var: scope_node.meta().var,
                    meta: scope_node.meta().hir_meta,
                }),
                None => ontol_hir::Binding::Wildcard,
            }
        }

        let rel_binding = make_binding(
            var_attribute
                .rel
                .and_then(|var| table.find_scope_var_child(var)),
        );
        let val_binding = make_binding(
            var_attribute
                .val
                .and_then(|var| table.find_scope_var_child(var)),
        );

        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
            pattern: ontol_hir::PropPattern::Attr(rel_binding, val_binding),
            nodes: body,
        });
    }

    pub fn add_seq_prop_variant_scope(
        &mut self,
        scope_var: ontol_hir::Var,
        (optional, struct_var, property_id): (ontol_hir::Optional, ontol_hir::Var, PropertyId),
        body: Vec<TypedHirNode<'m>>,
        table: &mut Table<'m>,
    ) {
        let scope_map = table
            .table_mut()
            .iter()
            .find(|scope_map| scope_map.scope.meta().var == scope_var)
            .unwrap();

        let flat_scope::Kind::SeqPropVariant(label, ..) = scope_map.scope.kind() else {
            panic!();
        };

        let label_binding = ontol_hir::Binding::Binder(TypedBinder {
            var: scope_var,
            meta: typed_hir::Meta {
                ty: label.ty,
                span: scope_map.scope.meta().hir_meta.span,
            },
        });

        let merged_match_arms = self
            .merged_match_arms_table
            .entry((struct_var, property_id))
            .or_default();

        if optional.0 {
            merged_match_arms.optional.0 = true;
        }

        merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
            pattern: ontol_hir::PropPattern::Seq(label_binding, ontol_hir::HasDefault(false)),
            nodes: body,
        });
    }
}

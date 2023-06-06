use ontol_hir::kind::{Attribute, Dimension, MatchArm, PatternBinding, PropPattern};
use tracing::debug;

use crate::{hir_unify::unified_target_node::UnifiedTargetNodes, typed_hir::TypedHirNode};

use super::{
    u_node::UBlockBody,
    unifier::{TypedMatchArm, Unifier, UnifierResult},
    unifier2::{LetAttr2, UnifyPatternBinding2},
};

impl<'s, 'm> Unifier<'s, 'm> {
    pub(super) fn unify_prop_variant_scoping(
        &mut self,
        u_block: UBlockBody<'m>,
        _scope_dimension: Dimension,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        if u_block.nodes.is_empty() {
            // treat this "transparently"
            Ok(self
                .let_attr2(u_block, scope_attr)?
                .map(|let_attr| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Attr(let_attr.rel_binding, let_attr.val_binding),
                        nodes: let_attr.target_nodes.into_hir_iter().collect(),
                    },
                    ty: let_attr.ty,
                }))
        } else {
            todo!()
        }
    }

    fn let_attr2(
        &mut self,
        mut u_block: UBlockBody<'m>,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<LetAttr2<'m>>> {
        let rel_binding =
            self.unify_pattern_binding2(Some(0), u_block.sub_scoping.remove(&0), &scope_attr.rel)?;
        let val_binding =
            self.unify_pattern_binding2(Some(1), u_block.sub_scoping.remove(&1), &scope_attr.val)?;

        Ok(match (rel_binding.target_nodes, val_binding.target_nodes) {
            (None, None) => {
                debug!("No nodes in variant binding");
                None
            }
            (Some(rel), None) => rel.type_iter().last().map(|ty| LetAttr2 {
                rel_binding: rel_binding.binding,
                val_binding: PatternBinding::Wildcard,
                target_nodes: rel,
                ty,
            }),
            (None, Some(val)) => val.type_iter().last().map(|ty| LetAttr2 {
                rel_binding: PatternBinding::Wildcard,
                val_binding: val_binding.binding,
                target_nodes: val,
                ty,
            }),
            (Some(rel), Some(val)) => {
                let mut concatenated = UnifiedTargetNodes::default();
                concatenated.0.extend(rel.0);
                concatenated.0.extend(val.0);
                concatenated.type_iter().last().map(|ty| LetAttr2 {
                    rel_binding: rel_binding.binding,
                    val_binding: val_binding.binding,
                    target_nodes: concatenated,
                    ty,
                })
            }
        })
    }

    fn unify_pattern_binding2(
        &mut self,
        debug_index: Option<usize>,
        u_block: Option<UBlockBody<'m>>,
        source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding2<'m>> {
        let u_block = match u_block {
            Some(u_block) => u_block,
            None => {
                return Ok(UnifyPatternBinding2 {
                    binding: PatternBinding::Wildcard,
                    target_nodes: None,
                })
            }
        };

        /*
        let Unified2 {
            target_nodes,
            binder,
        } = self.unify_nodes2(debug_index, nodes, source)?;
        let binding = match binder {
            Some(TypedBinder { variable, .. }) => PatternBinding::Binder(variable),
            None => PatternBinding::Wildcard,
        };
        Ok(UnifyPatternBinding2 {
            binding,
            target_nodes: Some(target_nodes),
        })
        */
        todo!("u_block: {u_block:?}")
    }
}

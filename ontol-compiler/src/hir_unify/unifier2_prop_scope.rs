use ontol_hir::kind::{
    Attribute, Dimension, MatchArm, NodeKind, Optional, PatternBinding, PropPattern, PropVariant,
};
use ontol_runtime::value::PropertyId;
use tracing::debug;

use crate::{
    hir_unify::unifier2::UnifiedBlock,
    typed_hir::{Meta, TypedBinder, TypedHir, TypedHirNode},
    types::Type,
};

use super::{
    u_node::{UBlockBody, UNode, UNodeKind},
    unifier::{TypedMatchArm, Unifier, UnifierResult},
    unifier2::{Block, LetAttr2, ScopeSource, UnifiedNode, UnifyPatternBinding2},
};

impl<'s, 'm> Unifier<'s, 'm> {
    pub(super) fn unify_prop_scope(
        &mut self,
        optional: Optional,
        struct_var: ontol_hir::Variable,
        property_id: PropertyId,
        variants: &'s [PropVariant<'m, TypedHir>],
        scope_meta: Meta<'m>,
        u_node: UNode<'m>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        let mut match_arms = vec![];
        let mut ty = &Type::Tautology;

        match u_node.kind {
            UNodeKind::Block(_, u_block) => {
                for (subscope_idx, sub_block) in u_block.sub_scoping {
                    let PropVariant { dimension, attr } = &variants[subscope_idx];

                    if let Some(typed_match_arm) =
                        self.unify_prop_variant_scoping(sub_block, *dimension, attr)?
                    {
                        // if let Type::Tautology = typed_match_arm.ty {
                        //     panic!("found Tautology");
                        // }

                        ty = typed_match_arm.ty;
                        match_arms.push(typed_match_arm.arm);
                    }
                }
            }
            _ => unimplemented!(),
        }

        Ok(UnifiedNode {
            binder: None,
            node: if match_arms.is_empty() {
                panic!("No match arms");
                TypedHirNode {
                    kind: NodeKind::Unit,
                    meta: scope_meta,
                }
            } else {
                if let Type::Tautology = ty {
                    panic!("Tautology");
                }

                TypedHirNode {
                    kind: NodeKind::MatchProp(struct_var, property_id, match_arms),
                    meta: Meta {
                        ty,
                        span: scope_meta.span,
                    },
                }
            },
        })
    }

    pub(super) fn unify_prop_variant_scoping(
        &mut self,
        u_block: UBlockBody<'m>,
        _scope_dimension: Dimension,
        scope_attr: &'s Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        if u_block.nodes.is_empty() {
            // treat this "transparently"
            Ok(self
                .let_attr2(u_block, scope_attr)?
                .map(|let_attr| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Attr(let_attr.rel_binding, let_attr.val_binding),
                        nodes: let_attr.target_nodes.0,
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
        scope_attr: &'s Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<LetAttr2<'m>>> {
        let rel_binding =
            self.unify_pattern_binding2(Some(0), u_block.sub_scoping.remove(&0), &scope_attr.rel)?;
        let val_binding =
            self.unify_pattern_binding2(Some(1), u_block.sub_scoping.remove(&1), &scope_attr.val)?;

        Ok(match (rel_binding.block, val_binding.block) {
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
                let mut concatenated = Block::default();
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
        scope: &'s TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding2<'m>> {
        let mut u_block = match u_block {
            Some(u_block) => u_block,
            None => {
                return Ok(UnifyPatternBinding2 {
                    binding: PatternBinding::Wildcard,
                    block: None,
                })
            }
        };

        debug!("pattern binding scope: {scope}");

        let UnifiedBlock { binder, block } =
            self.unify_u_block(&mut u_block, ScopeSource::Node(scope))?;

        let binding = match binder {
            Some(TypedBinder { variable, .. }) => PatternBinding::Binder(variable),
            None => PatternBinding::Wildcard,
        };
        Ok(UnifyPatternBinding2 {
            binding,
            block: Some(block),
        })
    }
}

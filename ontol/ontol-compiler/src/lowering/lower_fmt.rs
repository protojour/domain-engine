use ontol_parser::{
    cst::{
        inspect::{self as insp},
        view::NodeView,
    },
    U32Span,
};
use ontol_runtime::{
    ontology::domain::EdgeCardinalProjection,
    property::{PropertyCardinality, ValueCardinality},
    tuple::CardinalIdx,
    DefId, EdgeId,
};
use tracing::debug;

use crate::{
    def::{DefKind, FmtFinalState, RelParams, Relationship, TypeDef, TypeDefFlags},
    lowering::context::RelationKey,
    CompileError,
};

use super::context::{BlockContext, CstLowering, RootDefs};

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn lower_fmt_statement(
        &mut self,
        stmt: insp::FmtStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let mut root_defs = Vec::new();
        let mut transitions = stmt.transitions().peekable();

        let Some(origin) = transitions.next() else {
            CompileError::TODO("missing origin").span_report(stmt.0.span(), &mut self.ctx);
            return None;
        };
        let mut origin_def_id = self.resolve_type_reference(
            origin.type_ref()?,
            &BlockContext::NoContext,
            Some(&mut root_defs),
        )?;

        let Some(mut transition) = transitions.next() else {
            CompileError::FmtTooFewTransitions.span_report(stmt.0.span(), &mut self.ctx);
            return None;
        };

        let target = loop {
            let next_transition = match transitions.next() {
                Some(item) if transitions.peek().is_none() => {
                    // end of iterator, found the final target. Handle this outside the loop:
                    break item;
                }
                Some(item) => item,
                _ => {
                    CompileError::FmtTooFewTransitions.span_report(stmt.0.span(), &mut self.ctx);
                    return None;
                }
            };

            let target_def_id = self.ctx.define_anonymous_type(
                TypeDef {
                    ident: None,
                    rel_type_for: None,
                    flags: TypeDefFlags::CONCRETE,
                },
                next_transition.view().span(),
            );

            root_defs.push(self.lower_fmt_transition(
                (origin_def_id, transition.view().span()),
                transition,
                (target_def_id, next_transition.view().span()),
                FmtFinalState(false),
            )?);

            transition = next_transition;
            origin_def_id = target_def_id;
        };

        let final_def =
            self.resolve_type_reference(target.type_ref()?, &block, Some(&mut root_defs))?;

        root_defs.push(self.lower_fmt_transition(
            (origin_def_id, origin.view().span()),
            transition,
            (final_def, target.view().span()),
            FmtFinalState(true),
        )?);

        Some(root_defs)
    }

    fn lower_fmt_transition(
        &mut self,
        from: (DefId, U32Span),
        transition: insp::TypeMod<V>,
        to: (DefId, U32Span),
        final_state: FmtFinalState,
    ) -> Option<DefId> {
        let transition_def =
            self.resolve_type_reference(transition.type_ref()?, &BlockContext::FmtLeading, None)?;
        let relation_key = RelationKey::FmtTransition(transition_def, final_state);

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(relation_key, from.1);

        debug!("{:?}: <transition>", relation_def_id.0);

        Some(self.ctx.define_anonymous(
            DefKind::Relationship(Relationship {
                relation_def_id,
                projection: EdgeCardinalProjection {
                    id: EdgeId(relation_def_id),
                    subject: CardinalIdx(0),
                    object: CardinalIdx(1),
                    one_to_one: false,
                },
                relation_span: self.ctx.source_span(from.1),
                subject: (from.0, self.ctx.source_span(from.1)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                object: (to.0, self.ctx.source_span(to.1)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                rel_params: RelParams::Unit,
            }),
            to.1,
        ))
    }
}

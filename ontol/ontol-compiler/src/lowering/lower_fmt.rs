use ontol_parser::cst::{
    inspect::{self as insp},
    view::NodeView,
};
use ontol_runtime::property::ValueCardinality;
use tracing::{debug, debug_span};

use crate::{
    fmt::{FmtChain, FmtTransition},
    lowering::lower_misc::ReportError,
    CompileError,
};

use super::context::{BlockContext, CstLowering, RootDefs};

impl<V: NodeView> CstLowering<'_, '_, V> {
    pub(super) fn lower_fmt_statement(
        &mut self,
        stmt: insp::FmtStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let _entered = debug_span!("fmt").entered();

        let mut transition_iter = stmt.transitions().peekable();

        let Some(origin) = transition_iter.next() else {
            CompileError::TODO("missing origin").span_report(stmt.0.span(), &mut self.ctx);
            return None;
        };
        let origin_def_id = self
            .resolve_type_reference(
                origin.type_ref()?,
                ValueCardinality::Unit,
                &BlockContext::NoContext,
                ReportError::Yes,
                None,
            )?
            .def_id;

        let mut fmt_chain = FmtChain {
            span: self.ctx.source_span(stmt.view().span()),
            origin: FmtTransition {
                def: origin_def_id,
                span: self.ctx.source_span(origin.view().span()),
            },
            transitions: vec![],
        };

        let Some(mut transition) = transition_iter.next() else {
            CompileError::FmtTooFewTransitions.span_report(stmt.0.span(), &mut self.ctx);
            return None;
        };

        let final_transition = loop {
            let transition_def = self
                .resolve_type_reference(
                    transition.type_ref()?,
                    ValueCardinality::Unit,
                    &BlockContext::FmtLeading,
                    ReportError::Yes,
                    None,
                )?
                .def_id;

            debug!("transition def: {transition_def:?}");

            fmt_chain.transitions.push(FmtTransition {
                def: transition_def,
                span: self.ctx.source_span(transition.view().span()),
            });

            match transition_iter.next() {
                Some(item) if transition_iter.peek().is_none() => {
                    // end of iterator, found the final target. Handle this outside the loop:
                    break item;
                }
                Some(item) => {
                    transition = item;
                }
                _ => {
                    CompileError::FmtTooFewTransitions.span_report(stmt.0.span(), &mut self.ctx);
                    return None;
                }
            };
        };

        let final_def = self
            .resolve_type_reference(
                final_transition.type_ref()?,
                ValueCardinality::Unit,
                &block,
                ReportError::Yes,
                None,
            )?
            .def_id;

        self.ctx.outcome.fmt_chains.push((final_def, fmt_chain));

        None
    }
}

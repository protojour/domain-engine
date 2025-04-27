use ontol_core::span::U32Span;
use ontol_log::with_span::WithSpan;

use crate::{
    lowering::context::MapVarTable,
    pattern::{Pattern, PatternKind},
};

use super::LogLowering;

impl LogLowering<'_, '_, '_> {
    pub(super) fn lower_pattern(
        &mut self,
        pattern: &WithSpan<ontol_log::log_model::Pattern>,
        _var_table: &mut MapVarTable,
    ) -> Pattern {
        match &pattern.0 {
            ontol_log::log_model::Pattern::False => {
                self.mk_pattern(PatternKind::ConstBool(false), pattern.span())
            }
            ontol_log::log_model::Pattern::True => {
                self.mk_pattern(PatternKind::ConstBool(true), pattern.span())
            }
            ontol_log::log_model::Pattern::I64(int) => {
                self.mk_pattern(PatternKind::ConstInt(*int), pattern.span())
            }
            ontol_log::log_model::Pattern::Text(text) => {
                self.mk_pattern(PatternKind::ConstText(text.to_string()), pattern.span())
            }
            ontol_log::log_model::Pattern::Struct(_struct_pattern) => todo!(),
            ontol_log::log_model::Pattern::Set(_) => todo!(),
            ontol_log::log_model::Pattern::Atom(_) => todo!(),
            ontol_log::log_model::Pattern::Binary(_) => todo!(),
        }
    }

    fn mk_pattern(&mut self, kind: PatternKind, span: U32Span) -> Pattern {
        Pattern {
            id: self.compiler.patterns.alloc_pat_id(),
            kind,
            span: self.source_id.span(span),
        }
    }
}

use ontol_parser::cst::{
    inspect::{self as insp},
    view::{NodeView, TokenView},
};
use ontol_runtime::DefId;

use crate::{CompileError, def::DefKind, namespace::Space, pattern::PatId};

use super::context::{BlockContext, Coinage, CstLowering, MapVarTable};

impl<V: NodeView> CstLowering<'_, '_, V> {
    pub(super) fn lower_map_statement(
        &mut self,
        stmt: insp::MapStatement<V>,
        block_context: BlockContext,
    ) -> Option<DefId> {
        let mut var_table = MapVarTable::default();

        let mut arms = stmt.arms();
        let first = self.lower_map_arm(arms.next()?, &mut var_table)?;
        let second = self.lower_map_arm(arms.next()?, &mut var_table)?;

        let mut is_abstract = false;

        for modifier in stmt.modifiers() {
            let token = modifier.token()?;
            if token.slice() == "@abstract" {
                if matches!(block_context, BlockContext::SubDef(_)) {
                    CompileError::TODO("extern map cannot be abstract")
                        .span_report(token.span(), &mut self.ctx);
                }

                is_abstract = true;
            } else {
                CompileError::InvalidModifier.span_report(token.span(), &mut self.ctx);
            }
        }

        let (def_id, ident) = match stmt.ident_path() {
            Some(ident_path) => {
                let symbol = ident_path.symbols().next()?;
                let (def_id, coinage, _) = self.catch(|zelf| {
                    zelf.ctx.named_def_id(
                        zelf.ctx.domain_def_id,
                        Space::Map,
                        symbol.slice(),
                        symbol.span(),
                    )
                })?;
                if matches!(coinage, Coinage::Used) {
                    CompileError::DuplicateMapIdentifier.span_report(symbol.span(), &mut self.ctx);
                    return None;
                }
                let ident = self.ctx.compiler.str_ctx.intern(symbol.slice());
                (def_id, Some(ident))
            }
            None => (
                self.ctx.compiler.defs.alloc_def_id(self.ctx.domain_index),
                None,
            ),
        };

        self.ctx.set_def_kind(
            def_id,
            DefKind::Mapping {
                ident,
                arms: [first, second],
                var_alloc: var_table.into_allocator(),
                extern_def_id: if let Some(def_func) = block_context.def_func() {
                    let context_def_id = (*def_func)();
                    if matches!(
                        self.ctx.compiler.defs.def_kind(context_def_id),
                        DefKind::Extern(_)
                    ) {
                        Some(context_def_id)
                    } else {
                        None
                    }
                } else {
                    None
                },
                is_abstract,
            },
            stmt.0.span(),
        );

        Some(def_id)
    }

    fn lower_map_arm(
        &mut self,
        arm: insp::MapArm<V>,
        var_table: &mut MapVarTable,
    ) -> Option<PatId> {
        let pattern = match arm.pattern() {
            Some(p) => self.lower_pattern(p, var_table),
            None => self.mk_error_pattern(arm.0.span()),
        };

        let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
        self.ctx.compiler.patterns.table.insert(pat_id, pattern);

        Some(pat_id)
    }
}

use ontol_core::{
    property::{PropertyCardinality, ValueCardinality},
    span::U32Span,
};
use ontol_log::tag::Tag;
use ontol_runtime::{DefId, OntolDefTag, OntolDefTagExt};

use crate::{
    def::{Def, DefKind, TypeDef, TypeDefFlags},
    relation::{RelParams, Relationship},
};

use super::LogLowering;

impl LogLowering<'_, '_, '_> {
    /// Supplying the tag makes the symbol DefId persistent.
    /// sym statements are persistent, while `arc` symbols are non-persistent.
    pub(super) fn coin_symbol(
        &mut self,
        tag: Option<Tag>,
        ident: &str,
        ident_span: U32Span,
    ) -> DefId {
        let symbol_def_id = if let Some(tag) = tag {
            self.mk_persistent_def_id(self.domain_index, tag)
        } else {
            self.compiler.defs.alloc_transient_def_id(self.domain_index)
        };

        let ident = self.compiler.str_ctx.intern(ident);

        let kind = DefKind::Type(TypeDef {
            ident: Some(ident),
            rel_type_for: None,
            flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
        });

        let span = self.source_id.span(ident_span);

        self.outcome.root_defs.push(symbol_def_id);
        self.compiler.defs.table.insert(
            symbol_def_id,
            Def {
                id: symbol_def_id,
                domain_index: self.domain_index,
                kind,
                span,
            },
        );

        let ident_literal = self
            .compiler
            .defs
            .def_text_literal(ident, &mut self.compiler.str_ctx);

        let rel_id = self.compiler.rel_ctx.alloc_rel_id(symbol_def_id);
        self.outcome.predefine_rel(
            rel_id,
            Relationship {
                relation_def_id: OntolDefTag::RelationIs.def_id(),
                edge_projection: None,
                relation_span: span,
                subject: (symbol_def_id, span),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                object: (ident_literal, span),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                rel_params: RelParams::Unit,
                macro_source: None,
                modifiers: vec![],
            },
            span,
            None,
        );

        symbol_def_id
    }
}

use indexmap::map::Entry;
use ontol_core::span::U32Span;
use ontol_log::tag::Tag;
use ontol_runtime::DefId;
use tracing::info;

use crate::{
    CompileError,
    def::{Def, DefKind, TypeDef, TypeDefFlags},
    namespace::Space,
};

use super::LogLowering;

impl<'m> LogLowering<'_, 'm, '_> {
    pub(super) fn lower_def(&mut self, tag: Tag, def: &ontol_log::sem_stmts::Def) {
        info!("lower def {tag:?} {def:?}");

        let Some(ident) = &def.ident else {
            return;
        };

        if def.symbol {
            let def_id = self.coin_symbol(Some(tag), ident.0.text(), ident.span());
            self.outcome.root_defs.push(def_id);
            return;
        }

        let span = ident.span();
        let interned_ident = self.compiler.str_ctx.intern(ident.0.text());

        let def_id = self.mk_persistent_def_id(self.domain_index, tag);

        match self
            .compiler
            .namespaces
            .get_namespace_mut(self.subdomain_def_id, Space::Def)
            .entry(interned_ident)
        {
            Entry::Occupied(_) => {
                self.compiler.errors.push(
                    CompileError::TODO("redefinition of identifier")
                        .span(self.source_id.span(span)),
                );
            }
            Entry::Vacant(vacant) => {
                vacant.insert(def_id);
            }
        }

        let kind = if def
            .modifiers
            .contains(&ontol_log::log_model::DefModifier::Macro)
        {
            DefKind::Macro(interned_ident)
        } else if def
            .modifiers
            .contains(&ontol_log::log_model::DefModifier::Extern)
        {
            DefKind::Extern(interned_ident)
        } else {
            let mut flags = TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC;

            if def
                .modifiers
                .contains(&ontol_log::log_model::DefModifier::Private)
            {
                flags.remove(TypeDefFlags::PUBLIC);
            }

            if def
                .modifiers
                .contains(&ontol_log::log_model::DefModifier::Open)
            {
                flags.insert(TypeDefFlags::OPEN);
            }

            DefKind::Type(TypeDef {
                ident: Some(interned_ident),
                rel_type_for: None,
                flags,
            })
        };

        self.compiler.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index: self.domain_index,
                kind,
                span: self.source_id.span(span),
            },
        );

        self.outcome.root_defs.push(def_id);
    }

    pub fn define_anonymous(&mut self, kind: DefKind<'m>, span: U32Span) -> DefId {
        let def_id = self.compiler.defs.alloc_transient_def_id(self.domain_index);
        self.compiler.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index: self.domain_index,
                kind,
                span: self.source_id.span(span),
            },
        );
        def_id
    }
}

use ontol_parser::{
    cst::{
        inspect::{self as insp},
        view::{NodeView, TokenView},
    },
    U32Span,
};
use ontol_runtime::{
    property::{PropertyCardinality, ValueCardinality},
    DefId,
};
use tracing::{debug, debug_span};

use crate::{
    def::{DefKind, FmtFinalState, RelParams, Relationship, TypeDef, TypeDefFlags},
    lowering::context::RelationKey,
    namespace::Space,
    package::PackageReference,
    CompileError,
};

use super::context::{BlockContext, CstLowering, RootDefs};

pub(super) enum PreDefinedStmt<V> {
    Def(DefId, insp::DefStatement<V>),
    Rel(insp::RelStatement<V>),
    Fmt(insp::FmtStatement<V>),
    Map(insp::MapStatement<V>),
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn pre_define_statement(
        &mut self,
        statement: insp::Statement<V>,
    ) -> Option<PreDefinedStmt<V>> {
        match statement {
            insp::Statement::UseStatement(use_stmt) => {
                let name = use_stmt.name()?;
                let name_text = name.text().and_then(|result| self.unescape(result))?;

                let reference = PackageReference::Named(name_text);
                let Some(used_package_def_id) =
                    self.ctx.compiler.packages.loaded_packages.get(&reference)
                else {
                    CompileError::PackageNotFound(reference)
                        .span_report(name.0.span(), &mut self.ctx);
                    return None;
                };

                let type_namespace = self
                    .ctx
                    .compiler
                    .namespaces
                    .get_namespace_mut(self.ctx.package_id, Space::Type);

                let symbol = use_stmt.ident_path()?.symbols().next()?;

                let as_ident = self.ctx.compiler.strings.intern(symbol.slice());
                type_namespace.insert(as_ident, *used_package_def_id);

                None
            }
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;

                Some(PreDefinedStmt::Def(def_id, def_stmt))
            }
            insp::Statement::RelStatement(rel_stmt) => Some(PreDefinedStmt::Rel(rel_stmt)),
            insp::Statement::FmtStatement(fmt_stmt) => Some(PreDefinedStmt::Fmt(fmt_stmt)),
            insp::Statement::MapStatement(map_stmt) => Some(PreDefinedStmt::Map(map_stmt)),
        }
    }

    pub(super) fn lower_pre_defined(
        &mut self,
        stmt: PreDefinedStmt<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match stmt {
            PreDefinedStmt::Def(def_id, def_stmt) => self.lower_def_body(def_id, def_stmt),
            PreDefinedStmt::Rel(rel_stmt) => self.lower_statement(rel_stmt.into(), block_context),
            PreDefinedStmt::Fmt(fmt_stmt) => self.lower_statement(fmt_stmt.into(), block_context),
            PreDefinedStmt::Map(map_stmt) => self.lower_statement(map_stmt.into(), block_context),
        }
    }

    pub(super) fn lower_statement(
        &mut self,
        statement: insp::Statement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match statement {
            insp::Statement::UseStatement(_use_stmt) => None,
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.lower_def_body(def_id, def_stmt)?);
                Some(root_defs)
            }
            insp::Statement::RelStatement(rel_stmt) => {
                self.lower_rel_statement(rel_stmt, block_context)
            }
            insp::Statement::FmtStatement(fmt_stmt) => {
                self.lower_fmt_statement(fmt_stmt, block_context)
            }
            insp::Statement::MapStatement(map_stmt) => {
                let def_id = self.lower_map_statement(map_stmt, block_context)?;
                Some(vec![def_id])
            }
        }
    }

    pub fn lower_def_body(
        &mut self,
        def_id: DefId,
        stmt: insp::DefStatement<V>,
    ) -> Option<RootDefs> {
        self.append_documentation(def_id, stmt.0.clone());

        let mut root_defs: RootDefs = [def_id].into();

        if let Some(body) = stmt.body() {
            let _entered = debug_span!("def", id = ?def_id).entered();

            // The inherent relation block on the type uses the just defined
            // type as its context
            let context_fn = move || def_id;

            for statement in body.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }
        }

        Some(root_defs)
    }

    fn lower_fmt_statement(
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
                relation_span: self.ctx.source_span(from.1),
                subject: (from.0, self.ctx.source_span(from.1)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                object: (to.0, self.ctx.source_span(to.1)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                object_prop: None,
                rel_params: RelParams::Unit,
            }),
            to.1,
        ))
    }
}

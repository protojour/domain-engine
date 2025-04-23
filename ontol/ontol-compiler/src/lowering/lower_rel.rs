use std::ops::Range;

use ontol_core::span::U32Span;
use ontol_parser::{
    basic_syntax::extract_documentation,
    cst::{
        inspect as insp,
        view::{NodeView, NodeViewExt, TypedView},
    },
    source::SourceSpan,
};
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt,
    ontology::domain::EdgeCardinalProjection,
    property::{PropertyCardinality, ValueCardinality},
};
use tracing::debug_span;

use crate::{
    CompileError,
    def::{DefKind, RelationContext, TypeDef, TypeDefFlags},
    edge::EdgeId,
    relation::{DefRelTag, RelId, RelParams, Relationship},
};

use super::{
    context::{BlockContext, CstLowering, MapVarTable, RelationKey, RootDefs},
    lower_misc::{ReportError, ResolvedType},
};

impl<V: NodeView> CstLowering<'_, '_, V> {
    pub(super) fn lower_rel_statement(
        &mut self,
        stmt: insp::RelStatement<V>,
        mut block: BlockContext,
    ) -> Option<RootDefs> {
        let rel_subject = stmt.subject()?;
        let relation = stmt.relation()?;
        let rel_object = stmt.object()?;

        let mut root_defs = RootDefs::default();

        let subject_ty = self.resolve_quant_type_reference(
            rel_subject.type_quant()?,
            &block,
            Some(&mut root_defs),
            ReportError::Yes,
        )?;
        let object_ty = match rel_object.type_quant_or_pattern()? {
            insp::TypeQuantOrPattern::TypeQuant(type_quant) => self.resolve_quant_type_reference(
                type_quant,
                &block,
                Some(&mut root_defs),
                ReportError::Yes,
            )?,
            insp::TypeQuantOrPattern::Pattern(pattern) => {
                let span = pattern.view().span();
                let mut var_table = MapVarTable::default();
                let lowered = self.lower_pattern(pattern.clone(), &mut var_table);
                let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
                self.ctx.compiler.patterns.table.insert(pat_id, lowered);

                ResolvedType {
                    def_id: self
                        .ctx
                        .define_anonymous(DefKind::Constant(pat_id), pattern.view().span()),
                    cardinality: ValueCardinality::Unit,
                    span,
                }
            }
        };

        if let Some(mut defs) =
            self.lower_relationship(subject_ty, relation, object_ty, stmt.clone(), &mut block)
        {
            root_defs.append(&mut defs);
        }

        if subject_ty.cardinality == ValueCardinality::Unit
            && object_ty.cardinality == ValueCardinality::Unit
        {
            let identifies_relationship = Relationship {
                relation_def_id: OntolDefTag::RelationIdentifies.def_id(),
                edge_projection: None,
                relation_span: self.ctx.source_span(stmt.view().non_trivia_span()),
                subject: (object_ty.def_id, self.ctx.source_span(object_ty.span)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                object: (subject_ty.def_id, self.ctx.source_span(subject_ty.span)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                rel_params: RelParams::Unit,
                macro_source: None,
                modifiers: vec![],
            };

            let rel_id = self.ctx.compiler.rel_ctx.alloc_rel_id(subject_ty.def_id);
            self.ctx.outcome.predefine_rel(
                rel_id,
                identifies_relationship,
                self.ctx.source_span(stmt.view().non_trivia_span()),
                None,
            );
        }

        Some(root_defs)
    }

    fn lower_relationship(
        &mut self,
        subject_ty: ResolvedType,
        relation: insp::Relation<V>,
        object_ty: ResolvedType,
        rel_stmt: insp::RelStatement<V>,
        block: &mut BlockContext,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        let (key, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) = {
            let type_quant = relation.relation_type()?;
            match type_quant.type_ref()? {
                insp::TypeRef::NumberRange(range) => (
                    RelationKey::Indexed,
                    range.0.span(),
                    Some(self.lower_u16_range(range)),
                ),
                _ => {
                    let def_id = self
                        .resolve_quant_type_reference(
                            type_quant.clone(),
                            &BlockContext::NoContext,
                            Some(&mut root_defs),
                            ReportError::Yes,
                        )?
                        .def_id;
                    let span = type_quant.view().span();

                    match self.ctx.compiler.defs.def_kind(def_id) {
                        DefKind::TextLiteral(_) => (RelationKey::Named(def_id), span, None),
                        DefKind::Type(_) => {
                            if let Some(edge_id) =
                                self.ctx.compiler.edge_ctx.edge_id_by_symbol(def_id)
                            {
                                return self.lower_edge_relationship(
                                    subject_ty,
                                    (def_id, span, edge_id),
                                    relation,
                                    object_ty,
                                    rel_stmt,
                                );
                            } else {
                                (RelationKey::Named(def_id), span, None)
                            }
                        }
                        DefKind::BuiltinRelType(..) => (RelationKey::Builtin(def_id), span, None),
                        DefKind::NumberLiteral(lit) => match lit.parse::<u16>() {
                            Ok(int) => (RelationKey::Indexed, span, Some(Some(int)..Some(int + 1))),
                            Err(_) => {
                                CompileError::NumberParse("not an integer".to_string())
                                    .span_report(span, &mut self.ctx);
                                return None;
                            }
                        },
                        _ => {
                            CompileError::InvalidRelationType.span_report(span, &mut self.ctx);
                            return None;
                        }
                    }
                }
            }
        };

        self.lower_literal_relationship(
            subject_ty,
            (key, ident_span, index_range_rel_params),
            relation,
            object_ty,
            rel_stmt,
            block,
        )
    }

    fn lower_literal_relationship(
        &mut self,
        subject_ty: ResolvedType,
        (key, ident_span, index_range_rel_params): (
            RelationKey,
            U32Span,
            Option<Range<Option<u16>>>,
        ),
        relation: insp::Relation<V>,
        object_ty: ResolvedType,
        rel_stmt: insp::RelStatement<V>,
        block: &mut BlockContext,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(key);

        let rel_id = if subject_ty.def_id == DefId::unit() {
            // a parent relation modifier
            RelId(DefId::unit(), DefRelTag(0))
        } else {
            self.ctx.compiler.rel_ctx.alloc_rel_id(subject_ty.def_id)
        };

        let docs = extract_documentation(rel_stmt.0.clone());

        let mut relation_modifiers: Vec<(Relationship, SourceSpan)> = Default::default();

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some(rp) = relation.rel_params() {
                CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes
                    .span_report(rp.0.span(), &mut self.ctx);
                return None;
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some(rp) = relation.rel_params() {
            let _entered = debug_span!("rel_params").entered();
            let scan = self.scan_rel_params(rp.clone());

            if scan.has_def_receiver {
                let rel_def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: Some(rel_id),
                        flags: TypeDefFlags::CONCRETE,
                    },
                    rp.0.span(),
                );
                let context_fn = || rel_def_id;

                root_defs.push(rel_def_id);

                // This type needs to be part of the anonymous part of the namespace
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.domain_def_id, rel_def_id);

                for statement in rp.statements() {
                    if let Some(mut defs) = self.lower_statement(
                        statement,
                        BlockContext::RelParams {
                            def_fn: &context_fn,
                            relation_modifiers: &mut relation_modifiers,
                        },
                    ) {
                        root_defs.append(&mut defs);
                    }
                }

                RelParams::Def(rel_def_id)
            } else {
                for statement in rp.statements() {
                    if let Some(mut defs) = self.lower_statement(
                        statement,
                        BlockContext::RelParams {
                            def_fn: &DefId::unit,
                            relation_modifiers: &mut relation_modifiers,
                        },
                    ) {
                        root_defs.append(&mut defs);
                    }
                }

                RelParams::Unit
            }
        } else {
            RelParams::Unit
        };

        let relationship = {
            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                object_ty.cardinality,
            );
            let object_cardinality = (PropertyCardinality::Optional, subject_ty.cardinality);

            Relationship {
                relation_def_id,
                edge_projection: None,
                relation_span: self.ctx.source_span(ident_span),
                subject: (subject_ty.def_id, self.ctx.source_span(subject_ty.span)),
                subject_cardinality,
                object: (object_ty.def_id, self.ctx.source_span(object_ty.span)),
                object_cardinality,
                rel_params: rel_params.clone(),
                macro_source: None,
                modifiers: relation_modifiers,
            }
        };

        let rel_context = match self.ctx.compiler.defs.def_kind(relation_def_id) {
            DefKind::BuiltinRelType(kind, _) => kind.context(),
            _ => RelationContext::Def,
        };

        match rel_context {
            RelationContext::Def => {
                self.ctx.outcome.predefine_rel(
                    rel_id,
                    relationship,
                    self.ctx.source_span(rel_stmt.0.non_trivia_span()),
                    docs.map(Into::into),
                );
            }
            RelationContext::Rel => match block {
                BlockContext::RelParams {
                    relation_modifiers, ..
                } => {
                    relation_modifiers
                        .push((relationship, self.ctx.source_span(rel_stmt.0.span())));
                }
                _ => {
                    todo!("report error");
                }
            },
        }

        Some(root_defs)
    }

    fn lower_edge_relationship(
        &mut self,
        subject_ty: ResolvedType,
        (sym_id, ident_span, edge_id): (DefId, U32Span, EdgeId),
        relation: insp::Relation<V>,
        object_ty: ResolvedType,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let edge = self
            .ctx
            .compiler
            .edge_ctx
            .symbolic_edges
            .get_mut(&edge_id)
            .unwrap();

        let docs = extract_documentation(rel_stmt.0.clone());

        let relationship = {
            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                object_ty.cardinality,
            );
            let object_cardinality = (PropertyCardinality::Optional, subject_ty.cardinality);

            let unique = matches!(subject_cardinality.1, ValueCardinality::Unit);
            let slot = *edge.symbols.get(&sym_id).unwrap();
            let cardinality = edge.cardinals.len();

            let projection = EdgeCardinalProjection {
                edge_id: edge_id.0,
                subject: slot.left,
                object: slot.right,
                pinned: unique,
            };

            if unique {
                let edge_variable = edge.cardinals.get_mut(&slot.left).unwrap();
                edge_variable.unique_count += 1;

                // The current heuristic for "edge cardinal def pinning":
                // Using a clause that is less wide than the overall cardinality.
                let clause_width = edge.clause_widths.get(&slot.clause_idx).unwrap();

                if *clause_width < cardinality {
                    edge_variable.pinned_count += 1;
                }
            }

            Relationship {
                relation_def_id: sym_id,
                edge_projection: Some(projection),
                relation_span: self.ctx.source_span(ident_span),
                subject: (subject_ty.def_id, self.ctx.source_span(subject_ty.span)),
                subject_cardinality,
                object: (object_ty.def_id, self.ctx.source_span(object_ty.span)),
                object_cardinality,
                rel_params: RelParams::Unit,
                macro_source: None,
                modifiers: vec![],
            }
        };

        let rel_id = self.ctx.compiler.rel_ctx.alloc_rel_id(subject_ty.def_id);
        self.ctx.outcome.predefine_rel(
            rel_id,
            relationship,
            self.ctx.source_span(rel_stmt.0.span()),
            docs.map(Into::into),
        );

        Some(vec![])
    }

    fn scan_rel_params(&mut self, rp: insp::RelParams<V>) -> RelParamsScan {
        let mut has_def_receiver = false;
        let mut has_rel_receiver = false;

        for statement in rp.statements() {
            if let insp::Statement::RelStatement(rel_statement) = statement {
                if let Some(relation) = rel_statement.relation() {
                    if let Some(type_quant) = relation.relation_type() {
                        if let Some(resolved_type) = self.resolve_quant_type_reference(
                            type_quant,
                            &BlockContext::NoContext,
                            Some(&mut vec![]),
                            ReportError::No,
                        ) {
                            if let DefKind::BuiltinRelType(kind, _) =
                                self.ctx.compiler.defs.def_kind(resolved_type.def_id)
                            {
                                match kind.context() {
                                    RelationContext::Def => {
                                        has_def_receiver = true;
                                    }
                                    RelationContext::Rel => {
                                        has_rel_receiver = true;
                                    }
                                }
                            } else {
                                has_def_receiver = true;
                            }
                        }
                    }
                }
            }
        }

        RelParamsScan {
            has_def_receiver,
            has_rel_receiver,
        }
    }
}

struct RelParamsScan {
    has_def_receiver: bool,
    #[expect(unused)]
    has_rel_receiver: bool,
}

fn property_cardinality<V: NodeView>(
    prop_cardinality: Option<insp::PropCardinality<V>>,
) -> Option<PropertyCardinality> {
    Some(if prop_cardinality?.question().is_some() {
        PropertyCardinality::Optional
    } else {
        PropertyCardinality::Mandatory
    })
}

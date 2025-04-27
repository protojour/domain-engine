use ontol_core::property::{PropertyCardinality, ValueCardinality};
use ontol_log::{
    log_model::{ArcCoord, PathRef, TypeRef},
    tag::Tag,
    with_span::WithSpan,
};
use ontol_parser::source::SourceSpan;
use ontol_runtime::{DefId, OntolDefTag, OntolDefTagExt, ontology::domain::EdgeCardinalProjection};
use tracing::info;

use crate::{
    CompileError,
    edge::EdgeId,
    relation::{DefRelTag, RelId, RelParams, Relationship},
};

use super::{LogLowering, TypeContext, map_sem_value_crd};

impl LogLowering<'_, '_, '_> {
    pub(super) fn lower_rel(
        &mut self,
        tag: Tag,
        rel: &ontol_log::sem_stmts::Rel,
        type_ctx: &mut TypeContext,
    ) -> Option<()> {
        match rel.rel_type.as_ref()? {
            WithSpan(TypeRef::Path(PathRef::LocalArc(arc_tag, coord)), rel_span) => {
                let edge_id = EdgeId(self.mk_persistent_def_id(self.domain_index, *arc_tag));
                self.lower_edge_rel(edge_id, *coord, tag, rel, self.source_id.span(*rel_span))
            }
            WithSpan(TypeRef::Path(PathRef::ForeignArc(use_tag, arc_tag, coord)), rel_span) => {
                let domain_index = self.resolve_foreign_domain_index(*use_tag)?;
                let edge_id = EdgeId(self.mk_persistent_def_id(domain_index, *arc_tag));
                self.lower_edge_rel(edge_id, *coord, tag, rel, self.source_id.span(*rel_span))
            }
            WithSpan(TypeRef::Number(lit), rel_span) => match lit.parse::<u16>() {
                Ok(num) => self.lower_literal_rel(
                    tag,
                    rel,
                    OntolDefTag::RelationIndexed.def_id(),
                    self.source_id.span(*rel_span),
                    Some(RelParams::IndexRange(Some(num)..Some(num + 1))),
                    type_ctx,
                ),
                Err(_) => {
                    self.compiler.errors.push(
                        CompileError::NumberParse("not an integer".to_string())
                            .span(self.source_id.span(*rel_span)),
                    );
                    None
                }
            },
            WithSpan(TypeRef::NumberRange(start, end), rel_span) => {
                match (
                    start.as_ref().map(|lit| lit.parse::<u16>()).transpose(),
                    end.as_ref().map(|lit| lit.parse::<u16>()).transpose(),
                ) {
                    (Ok(start), Ok(end)) => self.lower_literal_rel(
                        tag,
                        rel,
                        OntolDefTag::RelationIndexed.def_id(),
                        self.source_id.span(*rel_span),
                        Some(RelParams::IndexRange(start..end)),
                        type_ctx,
                    ),
                    (start, end) => {
                        for result in [start, end] {
                            if let Err(_err) = result {
                                self.compiler.errors.push(
                                    CompileError::NumberParse("not an integer".to_string())
                                        .span(self.source_id.span(*rel_span)),
                                );
                            }
                        }

                        None
                    }
                }
            }
            other => {
                let (relation_def_id, relation_span) =
                    self.resolve_unit_type_ref(other, type_ctx)?;
                self.lower_literal_rel(tag, rel, relation_def_id, relation_span, None, type_ctx)
            }
        }
    }

    fn lower_literal_rel(
        &mut self,
        tag: Tag,
        rel: &ontol_log::sem_stmts::Rel,
        relation_def_id: DefId,
        relation_span: SourceSpan,
        implicit_rel_params: Option<RelParams>,
        type_ctx: &mut TypeContext,
    ) -> Option<()> {
        let (subj_def_id, subj_type_span) = self.resolve_type_ref(&rel.subj_type, type_ctx)?;
        let (obj_def_id, obj_type_span) =
            self.resolve_type_ref_or_pattern(&rel.obj_type, type_ctx)?;

        let rel_id = if subj_def_id == DefId::unit() {
            // a parent relation modifier
            RelId(DefId::unit(), DefRelTag(0))
        } else {
            RelId(
                subj_def_id,
                DefRelTag(u16::try_from(tag.0).expect("FIXME: def rel tags exceeded")),
            )
        };

        self.outcome.root_defs.push(rel_id.0);

        let (rel_params, modifiers) = if let Some(implicit_rel_params) = implicit_rel_params {
            if self.rel_by_parent.remove(&tag).is_some() {
                self.compiler.errors.push(
                    CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes.span(relation_span),
                );
            }

            (implicit_rel_params, vec![])
        } else if let Some(param_rels) = self.rel_by_parent.remove(&tag) {
            let mut context = TypeContext::RelParams {
                rel_id,
                rel_def_id: None,
                modifiers: vec![],
            };

            for (tag, rel) in param_rels {
                self.lower_rel(tag, rel, &mut context);
            }

            let TypeContext::RelParams {
                modifiers,
                rel_def_id,
                ..
            } = context
            else {
                unreachable!()
            };

            (
                rel_def_id.map(RelParams::Def).unwrap_or(RelParams::Unit),
                modifiers,
            )
        } else {
            (RelParams::Unit, vec![])
        };

        let relationship = {
            // FIXME: invertion of subject and object cardinality is confusing:
            Relationship {
                relation_def_id,
                edge_projection: None,
                relation_span,
                subject: (subj_def_id, subj_type_span),
                subject_cardinality: (
                    if rel.opt {
                        PropertyCardinality::Optional
                    } else {
                        PropertyCardinality::Mandatory
                    },
                    map_sem_value_crd(rel.obj_cardinality),
                ),
                object: (obj_def_id, obj_type_span),
                object_cardinality: (
                    PropertyCardinality::Optional,
                    map_sem_value_crd(rel.subj_cardinality),
                ),
                rel_params,
                macro_source: None,
                modifiers,
            }
        };

        self.check_identifies(&relationship);

        match type_ctx {
            TypeContext::Root => {
                self.outcome.predefine_rel(
                    rel_id,
                    relationship,
                    relation_span,
                    None, // FIXME: docs
                );
            }
            TypeContext::RelParams { modifiers, .. } => {
                modifiers.push((relationship, relation_span));
            }
        }

        Some(())
    }

    fn lower_edge_rel(
        &mut self,
        edge_id: EdgeId,
        coord: ArcCoord,
        rel_tag: Tag,
        rel: &ontol_log::sem_stmts::Rel,
        relation_span: SourceSpan,
    ) -> Option<()> {
        // TODO: documentation

        let (subj_def_id, subj_type_span) =
            self.resolve_type_ref(&rel.subj_type, &mut TypeContext::Root)?;
        let (obj_def_id, obj_type_span) =
            self.resolve_type_ref_or_pattern(&rel.obj_type, &mut TypeContext::Root)?;

        let edge = self
            .compiler
            .edge_ctx
            .symbolic_edges
            .get_mut(&edge_id)
            .unwrap();

        let (sym_id, slot) = edge.symbols.iter().find(|(_sym_id, slot)| {
            slot.clause_idx == coord.0 as usize && slot.right.0 == coord.1
        })?;

        info!("lower edge rel {edge_id:?} sym_id={sym_id:?}");

        let relationship = {
            // FIXME: this is not inverted, though compiler expects inversion..?
            let subject_cardinality = map_sem_value_crd(rel.subj_cardinality);
            let object_cardinality = map_sem_value_crd(rel.obj_cardinality);

            let unique = matches!(subject_cardinality, ValueCardinality::Unit);
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
                relation_def_id: *sym_id,
                edge_projection: Some(projection),
                relation_span,
                subject: (subj_def_id, subj_type_span),
                subject_cardinality: (
                    if rel.opt {
                        PropertyCardinality::Optional
                    } else {
                        PropertyCardinality::Mandatory
                    },
                    subject_cardinality,
                ),
                object: (obj_def_id, obj_type_span),
                object_cardinality: (PropertyCardinality::Optional, object_cardinality),
                rel_params: RelParams::Unit,
                macro_source: None,
                modifiers: vec![],
            }
        };

        self.check_identifies(&relationship);

        let rel_id = RelId(
            subj_def_id,
            DefRelTag(u16::try_from(rel_tag.0).expect("FIXME: def rel tags exceeded")),
        );
        self.outcome.predefine_rel(
            rel_id,
            relationship,
            relation_span,
            None, // FIXME: docs
        );

        Some(())
    }

    fn check_identifies(&mut self, relationship: &Relationship) {
        if relationship.subject_cardinality.1 == ValueCardinality::Unit
            && relationship.object_cardinality.1 == ValueCardinality::Unit
        {
            let identifies_relationship = Relationship {
                relation_def_id: OntolDefTag::RelationIdentifies.def_id(),
                edge_projection: None,
                relation_span: relationship.relation_span,
                subject: relationship.object,
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                object: relationship.subject,
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                rel_params: RelParams::Unit,
                macro_source: None,
                modifiers: vec![],
            };

            let rel_id = self
                .compiler
                .rel_ctx
                .alloc_rel_id(identifies_relationship.subject.0);
            self.outcome.predefine_rel(
                rel_id,
                identifies_relationship,
                relationship.relation_span,
                None,
            );
        }
    }
}

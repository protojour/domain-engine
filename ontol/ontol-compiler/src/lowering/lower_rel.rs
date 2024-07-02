use std::ops::Range;

use ontol_parser::{
    cst::{inspect as insp, view::NodeView},
    U32Span,
};
use ontol_runtime::{
    ontology::domain::EdgeCardinalProjection,
    property::{PropertyCardinality, ValueCardinality},
    tuple::CardinalIdx,
    DefId, EdgeId, RelationshipId,
};

use crate::{
    def::{DefKind, RelParams, Relationship, TypeDef, TypeDefFlags},
    CompileError,
};

use super::context::{BlockContext, CstLowering, MapVarTable, RelationKey, RootDefs};

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn lower_rel_statement(
        &mut self,
        stmt: insp::RelStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let rel_subject = stmt.subject()?;
        let fwd_set = stmt.fwd_set()?;
        let rel_object = stmt.object()?;

        let mut root_defs = RootDefs::default();

        let subject_def_id = self.resolve_type_reference(
            rel_subject.type_quant()?.type_ref()?,
            &block,
            Some(&mut root_defs),
        )?;
        let object_def_id = match rel_object.type_quant_or_pattern()? {
            insp::TypeQuantOrPattern::TypeQuant(type_quant) => {
                self.resolve_type_reference(type_quant.type_ref()?, &block, Some(&mut root_defs))?
            }
            insp::TypeQuantOrPattern::Pattern(pattern) => {
                let mut var_table = MapVarTable::default();
                let lowered = self.lower_pattern(pattern.clone(), &mut var_table);
                let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
                self.ctx.compiler.patterns.table.insert(pat_id, lowered);

                self.ctx
                    .define_anonymous(DefKind::Constant(pat_id), pattern.view().span())
            }
        };

        for (index, relation) in fwd_set.relations().enumerate() {
            if let Some(mut defs) = self.lower_relationship(
                subject_def_id,
                rel_subject.clone(),
                relation,
                object_def_id,
                rel_object.clone(),
                if index == 0 {
                    stmt.clone().backwd_set()
                } else {
                    None
                },
                stmt.clone(),
            ) {
                root_defs.append(&mut defs);
            }
        }

        Some(root_defs)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_relationship(
        &mut self,
        subject_def: DefId,
        rel_subject: insp::RelSubject<V>,
        relation: insp::Relation<V>,
        object_def: DefId,
        rel_object: insp::RelObject<V>,
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
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
                type_ref => {
                    let def_id = self.resolve_type_reference(
                        type_ref,
                        &BlockContext::NoContext,
                        Some(&mut root_defs),
                    )?;
                    let span = type_quant.view().span();

                    match self.ctx.compiler.defs.def_kind(def_id) {
                        DefKind::TextLiteral(_) => (RelationKey::Named(def_id), span, None),
                        DefKind::Type(_) => {
                            if let Some(edge_id) =
                                self.ctx.compiler.edge_ctx.edge_id_by_symbol(def_id)
                            {
                                return self.lower_edge_relationship(
                                    subject_def,
                                    rel_subject,
                                    (def_id, span, edge_id),
                                    relation,
                                    object_def,
                                    rel_object,
                                    backward_relation,
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
            subject_def,
            rel_subject,
            (key, ident_span, index_range_rel_params),
            relation,
            object_def,
            rel_object,
            backward_relation,
            rel_stmt,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_literal_relationship(
        &mut self,
        subject_def: DefId,
        rel_subject: insp::RelSubject<V>,
        (key, ident_span, index_range_rel_params): (
            RelationKey,
            U32Span,
            Option<Range<Option<u16>>>,
        ),
        relation: insp::Relation<V>,
        object_def: DefId,
        rel_object: insp::RelObject<V>,
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        let has_object_prop = backward_relation.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(key, ident_span);

        let relationship_id = self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id);
        self.append_documentation(relationship_id, rel_stmt.0.clone());

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some(rp) = relation.rel_params() {
                CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes
                    .span_report(rp.0.span(), &mut self.ctx);
                return None;
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some(rp) = relation.rel_params() {
            let rel_def_id = self.ctx.define_anonymous_type(
                TypeDef {
                    ident: None,
                    rel_type_for: Some(RelationshipId(relationship_id)),
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
                .add_anonymous(self.ctx.package_id, rel_def_id);

            for statement in rp.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }

            RelParams::Type(rel_def_id)
        } else {
            RelParams::Unit
        };

        // Just reuse relationship id as the edge id
        let edge_id = EdgeId(relationship_id);

        let object_prop = backward_relation
            .clone()
            .and_then(|rel| rel.name())
            .and_then(|name| Some((name.clone(), name.text()?)))
            .and_then(|(name, result)| Some((name, self.ctx.unescape(result)?)))
            .map(|(name, prop)| (name, self.ctx.compiler.str_ctx.intern(&prop)));

        let mut relationship0 = {
            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                value_cardinality(match rel_object.type_quant_or_pattern() {
                    Some(insp::TypeQuantOrPattern::TypeQuant(type_quant)) => Some(type_quant),
                    _ => None,
                })
                .unwrap_or(ValueCardinality::Unit),
            );
            let object_cardinality = {
                let default = if has_object_prop {
                    // i.e. no syntax sugar: The object prop is explicit,
                    // therefore the object cardinality is explicit.
                    (PropertyCardinality::Mandatory, ValueCardinality::Unit)
                } else {
                    // The syntactic sugar case, which is the default behaviour:
                    // Many incoming edges to the same object:
                    (PropertyCardinality::Optional, ValueCardinality::IndexSet)
                };

                (
                    backward_relation
                        .and_then(|rel| property_cardinality(rel.prop_cardinality()))
                        .unwrap_or(default.0),
                    value_cardinality(rel_subject.type_quant()).unwrap_or(default.1),
                )
            };

            Relationship {
                relation_def_id,
                projection: EdgeCardinalProjection {
                    id: edge_id,
                    subject: CardinalIdx(0),
                    object: CardinalIdx(1),
                    one_to_one: false,
                },
                relation_span: self.ctx.source_span(ident_span),
                subject: (subject_def, self.ctx.source_span(rel_subject.0.span())),
                subject_cardinality,
                object: (object_def, self.ctx.source_span(rel_object.0.span())),
                object_cardinality,
                rel_params: rel_params.clone(),
            }
        };

        if let Some((name, prop)) = object_prop {
            let relation1_def_id = self.unescaped_text_literal_def_id(prop);
            let relationship_id1 = self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id);

            let relationship1 = Relationship {
                relation_def_id: relation1_def_id,
                projection: EdgeCardinalProjection {
                    id: edge_id,
                    subject: CardinalIdx(1),
                    object: CardinalIdx(0),
                    one_to_one: false,
                },
                relation_span: self.ctx.source_span(name.view().span()),
                subject: relationship0.object,
                subject_cardinality: relationship0.object_cardinality,
                object: relationship0.subject,
                object_cardinality: relationship0.subject_cardinality,
                rel_params,
            };

            self.ctx.set_def_kind(
                relationship_id1,
                DefKind::Relationship(relationship1),
                rel_stmt.0.span(),
            );
            root_defs.push(relationship_id1);
        }

        // HACK(for now): invert id relationship
        if relation_def_id == self.ctx.compiler.primitives.relations.id {
            relationship0 = Relationship {
                relation_def_id: self.ctx.compiler.primitives.relations.identifies,
                projection: EdgeCardinalProjection {
                    id: edge_id,
                    object: CardinalIdx(0),
                    subject: CardinalIdx(1),
                    one_to_one: false,
                },
                relation_span: relationship0.relation_span,
                subject: relationship0.object,
                subject_cardinality: relationship0.object_cardinality,
                object: relationship0.subject,
                object_cardinality: relationship0.subject_cardinality,
                rel_params: relationship0.rel_params,
            };
        }

        self.ctx.set_def_kind(
            relationship_id,
            DefKind::Relationship(relationship0),
            rel_stmt.0.span(),
        );
        root_defs.push(relationship_id);

        Some(root_defs)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_edge_relationship(
        &mut self,
        subject_def: DefId,
        rel_subject: insp::RelSubject<V>,
        (sym_id, ident_span, edge_id): (DefId, U32Span, EdgeId),
        relation: insp::Relation<V>,
        object_def: DefId,
        rel_object: insp::RelObject<V>,
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let edge = self
            .ctx
            .compiler
            .edge_ctx
            .symbolic_edges
            .get_mut(&edge_id)
            .unwrap();
        let relationship_id = self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id);

        let relationship = {
            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                value_cardinality(match rel_object.type_quant_or_pattern() {
                    Some(insp::TypeQuantOrPattern::TypeQuant(type_quant)) => Some(type_quant),
                    _ => None,
                })
                .unwrap_or(ValueCardinality::Unit),
            );
            let object_cardinality = {
                let default = (PropertyCardinality::Optional, ValueCardinality::IndexSet);

                (
                    backward_relation
                        .and_then(|rel| property_cardinality(rel.prop_cardinality()))
                        .unwrap_or(default.0),
                    value_cardinality(rel_subject.type_quant()).unwrap_or(default.1),
                )
            };

            let one_to_one = matches!(subject_cardinality.1, ValueCardinality::Unit);
            let slot = *edge.symbols.get(&sym_id).unwrap();

            let projection = EdgeCardinalProjection {
                id: edge_id,
                subject: slot.left,
                object: slot.right,
                one_to_one,
            };

            if one_to_one {
                let edge_variable = edge.variables.get_mut(&slot.left).unwrap();
                edge_variable.one_to_one_count += 1;
            }

            Relationship {
                relation_def_id: sym_id,
                projection,
                relation_span: self.ctx.source_span(ident_span),
                subject: (subject_def, self.ctx.source_span(rel_subject.0.span())),
                subject_cardinality,
                object: (object_def, self.ctx.source_span(rel_object.0.span())),
                object_cardinality,
                rel_params: RelParams::Unit,
            }
        };

        self.ctx.set_def_kind(
            relationship_id,
            DefKind::Relationship(relationship),
            rel_stmt.0.span(),
        );

        Some(vec![relationship_id])
    }
}

fn value_cardinality<V: NodeView>(
    type_quant: Option<insp::TypeQuant<V>>,
) -> Option<ValueCardinality> {
    Some(match type_quant? {
        insp::TypeQuant::TypeQuantUnit(_) => ValueCardinality::Unit,
        insp::TypeQuant::TypeQuantSet(_) => ValueCardinality::IndexSet,
        insp::TypeQuant::TypeQuantList(_) => ValueCardinality::List,
    })
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

use ontol_runtime::{
    ontology::domain::EdgeCardinalProjection,
    property::{PropertyCardinality, ValueCardinality},
    tuple::CardinalIdx,
    DefId, EdgeId, RelId,
};

use crate::{
    def::DefKind,
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    properties::{Constructor, Properties},
    relation::{RelParams, Relationship},
    text_patterns::TextPatternSegment,
    CompileError, Compiler, SourceSpan,
};

#[derive(Debug)]
pub struct FmtChain {
    pub span: SourceSpan,
    pub origin: FmtTransition,
    pub transitions: Vec<FmtTransition>,
}

#[derive(Debug)]
pub struct FmtTransition {
    pub def: DefId,
    pub span: SourceSpan,
}

impl<'m> Compiler<'m> {
    pub fn check_fmt_chain(&mut self, def_id: DefId, chain: FmtChain) {
        let Ok(origin_segment) = self.get_segment(def_id, chain.origin.def, chain.origin.span)
        else {
            return;
        };

        let mut segments: Vec<TextPatternSegment> = vec![];
        segments.push(origin_segment);

        for transition in chain.transitions {
            let Ok(segment) = self.get_segment(def_id, transition.def, transition.span) else {
                return;
            };
            segments.push(segment);
        }

        let properties = self.prop_ctx.properties_by_def_id_mut(def_id);
        properties.constructor = Constructor::TextFmt(TextPatternSegment::concat(segments));

        self.misc_ctx.text_pattern_constructors.insert(def_id);
    }

    fn get_segment(
        &mut self,
        fmt_def_id: DefId,
        transition_def_id: DefId,
        span: SourceSpan,
    ) -> Result<TextPatternSegment, ()> {
        match self.defs.def_kind(transition_def_id) {
            DefKind::Primitive(PrimitiveKind::Text, _) => Ok(TextPatternSegment::AnyString),
            DefKind::TextLiteral(str) => Ok(if str.is_empty() {
                TextPatternSegment::EmptyString
            } else {
                TextPatternSegment::new_literal(str)
            }),
            DefKind::Regex(_) => Ok(TextPatternSegment::Regex(
                self.defs
                    .literal_regex_meta_table
                    .get(&transition_def_id)
                    .expect("regex hir not found for literal regex")
                    .hir
                    .clone(),
            )),
            DefKind::Primitive(PrimitiveKind::Serial, _) => {
                let rel_id = self.make_relationship(fmt_def_id, transition_def_id, span);
                Ok(TextPatternSegment::Attribute {
                    rel_id,
                    type_def_id: transition_def_id,
                    segment: Box::new(TextPatternSegment::Serial { radix: 10 }),
                })
            }
            _ => {
                let rel_id = self.make_relationship(fmt_def_id, transition_def_id, span);

                let constructor = self
                    .prop_ctx
                    .properties_by_def_id(transition_def_id)
                    .map(Properties::constructor);

                match constructor {
                    Some(Constructor::TextFmt(rel_segment)) => Ok(TextPatternSegment::Attribute {
                        rel_id,
                        type_def_id: transition_def_id,
                        segment: Box::new(rel_segment.clone()),
                    }),
                    _ => {
                        CompileError::CannotConcatenateStringPattern
                            .span(span)
                            .report(self);
                        Err(())
                    }
                }
            }
        }
    }

    fn make_relationship(
        &mut self,
        fmt_def_id: DefId,
        transition_def_id: DefId,
        span: SourceSpan,
    ) -> RelId {
        let rel_id = self.rel_ctx.alloc_rel_id(fmt_def_id);
        self.rel_ctx.commit_rel(
            rel_id,
            Relationship {
                relation_def_id: transition_def_id,
                projection: EdgeCardinalProjection {
                    id: EdgeId(ONTOL_PKG, 0),
                    subject: CardinalIdx(0),
                    object: CardinalIdx(0),
                    one_to_one: false,
                },
                subject: (fmt_def_id, span),
                object: (transition_def_id, span),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
                rel_params: RelParams::Unit,
                relation_span: span,
                macro_source: None,
            },
            span,
        );

        rel_id
    }
}

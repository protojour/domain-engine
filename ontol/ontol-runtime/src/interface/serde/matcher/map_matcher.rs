use std::ops::ControlFlow;

use ontol_macros::OntolDebug;
use tracing::trace;

use crate::{
    debug::OntolDebug,
    interface::{
        discriminator::{Discriminant, LeafDiscriminant, VariantPurpose},
        serde::{
            OntologyCtx,
            operator::{
                AppliedVariants, PossibleVariants, SerdeOperator, SerdeOperatorAddr,
                SerdeUnionVariant, StructOperator,
            },
            processor::{
                ProcessorLevel, ProcessorMode, ProcessorProfile, ScalarFormat, SpecialProperty,
                SubProcessorContext,
            },
        },
    },
};

/// A state machine for map matching
pub struct MapMatcher<'on, 'p> {
    possible_variants: PossibleVariants<'on>,
    match_if_singleton: Option<&'on SerdeUnionVariant>,
    ontology: OntologyCtx<'on>,
    ctx: SubProcessorContext,
    profile: &'p ProcessorProfile<'p>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

#[derive(OntolDebug)]
pub struct MatchOk<'on> {
    pub struct_op: &'on StructOperator,
    pub addr: SerdeOperatorAddr,
    pub ctx: SubProcessorContext,
}

enum AttrMatch<'on> {
    Unmatched,
    Match(&'on SerdeUnionVariant),
}

impl<'on, 'p> MapMatcher<'on, 'p> {
    pub fn new(
        possible_variants: PossibleVariants<'on>,
        ontology: OntologyCtx<'on>,
        ctx: SubProcessorContext,
        profile: &'p ProcessorProfile<'p>,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> Self {
        Self {
            possible_variants,
            match_if_singleton: None,
            ontology,
            ctx,
            profile,
            mode,
            level,
        }
    }

    /// consume the last attribute in the buffer,
    /// (or the whole buffer recursively if there's a match that requires re-match),
    /// Returns ControlFlow::Break if a match was found,
    /// ControlFlow::Continue if not.
    /// NB: panics if buffer is empty
    pub fn consume_next_attr(
        &mut self,
        buffer: &[(Box<str>, serde_value::Value)],
    ) -> ControlFlow<MatchOk<'on>> {
        self.match_if_singleton = None;

        match self.match_attribute(buffer, buffer.len() - 1) {
            AttrMatch::Match(variant) => match self.commit_match(variant) {
                ControlFlow::Break(match_ok) => ControlFlow::Break(match_ok),
                ControlFlow::Continue(()) => self.match_whole_buffer(buffer),
            },
            AttrMatch::Unmatched => ControlFlow::Continue(()),
        }
    }

    pub fn consume_end(
        &mut self,
        buffer: &[(Box<str>, serde_value::Value)],
    ) -> Result<MatchOk<'on>, ()> {
        if let Some(variant) = std::mem::take(&mut self.match_if_singleton) {
            if buffer.len() == 1 {
                return match self.commit_match(variant) {
                    ControlFlow::Break(match_ok) => Ok(match_ok),
                    ControlFlow::Continue(()) => match self.match_whole_buffer(buffer) {
                        ControlFlow::Break(match_ok) => Ok(match_ok),
                        ControlFlow::Continue(()) => Err(()),
                    },
                };
            }
        }

        Err(())
    }

    fn match_whole_buffer(
        &mut self,
        buffer: &[(Box<str>, serde_value::Value)],
    ) -> ControlFlow<MatchOk<'on>> {
        let mut retry = true;

        'outer: while retry {
            retry = false;

            for index in (0..buffer.len()).rev() {
                match self.match_attribute(buffer, index) {
                    AttrMatch::Match(variant) => match self.commit_match(variant) {
                        ControlFlow::Break(ok) => {
                            return ControlFlow::Break(ok);
                        }
                        ControlFlow::Continue(()) => {
                            retry = true;
                            continue 'outer;
                        }
                    },
                    AttrMatch::Unmatched => {}
                }
            }
        }

        ControlFlow::Continue(())
    }

    fn match_attribute(
        &self,
        buffer: &[(Box<str>, serde_value::Value)],
        index: usize,
    ) -> AttrMatch<'on> {
        let (property, value) = &buffer[index];

        for possible_variant in self.possible_variants.into_iter() {
            if let Discriminant::HasAttribute(_, match_property, scalar_discriminant) =
                possible_variant.discriminant()
            {
                if property.as_ref() == &self.ontology.defs[*match_property]
                    && self.match_attribute_value(
                        value,
                        possible_variant.purpose(),
                        scalar_discriminant,
                    )
                {
                    trace!("matched attribute name `{:?}`", property.as_ref());
                    return AttrMatch::Match(possible_variant);
                }
            }
        }

        {
            let profile_api = self.profile.api;
            if let Some(type_annotation_property) =
                profile_api.find_special_property_name(SpecialProperty::TypeAnnotation)
            {
                if property.as_ref() == type_annotation_property {
                    if let Some(def_id) = profile_api.annotate_type(value) {
                        trace!("type annotated as {def_id:?}");
                        for variant in self.possible_variants.into_iter() {
                            if variant.deserialize.def_id == def_id {
                                return AttrMatch::Match(variant);
                            }
                        }
                    }
                }
            }
        }

        AttrMatch::Unmatched
    }

    fn match_attribute_value(
        &self,
        value: &serde_value::Value,
        variant_purpose: &VariantPurpose,
        scalar_discriminant: &LeafDiscriminant,
    ) -> bool {
        // Prevent ID pattern "injection":
        // When the id_format is RawText, it cannot not be used for disambiguation,
        // instead we have to fall back to relying on explicit type annotation.
        if matches!(variant_purpose, VariantPurpose::Identification)
            && matches!(self.profile.id_format, ScalarFormat::RawText)
        {
            return false;
        }

        match (scalar_discriminant, value) {
            (LeafDiscriminant::IsAny, _) => true,
            (LeafDiscriminant::IsUnit, serde_value::Value::Unit) => true,
            (
                LeafDiscriminant::IsInt,
                serde_value::Value::I8(_)
                | serde_value::Value::I16(_)
                | serde_value::Value::I32(_)
                | serde_value::Value::I64(_),
            ) => true,
            (LeafDiscriminant::IsSequence, serde_value::Value::Seq(_)) => true,
            (LeafDiscriminant::IsTextLiteral(constant), serde_value::Value::String(value)) => {
                value == &self.ontology.defs[*constant]
            }
            (
                LeafDiscriminant::MatchesCapturingTextPattern(def_id),
                serde_value::Value::String(value),
            ) => {
                let pattern = self.ontology.defs.text_patterns.get(def_id).unwrap();
                pattern.regex.is_match(value)
            }
            _ => false,
        }
    }

    fn commit_match(
        &mut self,
        matched_variant: &'on SerdeUnionVariant,
    ) -> ControlFlow<MatchOk<'on>> {
        match (
            &self.ontology.serde[matched_variant.deserialize.addr],
            matched_variant.purpose(),
        ) {
            (SerdeOperator::Struct(struct_op), _) => ControlFlow::Break(MatchOk {
                struct_op,
                addr: matched_variant.deserialize.addr,
                ctx: self.ctx,
            }),
            (SerdeOperator::Union(union_op), _) => {
                match union_op.applied_deserialize_variants(self.mode, self.level) {
                    AppliedVariants::Unambiguous(_) => todo!(),
                    AppliedVariants::OneOf(variants) => {
                        self.possible_variants = variants;
                        // possible variants replaced with new variants, now continue the search
                        ControlFlow::Continue(())
                    }
                }
            }
            other => panic!(
                "Matched discriminator is not a map type: {other:?}",
                other = other.debug(self.ontology.defs)
            ),
        }
    }
}

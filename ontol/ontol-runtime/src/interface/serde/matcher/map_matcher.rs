use std::ops::ControlFlow;

use ontol_macros::OntolDebug;
use tracing::debug;

use crate::{
    debug::OntolDebug,
    interface::{
        discriminator::{Discriminant, LeafDiscriminant, PropCount, VariantPurpose},
        serde::{
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
    ontology::{ontol::TextConstant, Ontology},
    DefId,
};

/// A state machine for map matching
pub struct MapMatcher<'on, 'p> {
    possible_variants: PossibleVariants<'on>,
    match_if_singleton: Option<&'on SerdeUnionVariant>,
    ontology: &'on Ontology,
    ctx: SubProcessorContext,
    profile: &'p ProcessorProfile<'p>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

#[derive(OntolDebug)]
pub struct MatchOk<'on> {
    pub mode: MapMatchMode<'on>,
    pub ctx: SubProcessorContext,
}

#[derive(OntolDebug)]
pub enum MapMatchMode<'on> {
    Struct(SerdeOperatorAddr, &'on StructOperator),
    EntityId(DefId, TextConstant, SerdeOperatorAddr),
    RawDynamicEntity(&'on StructOperator),
}

enum AttrMatch<'on> {
    Unmatched,
    Match(&'on SerdeUnionVariant),
    MatchIfSingleton(&'on SerdeUnionVariant),
}

impl<'on, 'p> MapMatcher<'on, 'p> {
    pub fn new(
        possible_variants: PossibleVariants<'on>,
        ontology: &'on Ontology,
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
                ControlFlow::Break(mode) => ControlFlow::Break(MatchOk {
                    mode,
                    ctx: self.ctx,
                }),
                ControlFlow::Continue(()) => self.match_whole_buffer(buffer),
            },
            AttrMatch::MatchIfSingleton(possible) => {
                self.match_if_singleton = Some(possible);
                ControlFlow::Continue(())
            }
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
                    ControlFlow::Break(mode) => Ok(MatchOk {
                        mode,
                        ctx: self.ctx,
                    }),
                    ControlFlow::Continue(()) => match self.match_whole_buffer(buffer) {
                        ControlFlow::Break(ok) => Ok(ok),
                        ControlFlow::Continue(()) => Err(()),
                    },
                };
            }
        }

        for variant in self.possible_variants {
            if matches!(variant.discriminant(), Discriminant::StructFallback) {
                match &self.ontology[variant.deserialize.addr] {
                    SerdeOperator::Struct(struct_op) => {
                        return Ok(MatchOk {
                            mode: MapMatchMode::Struct(variant.deserialize.addr, struct_op),
                            ctx: self.ctx,
                        })
                    }
                    SerdeOperator::IdSingletonStruct(entity_id, name_constant, addr) => {
                        return Ok(MatchOk {
                            mode: MapMatchMode::EntityId(*entity_id, *name_constant, *addr),
                            ctx: self.ctx,
                        })
                    }
                    other => panic!(
                        "Matched discriminator is not a map type: {other:?}",
                        other = other.debug(self.ontology)
                    ),
                }
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
                        ControlFlow::Break(mode) => {
                            return ControlFlow::Break(MatchOk {
                                mode,
                                ctx: self.ctx,
                            });
                        }
                        ControlFlow::Continue(()) => {
                            retry = true;
                            continue 'outer;
                        }
                    },
                    AttrMatch::MatchIfSingleton(possible) => {
                        self.match_if_singleton = Some(possible);
                        return ControlFlow::Continue(());
                    }
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
            match possible_variant.discriminant() {
                Discriminant::HasAttribute(
                    _,
                    match_property,
                    PropCount::Any,
                    scalar_discriminant,
                ) => {
                    if property.as_ref() == &self.ontology[*match_property]
                        && self.match_attribute_value(
                            value,
                            possible_variant.purpose(),
                            scalar_discriminant,
                        )
                    {
                        debug!("matched attribute name `{:?}`", property.as_ref());
                        return AttrMatch::Match(possible_variant);
                    }
                }
                Discriminant::HasAttribute(
                    _,
                    match_property,
                    PropCount::One,
                    scalar_discriminant,
                ) => {
                    if property.as_ref() == &self.ontology[*match_property]
                        && buffer.len() == 1
                        && self.match_attribute_value(
                            value,
                            possible_variant.purpose(),
                            scalar_discriminant,
                        )
                    {
                        return AttrMatch::MatchIfSingleton(possible_variant);
                    }
                }
                _ => {}
            }
        }

        {
            let profile_api = self.profile.api;
            if let Some(type_annotation_property) =
                profile_api.find_special_property_name(SpecialProperty::TypeAnnotation)
            {
                if property.as_ref() == type_annotation_property {
                    if let Some(def_id) = profile_api.annotate_type(value) {
                        for variant in self.possible_variants.into_iter() {
                            if variant.deserialize.def_id == def_id {
                                return AttrMatch::Match(variant);
                            } else if let VariantPurpose::Identification { entity_id } =
                                variant.purpose()
                            {
                                if entity_id == &def_id {
                                    return AttrMatch::Match(variant);
                                }
                            } else if let VariantPurpose::Identification2 = variant.purpose() {
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
        if matches!(
            variant_purpose,
            VariantPurpose::Identification { .. }
                | VariantPurpose::RawDynamicEntity
                | VariantPurpose::Identification2
        ) && matches!(self.profile.id_format, ScalarFormat::RawText)
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
                value == &self.ontology[*constant]
            }
            (
                LeafDiscriminant::MatchesCapturingTextPattern(def_id),
                serde_value::Value::String(value),
            ) => {
                let pattern = self.ontology.data.text_patterns.get(def_id).unwrap();
                pattern.regex.is_match(value)
            }
            _ => false,
        }
    }

    fn commit_match(
        &mut self,
        matched_variant: &'on SerdeUnionVariant,
    ) -> ControlFlow<MapMatchMode<'on>> {
        match (
            &self.ontology[matched_variant.deserialize.addr],
            matched_variant.purpose(),
        ) {
            (SerdeOperator::Struct(struct_op), VariantPurpose::RawDynamicEntity) => {
                ControlFlow::Break(MapMatchMode::RawDynamicEntity(struct_op))
            }
            (SerdeOperator::Struct(struct_op), _) => ControlFlow::Break(MapMatchMode::Struct(
                matched_variant.deserialize.addr,
                struct_op,
            )),
            (SerdeOperator::IdSingletonStruct(entity_id, name_constant, addr), _) => {
                ControlFlow::Break(MapMatchMode::EntityId(*entity_id, *name_constant, *addr))
            }
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
                other = other.debug(self.ontology)
            ),
        }
    }
}

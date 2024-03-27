use ontol_macros::OntolDebug;

use crate::{
    interface::{
        discriminator::{Discriminant, LeafDiscriminant, VariantPurpose},
        serde::{
            operator::{
                AppliedVariants, PossibleVariant, PossibleVariants, SerdeOperator,
                SerdeOperatorAddr, StructOperator,
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

pub enum MapMatchResult<'on, 'p> {
    Match(MapMatch<'on>),
    Indecisive(MapMatcher<'on, 'p>),
}

#[derive(OntolDebug)]
pub struct MapMatch<'on> {
    pub mode: MapMatchMode<'on>,
    pub ctx: SubProcessorContext,
}

#[derive(OntolDebug)]
pub enum MapMatchMode<'on> {
    Struct(&'on StructOperator),
    EntityId(DefId, TextConstant, SerdeOperatorAddr),
    RawDynamicEntity(&'on StructOperator),
}

#[derive(Clone)]
pub struct MapMatcher<'on, 'p> {
    pub(super) possible_variants: PossibleVariants<'on>,
    pub(super) ontology: &'on Ontology,
    pub ctx: SubProcessorContext,
    pub(super) profile: &'p ProcessorProfile<'p>,
    pub(super) mode: ProcessorMode,
    pub(super) level: ProcessorLevel,
}

impl<'on, 'p> MapMatcher<'on, 'p> {
    pub fn match_attribute(
        self,
        property: &str,
        value: &serde_value::Value,
    ) -> MapMatchResult<'on, 'p> {
        // trace!("match_attribute '{property}': {:#?}", self.variants);

        let match_fn = |variant: &PossibleVariant| -> bool {
            let discriminant = variant.discriminant;
            // debug!("match_fn property `{property}` variant: {variant:?}");

            let Discriminant::HasAttribute(_, match_attr_constant, scalar_discriminant) =
                discriminant
            else {
                return false;
            };

            if property != &self.ontology[*match_attr_constant] {
                return false;
            }

            // Prevent ID pattern "injection":
            // When the id_format is RawText, it cannot not be used for disambiguation,
            // instead we have to fall back to relying on explicit type annotation.
            if matches!(
                variant.purpose,
                VariantPurpose::Identification { .. } | VariantPurpose::RawDynamicEntity
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
        };

        let mut found_variant = self.possible_variants.into_iter().find(match_fn);

        if found_variant.is_none() {
            let profile_api = self.profile.api;
            if let Some(type_annotation_property) =
                profile_api.find_special_property_name(SpecialProperty::TypeAnnotation)
            {
                if property == type_annotation_property {
                    if let Some(def_id) = profile_api.annotate_type(value) {
                        for variant in self.possible_variants {
                            if variant.serde_def.def_id == def_id {
                                found_variant = Some(variant);
                                break;
                            } else if let VariantPurpose::Identification { entity_id } =
                                variant.purpose
                            {
                                if entity_id == def_id {
                                    found_variant = Some(variant);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        let result =
            found_variant.map(
                |variant| match (&self.ontology[variant.addr], variant.purpose) {
                    (SerdeOperator::Struct(struct_op), VariantPurpose::RawDynamicEntity) => {
                        self.map_match(MapMatchMode::RawDynamicEntity(struct_op))
                    }
                    (SerdeOperator::Struct(struct_op), _) => {
                        self.map_match(MapMatchMode::Struct(struct_op))
                    }
                    (SerdeOperator::IdSingletonStruct(entity_id, name_constant, addr), _) => {
                        self.map_match(MapMatchMode::EntityId(*entity_id, *name_constant, *addr))
                    }
                    (SerdeOperator::Union(union_op), _) => {
                        match union_op.applied_variants(self.mode, self.level) {
                            AppliedVariants::Unambiguous(_) => todo!(),
                            AppliedVariants::OneOf(variants) => MapMatcher {
                                possible_variants: variants,
                                ctx: self.ctx,
                                ontology: self.ontology,
                                profile: self.profile,
                                mode: self.mode,
                                level: self.level,
                            }
                            .match_attribute(property, value),
                        }
                    }
                    other => panic!(
                        "Matched discriminator is not a map type: {other:?}",
                        other = self.ontology.debug(&other)
                    ),
                },
            );

        match result {
            None => MapMatchResult::Indecisive(self),
            Some(result) => result,
        }
    }

    pub fn match_fallback(self) -> MapMatchResult<'on, 'p> {
        // debug!("match_fallback");

        for variant in self.possible_variants {
            if matches!(variant.discriminant, Discriminant::StructFallback) {
                match &self.ontology[variant.addr] {
                    SerdeOperator::Struct(struct_op) => {
                        return self.map_match(MapMatchMode::Struct(struct_op))
                    }
                    SerdeOperator::IdSingletonStruct(entity_id, name_constant, addr) => {
                        return self.map_match(MapMatchMode::EntityId(
                            *entity_id,
                            *name_constant,
                            *addr,
                        ))
                    }
                    other => panic!(
                        "Matched discriminator is not a map type: {other:?}",
                        other = self.ontology.debug(&other)
                    ),
                }
            }
        }

        MapMatchResult::Indecisive(self)
    }

    fn map_match(&self, mode: MapMatchMode<'on>) -> MapMatchResult<'on, 'p> {
        MapMatchResult::Match(MapMatch {
            mode,
            ctx: self.ctx,
        })
    }
}

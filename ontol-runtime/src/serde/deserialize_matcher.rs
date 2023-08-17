use std::{fmt::Display, ops::RangeInclusive};

use crate::{
    discriminator::Discriminant,
    format_utils::{Backticks, LogicOp, Missing},
    ontology::Ontology,
    string_pattern::StringPattern,
    string_types::ParseError,
    value::{Data, Value},
    DefId,
};

use super::{
    operator::{FilteredVariants, SequenceRange, SerdeOperator, ValueUnionVariant},
    processor::{ProcessorLevel, ProcessorMode, SubProcessorContext},
    SerdeOperatorId, StructOperator,
};

pub struct ExpectingMatching<'v>(pub &'v dyn ValueMatcher);

impl<'v> Display for ExpectingMatching<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.expecting(f)
    }
}

pub struct SeqElementMatch {
    pub element_operator_id: SerdeOperatorId,
    pub ctx: SubProcessorContext,
}

/// Trait for matching incoming types for deserialization
pub trait ValueMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;

    fn match_unit(&self) -> Result<Value, ()> {
        Err(())
    }

    fn match_boolean(&self, _: bool) -> Result<Value, ()> {
        Err(())
    }

    fn match_u64(&self, _: u64) -> Result<Value, ()> {
        Err(())
    }

    fn match_i64(&self, _: i64) -> Result<Value, ()> {
        Err(())
    }

    fn match_f64(&self, _: f64) -> Result<Value, ()> {
        Err(())
    }

    fn match_str(&self, _: &str) -> Result<Value, ()> {
        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceMatcher, ()> {
        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher, ()> {
        Err(())
    }
}

pub struct UnitMatcher;

impl ValueMatcher for UnitMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "unit")
    }

    fn match_unit(&self) -> Result<Value, ()> {
        Ok(Value::new(Data::Unit, DefId::unit()))
    }
}

pub enum BooleanMatcher {
    False(DefId),
    True(DefId),
    Boolean(DefId),
}

impl ValueMatcher for BooleanMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::False(_) => write!(f, "false"),
            Self::True(_) => write!(f, "true"),
            Self::Boolean(_) => write!(f, "boolean"),
        }
    }

    fn match_boolean(&self, val: bool) -> Result<Value, ()> {
        let int = if val { 1 } else { 0 };
        match (self, val) {
            (Self::False(def_id), false) => Ok(Value::new(Data::I64(int), *def_id)),
            (Self::True(def_id), true) => Ok(Value::new(Data::I64(int), *def_id)),
            (Self::Boolean(def_id), _) => Ok(Value::new(Data::I64(int), *def_id)),
            _ => Err(()),
        }
    }
}

pub struct NumberMatcher<T> {
    pub def_id: DefId,
    pub range: Option<RangeInclusive<T>>,
}

impl ValueMatcher for NumberMatcher<i64> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "integer{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value {
            data: Data::I64(value),
            type_def_id: self.def_id,
        })
    }
}

impl ValueMatcher for NumberMatcher<f64> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "float{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        self.match_f64(value as f64)
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        self.match_f64(value as f64)
    }

    fn match_f64(&self, value: f64) -> Result<Value, ()> {
        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value {
            data: Data::F64(value),
            type_def_id: self.def_id,
        })
    }
}

/// match any string
pub struct StringMatcher<'e> {
    pub def_id: DefId,
    pub ontology: &'e Ontology,
}

impl<'e> ValueMatcher for StringMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string")
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        Ok(Value::new(Data::String(str.into()), self.def_id))
    }
}

/// match a constant string
pub struct ConstantStringMatcher<'e> {
    pub literal: &'e str,
    pub def_id: DefId,
}

impl<'e> ValueMatcher for ConstantStringMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "\"{}\"", self.literal)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if str == self.literal {
            Ok(Value::new(Data::String(str.into()), self.def_id))
        } else {
            Err(())
        }
    }
}

pub struct StringPatternMatcher<'e> {
    pub pattern: &'e StringPattern,
    pub def_id: DefId,
    pub ontology: &'e Ontology,
}

impl<'e> ValueMatcher for StringPatternMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        expecting_custom_string(self.ontology, self.def_id, f)
            .unwrap_or_else(|| write!(f, "string matching /{}/", self.pattern.regex))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if self.pattern.regex.is_match(str) {
            try_deserialize_custom_string(self.ontology, self.def_id, str).map_err(|_| ())
        } else {
            Err(())
        }
    }
}

/// This is a matcher that doesn't necessarily
/// deserialize to a string, but can use capture groups
/// extract various data.
pub struct CapturingStringPatternMatcher<'e> {
    pub pattern: &'e StringPattern,
    pub def_id: DefId,
    pub ontology: &'e Ontology,
}

impl<'e> ValueMatcher for CapturingStringPatternMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        let data = self
            .pattern
            .try_capturing_match(str, self.ontology)
            .map_err(|_| ())?;
        Ok(Value::new(data, self.def_id))
    }
}

#[derive(Clone)]
pub struct SequenceMatcher<'e> {
    ranges: &'e [SequenceRange],
    range_cursor: usize,
    repetition_cursor: u16,

    pub type_def_id: DefId,
    pub ctx: SubProcessorContext,
}

impl<'e> ValueMatcher for SequenceMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut len: usize = 0;
        let mut finite = true;
        for range in self.ranges {
            if let Some(finite_repetition) = &range.finite_repetition {
                finite = true;
                len += *finite_repetition as usize;
            } else {
                finite = false;
            }
        }

        if finite {
            write!(f, "sequence with length {len}")
        } else {
            write!(f, "sequence with minimum length {len}")
        }
    }

    fn match_sequence(&self) -> Result<SequenceMatcher, ()> {
        Ok(self.clone())
    }
}

impl<'e> SequenceMatcher<'e> {
    pub fn new(ranges: &'e [SequenceRange], type_def_id: DefId, ctx: SubProcessorContext) -> Self {
        Self {
            ranges,
            range_cursor: 0,
            repetition_cursor: 0,
            type_def_id,
            ctx,
        }
    }

    pub fn match_next_seq_element(&mut self) -> Option<SeqElementMatch> {
        loop {
            let range = &self.ranges[self.range_cursor];

            if let Some(finite_repetition) = range.finite_repetition {
                if self.repetition_cursor < finite_repetition {
                    self.repetition_cursor += 1;

                    return Some(SeqElementMatch {
                        element_operator_id: range.operator_id,
                        ctx: self.ctx,
                    });
                } else {
                    self.range_cursor += 1;
                    self.repetition_cursor = 0;

                    if self.range_cursor == self.ranges.len() {
                        return None;
                    } else {
                        continue;
                    }
                }
            } else {
                return Some(SeqElementMatch {
                    element_operator_id: range.operator_id,
                    ctx: self.ctx,
                });
            }
        }
    }

    pub fn match_seq_end(&self) -> Result<(), ()> {
        if self.range_cursor == self.ranges.len() {
            return Ok(());
        } else if !self.ranges.is_empty() && self.range_cursor < self.ranges.len() - 1 {
            return Err(());
        }

        let range = &self.ranges[self.range_cursor];
        if let Some(finite_repetition) = range.finite_repetition {
            // note: repetition cursor has already been increased in match_next_seq_element
            if self.repetition_cursor - 1 < finite_repetition {
                Err(())
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

pub struct UnionMatcher<'e> {
    pub typename: &'e str,
    pub variants: &'e [ValueUnionVariant],
    pub ctx: SubProcessorContext,
    pub ontology: &'e Ontology,
    pub mode: ProcessorMode,
    pub level: ProcessorLevel,
}

impl<'e> ValueMatcher for UnionMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            Backticks(self.typename),
            Missing {
                items: self
                    .variants
                    .iter()
                    .map(|discriminator| self.ontology.new_serde_processor(
                        discriminator.operator_id,
                        self.mode,
                        ProcessorLevel::new_root()
                    ))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<Value, ()> {
        let def_id = self.match_discriminant(Discriminant::IsUnit)?;
        Ok(Value::new(Data::Unit, def_id))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        self.match_discriminant(Discriminant::IsInt)
            .map(|def_id| Value {
                data: Data::I64(value),
                type_def_id: def_id,
            })
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        for variant in self.variants {
            match &variant.discriminator.discriminant {
                Discriminant::IsString => {
                    return try_deserialize_custom_string(
                        self.ontology,
                        variant.discriminator.def_variant.def_id,
                        str,
                    )
                    .map_err(|_| ())
                }
                Discriminant::IsStringLiteral(lit) if lit == str => {
                    return try_deserialize_custom_string(
                        self.ontology,
                        variant.discriminator.def_variant.def_id,
                        str,
                    )
                    .map_err(|_| ())
                }
                Discriminant::MatchesCapturingStringPattern(def_id) => {
                    let result_type = variant.discriminator.def_variant.def_id;
                    let pattern = self.ontology.string_patterns.get(def_id).unwrap();

                    if let Ok(data) = pattern.try_capturing_match(str, self.ontology) {
                        return Ok(Value::new(data, result_type));
                    }
                }
                _ => {}
            }
        }

        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceMatcher, ()> {
        for variant in self.variants {
            if variant.discriminator.discriminant == Discriminant::IsSequence {
                match self.ontology.get_serde_operator(variant.operator_id) {
                    SerdeOperator::RelationSequence(seq_op) => {
                        return Ok(SequenceMatcher::new(
                            &seq_op.ranges,
                            seq_op.def_variant.def_id,
                            self.ctx,
                        ))
                    }
                    SerdeOperator::ConstructorSequence(seq_op) => {
                        return Ok(SequenceMatcher::new(
                            &seq_op.ranges,
                            seq_op.def_variant.def_id,
                            self.ctx,
                        ))
                    }
                    _ => panic!("not a sequence"),
                }
            }
        }

        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher, ()> {
        if !self.variants.iter().any(|variant| {
            matches!(
                &variant.discriminator.discriminant,
                Discriminant::MapFallback
                    | Discriminant::IsSingletonProperty(..)
                    | Discriminant::HasStringAttribute(..)
                    | Discriminant::HasAttributeMatchingStringPattern(..)
            )
        }) {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher {
            variants: self.variants,
            ctx: self.ctx,
            ontology: self.ontology,
            mode: self.mode,
            level: self.level,
        })
    }
}

impl<'e> UnionMatcher<'e> {
    fn match_discriminant(&self, discriminant: Discriminant) -> Result<DefId, ()> {
        for variant in self.variants {
            if variant.discriminator.discriminant == discriminant {
                return Ok(variant.discriminator.def_variant.def_id);
            }
        }

        Err(())
    }
}

#[derive(Clone)]
pub struct MapMatcher<'e> {
    variants: &'e [ValueUnionVariant],
    ontology: &'e Ontology,
    pub ctx: SubProcessorContext,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

pub enum MapMatchResult<'e> {
    Match(MapMatch<'e>),
    Indecisive(MapMatcher<'e>),
}

#[derive(Debug)]
pub struct MapMatch<'e> {
    pub kind: MapMatchKind<'e>,
    pub ctx: SubProcessorContext,
}

#[derive(Debug)]
pub enum MapMatchKind<'e> {
    StructType(&'e StructOperator),
    IdType(&'e str, SerdeOperatorId),
}

impl<'e> MapMatcher<'e> {
    pub fn match_attribute(self, property: &str, value: &serde_value::Value) -> MapMatchResult<'e> {
        // debug!("match_attribute '{property}': {:#?}", self.variants);

        let match_fn = |discriminant: &Discriminant| -> bool {
            match (discriminant, value) {
                (
                    Discriminant::HasStringAttribute(_, match_name, match_value),
                    serde_value::Value::String(value),
                ) => property == match_name && value == match_value,
                (Discriminant::IsSingletonProperty(_, match_name), _) => property == match_name,
                (
                    Discriminant::HasAttributeMatchingStringPattern(_, match_name, def_id),
                    serde_value::Value::String(value),
                ) => {
                    if property == match_name {
                        let pattern = self.ontology.string_patterns.get(def_id).unwrap();
                        pattern.regex.is_match(value)
                    } else {
                        false
                    }
                }
                _ => false,
            }
        };

        let result = self
            .variants
            .iter()
            .find(|variant| match_fn(&variant.discriminator.discriminant))
            .map(
                |variant| match self.ontology.get_serde_operator(variant.operator_id) {
                    SerdeOperator::Struct(struct_op) => {
                        MapMatchResult::Match(self.new_match(MapMatchKind::StructType(struct_op)))
                    }
                    SerdeOperator::PrimaryId(name, operator_id) => MapMatchResult::Match(
                        self.new_match(MapMatchKind::IdType(name.as_str(), *operator_id)),
                    ),
                    SerdeOperator::Union(union_op) => {
                        match union_op.variants(self.mode, self.level) {
                            FilteredVariants::Single(_) => todo!(),
                            FilteredVariants::Union(variants) => MapMatcher {
                                variants,
                                ctx: self.ctx,
                                ontology: self.ontology,
                                mode: self.mode,
                                level: self.level,
                            }
                            .match_attribute(property, value),
                        }
                    }
                    other => panic!("Matched discriminator is not a map type: {other:?}"),
                },
            );

        match result {
            None => MapMatchResult::Indecisive(self),
            Some(result) => result,
        }
    }

    pub fn match_fallback(self) -> MapMatchResult<'e> {
        // debug!("match_fallback");

        for variant in self.variants {
            if matches!(
                variant.discriminator.discriminant,
                Discriminant::MapFallback
            ) {
                match self.ontology.get_serde_operator(variant.operator_id) {
                    SerdeOperator::Struct(struct_op) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::StructType(struct_op)),
                        )
                    }
                    SerdeOperator::PrimaryId(name, operator_id) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::IdType(name.as_str(), *operator_id)),
                        )
                    }
                    other => panic!("Matched discriminator is not a map type: {other:?}"),
                }
            }
        }

        MapMatchResult::Indecisive(self)
    }

    fn new_match(&self, kind: MapMatchKind<'e>) -> MapMatch<'e> {
        MapMatch {
            kind,
            ctx: self.ctx,
        }
    }
}

fn try_deserialize_custom_string(
    ontology: &Ontology,
    def_id: DefId,
    str: &str,
) -> Result<Value, ParseError> {
    match ontology.string_like_types.get(&def_id) {
        Some(custom_string_deserializer) => custom_string_deserializer.try_deserialize(def_id, str),
        None => Ok(Value::new(Data::String(str.into()), def_id)),
    }
}

fn expecting_custom_string(
    ontology: &Ontology,
    def_id: DefId,
    f: &mut std::fmt::Formatter,
) -> Option<std::fmt::Result> {
    ontology
        .string_like_types
        .get(&def_id)
        .map(|custom_string_deserializer| write!(f, "`{}`", custom_string_deserializer.type_name()))
}

struct OptWithinRangeDisplay<'a, T>(&'a Option<RangeInclusive<T>>);

impl<'a, T: Display> Display for OptWithinRangeDisplay<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(range) = self.0 {
            write!(f, " in range {}..={}", range.start(), range.end())
        } else {
            Ok(())
        }
    }
}

fn u64_to_i64(value: u64) -> Result<i64, ()> {
    i64::try_from(value).map_err(|_| ())
}

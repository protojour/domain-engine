use std::{fmt::Display, ops::RangeInclusive};

use fnv::FnvHashMap;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use tracing::{error, trace};

use crate::{
    format_utils::{Backticks, LogicOp, Missing},
    interface::discriminator::{Discriminant, LeafDiscriminant},
    ontology::Ontology,
    text_like_types::ParseError,
    text_pattern::{TextPattern, TextPatternConstantPart},
    value::{Attribute, PropertyId, Value},
    DefId, RelationshipId, Role,
};

use super::{
    operator::{FilteredVariants, SequenceRange, SerdeOperator, SerdeUnionVariant},
    processor::{ProcessorLevel, ProcessorMode, ScalarFormat, SubProcessorContext},
    SerdeOperatorAddr, StructOperator,
};

pub struct ExpectingMatching<'v>(pub &'v dyn ValueMatcher);

impl<'v> Display for ExpectingMatching<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.expecting(f)
    }
}

pub struct SeqElementMatch {
    pub element_addr: SerdeOperatorAddr,
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
        Ok(Value::Unit(DefId::unit()))
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
            (Self::False(def_id), false) => Ok(Value::I64(int, *def_id)),
            (Self::True(def_id), true) => Ok(Value::I64(int, *def_id)),
            (Self::Boolean(def_id), _) => Ok(Value::I64(int, *def_id)),
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

        Ok(Value::I64(value, self.def_id))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_i64(str.parse().map_err(|_| ())?)
    }
}

impl ValueMatcher for NumberMatcher<i32> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "integer{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        let value: i32 = value.try_into().map_err(|_| ())?;

        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value::I64(value as i64, self.def_id))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_i64(str.parse().map_err(|_| ())?)
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

        Ok(Value::F64(value, self.def_id))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_f64(str.parse().map_err(|_| ())?)
    }
}

/// match any string
pub struct StringMatcher<'on> {
    pub def_id: DefId,
    pub ontology: &'on Ontology,
}

impl<'on> ValueMatcher for StringMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Even though it's called "text" in ONTOL, we can call it "string" in domain interfaces
        write!(f, "string")
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        Ok(Value::Text(str.into(), self.def_id))
    }
}

/// match a constant text
pub struct ConstantStringMatcher<'on> {
    pub literal: &'on str,
    pub def_id: DefId,
}

impl<'on> ValueMatcher for ConstantStringMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "\"{}\"", self.literal)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if str == self.literal {
            Ok(Value::Text(str.into(), self.def_id))
        } else {
            Err(())
        }
    }
}

pub struct TextPatternMatcher<'on> {
    pub pattern: &'on TextPattern,
    pub def_id: DefId,
    pub ontology: &'on Ontology,
}

impl<'on> ValueMatcher for TextPatternMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        expecting_custom_string(self.ontology, self.def_id, f)
            .unwrap_or_else(|| write!(f, "string matching /{}/", self.pattern.regex.as_str()))
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
pub struct CapturingTextPatternMatcher<'on> {
    pub pattern: &'on TextPattern,
    pub def_id: DefId,
    pub ontology: &'on Ontology,
    pub scalar_format: ScalarFormat,
}

impl<'on> ValueMatcher for CapturingTextPatternMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex.as_str())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        match self.scalar_format {
            ScalarFormat::DomainTransparent => Ok(self
                .pattern
                .try_capturing_match(str, self.def_id, self.ontology)
                .map_err(|_| ())?),

            ScalarFormat::RawText => {
                let mut attrs = FnvHashMap::default();

                for part in &self.pattern.constant_parts {
                    match part {
                        TextPatternConstantPart::Property(property) => {
                            if !attrs.is_empty() {
                                error!("A value was already read");
                                return Err(());
                            }

                            let type_info = self.ontology.get_type_info(property.type_def_id);
                            let processor = self.ontology.new_serde_processor(
                                type_info
                                    .operator_addr
                                    .expect("No operator addr for pattern constant part"),
                                ProcessorMode::Create,
                            );

                            let attribute = processor
                                .deserialize(StrDeserializer::<serde_json::Error>::new(str))
                                .map_err(|_| ())?;

                            attrs.insert(property.property_id, attribute);
                        }
                        TextPatternConstantPart::AnyString { .. } => {
                            let text_def_id = self.ontology.ontol_domain_meta.text;
                            let property_id = PropertyId {
                                role: Role::Subject,
                                relationship_id: RelationshipId(text_def_id),
                            };

                            attrs.insert(
                                property_id,
                                Attribute {
                                    rel: Value::unit(),
                                    val: Value::Text(str.into(), text_def_id),
                                },
                            );
                        }
                        TextPatternConstantPart::Literal(_) => {}
                    }
                }

                Ok(Value::Struct(Box::new(attrs), self.def_id))
            }
        }
    }
}

#[derive(Clone)]
pub struct SequenceMatcher<'on> {
    ranges: &'on [SequenceRange],
    range_cursor: usize,
    repetition_cursor: u16,

    pub type_def_id: DefId,
    pub ctx: SubProcessorContext,
}

impl<'on> ValueMatcher for SequenceMatcher<'on> {
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

impl<'on> SequenceMatcher<'on> {
    pub fn new(ranges: &'on [SequenceRange], type_def_id: DefId, ctx: SubProcessorContext) -> Self {
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
                        element_addr: range.addr,
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
                    element_addr: range.addr,
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

pub struct UnionMatcher<'on> {
    pub typename: &'on str,
    pub variants: &'on [SerdeUnionVariant],
    pub ctx: SubProcessorContext,
    pub ontology: &'on Ontology,
    pub mode: ProcessorMode,
    pub level: ProcessorLevel,
}

impl<'on> ValueMatcher for UnionMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            Backticks(self.typename),
            Missing {
                items: self
                    .variants
                    .iter()
                    .map(|discriminator| self
                        .ontology
                        .new_serde_processor(discriminator.addr, self.mode))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<Value, ()> {
        let def_id = self.match_leaf_discriminant(LeafDiscriminant::IsUnit)?;
        Ok(Value::Unit(def_id))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        for variant in self.variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminator.discriminant
            else {
                continue;
            };

            match leaf_discriminant {
                LeafDiscriminant::IsIntLiteral(literal) if value == *literal => {}
                LeafDiscriminant::IsInt => {}
                _ => continue,
            }
            return Ok(Value::I64(value, variant.discriminator.serde_def.def_id));
        }
        Err(())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        for variant in self.variants {
            let Discriminant::MatchesLeaf(scalar_discriminant) =
                &variant.discriminator.discriminant
            else {
                continue;
            };

            match scalar_discriminant {
                LeafDiscriminant::IsText => {
                    return try_deserialize_custom_string(
                        self.ontology,
                        variant.discriminator.serde_def.def_id,
                        str,
                    )
                    .map_err(|_| ())
                }
                LeafDiscriminant::IsTextLiteral(lit) if lit == str => {
                    return try_deserialize_custom_string(
                        self.ontology,
                        variant.discriminator.serde_def.def_id,
                        str,
                    )
                    .map_err(|_| ())
                }
                LeafDiscriminant::MatchesCapturingTextPattern(def_id) => {
                    let result_type = variant.discriminator.serde_def.def_id;
                    let pattern = self.ontology.text_patterns.get(def_id).unwrap();

                    if let Ok(value) = pattern.try_capturing_match(str, result_type, self.ontology)
                    {
                        return Ok(value);
                    }
                }
                _ => {}
            }
        }

        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceMatcher, ()> {
        for variant in self.variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminator.discriminant
            else {
                continue;
            };

            if leaf_discriminant == &LeafDiscriminant::IsSequence {
                match self.ontology.get_serde_operator(variant.addr) {
                    SerdeOperator::RelationSequence(seq_op) => {
                        return Ok(SequenceMatcher::new(
                            &seq_op.ranges,
                            seq_op.def.def_id,
                            self.ctx,
                        ))
                    }
                    SerdeOperator::ConstructorSequence(seq_op) => {
                        return Ok(SequenceMatcher::new(
                            &seq_op.ranges,
                            seq_op.def.def_id,
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
                Discriminant::StructFallback | Discriminant::HasAttribute(..)
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

impl<'on> UnionMatcher<'on> {
    fn match_leaf_discriminant(&self, discriminant: LeafDiscriminant) -> Result<DefId, ()> {
        for variant in self.variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminator.discriminant
            else {
                continue;
            };

            if leaf_discriminant == &discriminant {
                return Ok(variant.discriminator.serde_def.def_id);
            }
        }

        Err(())
    }
}

#[derive(Clone)]
pub struct MapMatcher<'on> {
    variants: &'on [SerdeUnionVariant],
    ontology: &'on Ontology,
    pub ctx: SubProcessorContext,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

pub enum MapMatchResult<'on> {
    Match(MapMatch<'on>),
    Indecisive(MapMatcher<'on>),
}

#[derive(Debug)]
pub struct MapMatch<'on> {
    pub kind: MapMatchKind<'on>,
    pub ctx: SubProcessorContext,
}

#[derive(Debug)]
pub enum MapMatchKind<'on> {
    StructType(&'on StructOperator),
    IdType(&'on str, SerdeOperatorAddr),
}

impl<'on> MapMatcher<'on> {
    pub fn match_attribute(
        self,
        property: &str,
        value: &serde_value::Value,
    ) -> MapMatchResult<'on> {
        trace!("match_attribute '{property}': {:#?}", self.variants);

        let match_fn = |discriminant: &Discriminant| -> bool {
            let Discriminant::HasAttribute(_, match_attr_name, scalar_discriminant) = discriminant
            else {
                return false;
            };

            if property != match_attr_name {
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
                (LeafDiscriminant::IsTextLiteral(lit), serde_value::Value::String(value)) => {
                    value == lit
                }
                (
                    LeafDiscriminant::MatchesCapturingTextPattern(def_id),
                    serde_value::Value::String(value),
                ) => {
                    let pattern = self.ontology.text_patterns.get(def_id).unwrap();
                    pattern.regex.is_match(value)
                }
                _ => false,
            }
        };

        let result = self
            .variants
            .iter()
            .find(|variant| match_fn(&variant.discriminator.discriminant))
            .map(
                |variant| match self.ontology.get_serde_operator(variant.addr) {
                    SerdeOperator::Struct(struct_op) => {
                        MapMatchResult::Match(self.new_match(MapMatchKind::StructType(struct_op)))
                    }
                    SerdeOperator::IdSingletonStruct(name, addr) => MapMatchResult::Match(
                        self.new_match(MapMatchKind::IdType(name.as_str(), *addr)),
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

    pub fn match_fallback(self) -> MapMatchResult<'on> {
        // debug!("match_fallback");

        for variant in self.variants {
            if matches!(
                variant.discriminator.discriminant,
                Discriminant::StructFallback
            ) {
                match self.ontology.get_serde_operator(variant.addr) {
                    SerdeOperator::Struct(struct_op) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::StructType(struct_op)),
                        )
                    }
                    SerdeOperator::IdSingletonStruct(name, addr) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::IdType(name.as_str(), *addr)),
                        )
                    }
                    other => panic!("Matched discriminator is not a map type: {other:?}"),
                }
            }
        }

        MapMatchResult::Indecisive(self)
    }

    fn new_match(&self, kind: MapMatchKind<'on>) -> MapMatch<'on> {
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
    match ontology.text_like_types.get(&def_id) {
        Some(custom_string_deserializer) => custom_string_deserializer.try_deserialize(def_id, str),
        None => Ok(Value::Text(str.into(), def_id)),
    }
}

fn expecting_custom_string(
    ontology: &Ontology,
    def_id: DefId,
    f: &mut std::fmt::Formatter,
) -> Option<std::fmt::Result> {
    ontology
        .text_like_types
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

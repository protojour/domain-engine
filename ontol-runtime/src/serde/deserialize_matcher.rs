use std::fmt::Display;

use tracing::debug;

use crate::{
    discriminator::Discriminant,
    env::Env,
    format_utils::{Backticks, LogicOp, Missing},
    string_pattern::StringPattern,
    string_types::ParseError,
    value::{Data, Value},
    DefId,
};

use super::{MapType, SequenceRange, SerdeOperator, SerdeOperatorId, ValueUnionType};

pub struct ExpectingMatching<'v>(pub &'v dyn ValueMatcher);

impl<'v> Display for ExpectingMatching<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.expecting(f)
    }
}

pub struct SeqElementMatch {
    pub element_operator_id: SerdeOperatorId,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
}

/// Trait for matching incoming types for deserialization
pub trait ValueMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;

    fn match_unit(&self) -> Result<Value, ()> {
        Err(())
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        Err(())
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
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

/// match any integer
pub struct IntMatcher(pub DefId);

impl ValueMatcher for IntMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "integer")
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        Ok(self.0)
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        Ok(self.0)
    }
}

/// match any number
pub struct NumberMatcher(pub DefId);

impl ValueMatcher for NumberMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "number")
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        Ok(self.0)
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        Ok(self.0)
    }
}

/// match any string
pub struct StringMatcher<'e> {
    pub def_id: DefId,
    pub env: &'e Env,
}

impl<'e> ValueMatcher for StringMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        expecting_custom_string(self.env, self.def_id, f)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        try_deserialize_custom_string(self.env, self.def_id, str).map_err(|_| ())
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
}

impl<'e> ValueMatcher for StringPatternMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if self.pattern.regex.is_match(str) {
            Ok(Value::new(Data::String(str.into()), self.def_id))
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
}

impl<'e> ValueMatcher for CapturingStringPatternMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        let data = self.pattern.try_capturing_match(str).map_err(|_| ())?;
        Ok(Value::new(data, self.def_id))
    }
}

#[derive(Clone)]
pub struct SequenceMatcher<'e> {
    ranges: &'e [SequenceRange],
    range_cursor: usize,
    repetition_cursor: u16,

    pub type_def_id: DefId,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
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
    pub fn new(
        ranges: &'e [SequenceRange],
        type_def_id: DefId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        Self {
            ranges,
            range_cursor: 0,
            repetition_cursor: 0,
            type_def_id,
            rel_params_operator_id,
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
                        rel_params_operator_id: self.rel_params_operator_id,
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
                    rel_params_operator_id: self.rel_params_operator_id,
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
    pub value_union_type: &'e ValueUnionType,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
    pub env: &'e Env,
}

impl<'e> ValueMatcher for UnionMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            Backticks(&self.value_union_type.typename),
            Missing {
                items: self
                    .value_union_type
                    .discriminators
                    .iter()
                    .map(|discriminator| self
                        .env
                        .new_serde_processor(discriminator.operator_id, None))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<Value, ()> {
        let def_id = self.match_discriminant(Discriminant::IsUnit)?;
        Ok(Value::new(Data::Unit, def_id))
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsInt)
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsInt)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        for discriminator in &self.value_union_type.discriminators {
            match &discriminator.discriminator.discriminant {
                Discriminant::IsString => {
                    return try_deserialize_custom_string(
                        self.env,
                        discriminator.discriminator.result_type,
                        str,
                    )
                    .map_err(|_| ())
                }
                Discriminant::IsStringLiteral(lit) if lit == str => {
                    return try_deserialize_custom_string(
                        self.env,
                        discriminator.discriminator.result_type,
                        str,
                    )
                    .map_err(|_| ())
                }
                Discriminant::MatchesCapturingStringPattern => {
                    let result_type = discriminator.discriminator.result_type;
                    let pattern = self.env.string_patterns.get(&result_type).unwrap();

                    if let Ok(data) = pattern.try_capturing_match(str) {
                        return Ok(Value::new(data, result_type));
                    }
                }
                _ => {}
            }
        }

        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceMatcher, ()> {
        for discriminator in &self.value_union_type.discriminators {
            if discriminator.discriminator.discriminant == Discriminant::IsSequence {
                let processor = self
                    .env
                    .new_serde_processor(discriminator.operator_id, None);

                match &processor.value_operator {
                    SerdeOperator::Sequence(ranges, def_id) => {
                        return Ok(SequenceMatcher::new(
                            ranges,
                            *def_id,
                            self.rel_params_operator_id,
                        ))
                    }
                    _ => panic!("not a sequence"),
                }
            }
        }

        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher, ()> {
        if !self
            .value_union_type
            .discriminators
            .iter()
            .any(|discriminator| {
                matches!(
                    &discriminator.discriminator.discriminant,
                    Discriminant::MapFallback
                        | Discriminant::HasProperty(_, _)
                        | Discriminant::HasStringAttribute(_, _, _)
                )
            })
        {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher {
            value_union_type: self.value_union_type,
            rel_params_operator_id: self.rel_params_operator_id,
            env: self.env,
        })
    }
}

impl<'e> UnionMatcher<'e> {
    fn match_discriminant(&self, discriminant: Discriminant) -> Result<DefId, ()> {
        for discriminator in &self.value_union_type.discriminators {
            if discriminator.discriminator.discriminant == discriminant {
                return Ok(discriminator.discriminator.result_type);
            }
        }

        Err(())
    }
}

#[derive(Clone)]
pub struct MapMatcher<'e> {
    value_union_type: &'e ValueUnionType,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
    env: &'e Env,
}

pub enum MapMatchResult<'e> {
    Match(MapMatch<'e>),
    Indecisive(MapMatcher<'e>),
}

#[derive(Debug)]
pub struct MapMatch<'e> {
    pub kind: MapMatchKind<'e>,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
}

#[derive(Debug)]
pub enum MapMatchKind<'e> {
    MapType(&'e MapType),
    IdType(SerdeOperatorId),
}

impl<'e> MapMatcher<'e> {
    pub fn match_attribute(self, property: &str, value: &serde_value::Value) -> MapMatchResult<'e> {
        debug!("match_attribute '{property}': {:?}", self.value_union_type);

        let match_fn = |discriminant: &Discriminant| -> bool {
            match (discriminant, value) {
                (
                    Discriminant::HasStringAttribute(_, match_name, match_value),
                    serde_value::Value::String(value),
                ) => property == match_name && value == match_value,
                (Discriminant::HasProperty(_, match_name), _) => property == match_name,
                _ => false,
            }
        };

        let result = self
            .value_union_type
            .discriminators
            .iter()
            .find(|discriminator| match_fn(&discriminator.discriminator.discriminant))
            .map(|discriminator| {
                match self
                    .env
                    .new_serde_processor(discriminator.operator_id, None)
                    .value_operator
                {
                    SerdeOperator::MapType(map_type) => {
                        MapMatchResult::Match(self.new_match(MapMatchKind::MapType(map_type)))
                    }
                    SerdeOperator::Id(operator_id) => {
                        MapMatchResult::Match(self.new_match(MapMatchKind::IdType(*operator_id)))
                    }
                    SerdeOperator::ValueUnionType(value_union_type) => MapMatcher {
                        value_union_type,
                        rel_params_operator_id: self.rel_params_operator_id,
                        env: self.env,
                    }
                    .match_attribute(property, value),
                    other => panic!("Matched discriminator is not a map type: {other:?}"),
                }
            });

        match result {
            None => MapMatchResult::Indecisive(self),
            Some(result) => result,
        }
    }

    pub fn match_fallback(self) -> MapMatchResult<'e> {
        debug!("match_fallback");

        for discriminator in &self.value_union_type.discriminators {
            if matches!(
                discriminator.discriminator.discriminant,
                Discriminant::MapFallback
            ) {
                match self
                    .env
                    .new_serde_processor(discriminator.operator_id, None)
                    .value_operator
                {
                    SerdeOperator::MapType(map_type) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::MapType(map_type)),
                        )
                    }
                    SerdeOperator::Id(operator_id) => {
                        return MapMatchResult::Match(
                            self.new_match(MapMatchKind::IdType(*operator_id)),
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
            rel_params_operator_id: self.rel_params_operator_id,
        }
    }
}

fn try_deserialize_custom_string(env: &Env, def_id: DefId, str: &str) -> Result<Value, ParseError> {
    match env.string_like_types.get(&def_id) {
        Some(custom_string_deserializer) => custom_string_deserializer.try_deserialize(def_id, str),
        None => Ok(Value::new(Data::String(str.into()), def_id)),
    }
}

fn expecting_custom_string(
    env: &Env,
    def_id: DefId,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    match env.string_like_types.get(&def_id) {
        Some(custom_string_deserializer) => {
            write!(f, "`{}`", custom_string_deserializer.type_name())
        }
        None => write!(f, "string"),
    }
}

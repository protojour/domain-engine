use std::fmt::Display;

use crate::{
    discriminator::Discriminant,
    env::Env,
    format_utils::{Backticks, LogicOp, Missing},
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
    pub edge_operator_id: Option<SerdeOperatorId>,
}

/// Trait for matching incoming types for deserialization
pub trait ValueMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;

    fn match_unit(&self) -> Result<DefId, ()> {
        Err(())
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        Err(())
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        Err(())
    }

    fn match_str(&self, _: &str) -> Result<DefId, ()> {
        Err(())
    }

    fn match_seq(&self) -> Result<DefId, ()> {
        Err(())
    }

    fn match_next_seq_element(&mut self) -> Option<SeqElementMatch> {
        None
    }

    fn match_seq_end(&self) -> Result<(), ()> {
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

    fn match_unit(&self) -> Result<DefId, ()> {
        Ok(DefId::unit())
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
pub struct StringMatcher(pub DefId);

impl ValueMatcher for StringMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string")
    }

    fn match_str(&self, _: &str) -> Result<DefId, ()> {
        Ok(self.0)
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

    fn match_str(&self, str: &str) -> Result<DefId, ()> {
        if str == self.literal {
            Ok(self.def_id)
        } else {
            Err(())
        }
    }
}

pub struct SequenceMatcher<'e> {
    pub ranges: &'e [SequenceRange],
    pub range_cursor: usize,
    pub repetition_cursor: u16,
    pub def_id: DefId,
    pub edge_operator_id: Option<SerdeOperatorId>,
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

    fn match_seq(&self) -> Result<DefId, ()> {
        Ok(self.def_id)
    }

    fn match_next_seq_element(&mut self) -> Option<SeqElementMatch> {
        loop {
            let range = &self.ranges[self.range_cursor];

            if let Some(finite_repetition) = range.finite_repetition {
                if self.repetition_cursor < finite_repetition {
                    self.repetition_cursor += 1;

                    return Some(SeqElementMatch {
                        element_operator_id: range.operator_id,
                        edge_operator_id: self.edge_operator_id,
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
                    edge_operator_id: self.edge_operator_id,
                });
            }
        }
    }

    fn match_seq_end(&self) -> Result<(), ()> {
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
    pub edge_operator_id: Option<SerdeOperatorId>,
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
                    .map(|discriminator| self.env.new_serde_processor(discriminator.operator_id))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsUnit)
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsInt)
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsInt)
    }

    fn match_str(&self, v: &str) -> Result<DefId, ()> {
        for discriminator in &self.value_union_type.discriminators {
            match &discriminator.discriminator.discriminant {
                Discriminant::IsString => {
                    return Ok(discriminator.discriminator.result_type);
                }
                Discriminant::IsStringLiteral(lit) if lit == v => {
                    return Ok(discriminator.discriminator.result_type);
                }
                _ => {}
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
                    Discriminant::HasProperty(_, _) | Discriminant::HasStringAttribute(_, _, _)
                )
            })
        {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher {
            value_union_type: self.value_union_type,
            edge_operator_id: self.edge_operator_id,
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

pub struct MapMatcher<'e> {
    value_union_type: &'e ValueUnionType,
    pub edge_operator_id: Option<SerdeOperatorId>,
    env: &'e Env,
}

impl<'e> MapMatcher<'e> {
    pub fn match_attribute(
        &self,
        property: &str,
        value: &serde_value::Value,
    ) -> Result<&'e MapType, MapMatchError> {
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

        self.value_union_type
            .discriminators
            .iter()
            .find(|discriminator| match_fn(&discriminator.discriminator.discriminant))
            .map(|discriminator| {
                match self
                    .env
                    .new_serde_processor(discriminator.operator_id)
                    .value_operator
                {
                    SerdeOperator::MapType(map_type) => map_type,
                    _ => panic!("Matched discriminator is not a map type"),
                }
            })
            .ok_or(MapMatchError::Indecisive)
    }
}

pub enum MapMatchError {
    Indecisive,
}

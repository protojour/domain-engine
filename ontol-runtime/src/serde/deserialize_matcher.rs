use std::fmt::Display;

use crate::{
    discriminator::Discriminant,
    env::Env,
    format_utils::{Backticks, LogicOp, Missing},
    DefId,
};

use super::{MapType, SerdeOperator, SerdeOperatorId, ValueUnionDiscriminator, ValueUnionType};

pub struct ExpectingMatching<'v>(pub &'v dyn ValueMatcher);

impl<'v> Display for ExpectingMatching<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.expecting(f)
    }
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

    fn match_seq_element(&self, _: usize) -> Option<SerdeOperatorId> {
        None
    }

    fn match_seq_end(&self, _: usize) -> Result<(), ()> {
        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher, ()> {
        Err(())
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

/// match a tuple
pub struct TupleMatcher<'e> {
    pub elements: &'e [SerdeOperatorId],
    pub def_id: DefId,
}

impl<'e> ValueMatcher for TupleMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "tuple with length {}", self.elements.len())
    }

    fn match_seq(&self) -> Result<DefId, ()> {
        Ok(self.def_id)
    }

    fn match_seq_element(&self, index: usize) -> Option<SerdeOperatorId> {
        if index < self.elements.len() {
            Some(self.elements[index])
        } else {
            None
        }
    }

    fn match_seq_end(&self, end: usize) -> Result<(), ()> {
        if end == self.elements.len() {
            Ok(())
        } else {
            Err(())
        }
    }
}

pub struct ArrayMatcher {
    pub element_def_id: DefId,
    pub element_operator_id: SerdeOperatorId,
}

impl ValueMatcher for ArrayMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "array")
    }

    fn match_seq(&self) -> Result<DefId, ()> {
        Ok(self.element_def_id)
    }

    fn match_seq_element(&self, _: usize) -> Option<SerdeOperatorId> {
        Some(self.element_operator_id)
    }

    fn match_seq_end(&self, _: usize) -> Result<(), ()> {
        Ok(())
    }
}

pub struct UnionMatcher<'e> {
    pub value_union_type: &'e ValueUnionType,
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
        if self
            .value_union_type
            .discriminators
            .iter()
            .find(
                |discriminator| match &discriminator.discriminator.discriminant {
                    Discriminant::HasProperty(_, _) => true,
                    Discriminant::HasStringAttribute(_, _, _) => true,
                    _ => false,
                },
            )
            .is_none()
        {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher {
            value_union_type: self.value_union_type,
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
    env: &'e Env,
}

impl<'e> MapMatcher<'e> {
    /// Match a property name without a value
    pub fn match_property(&self, property: &str) -> Result<&'e MapType, MapMatchError> {
        for discriminator in &self.value_union_type.discriminators {
            if let Discriminant::HasProperty(_, match_name) =
                &discriminator.discriminator.discriminant
            {
                if property == match_name {
                    return Ok(self.get_map_type(discriminator));
                }
            }
        }

        Err(MapMatchError::Indecisive)
    }

    /// Match a full attribute; property + value
    pub fn match_attribute(
        &self,
        property: &str,
        value: &serde_value::Value,
    ) -> Result<&'e MapType, MapMatchError> {
        for discriminator in &self.value_union_type.discriminators {
            match (&discriminator.discriminator.discriminant, value) {
                (
                    Discriminant::HasStringAttribute(_, match_name, match_value),
                    serde_value::Value::String(value),
                ) => {
                    if property == match_name && value == match_value {
                        return Ok(self.get_map_type(discriminator));
                    }
                }
                _ => {}
            }
        }

        Err(MapMatchError::Indecisive)
    }

    fn get_map_type(&self, discriminator: &ValueUnionDiscriminator) -> &'e MapType {
        match self
            .env
            .new_serde_processor(discriminator.operator_id)
            .current
        {
            SerdeOperator::MapType(map_type) => map_type,
            _ => panic!("Matched discriminator is not a map type"),
        }
    }
}

pub enum MapMatchError {
    Indecisive,
}

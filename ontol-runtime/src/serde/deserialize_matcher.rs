use std::fmt::Display;

use crate::{discriminator::Discriminant, DefId};

use super::{
    deserialize::{LogicOp, Missing},
    SerdeRegistry, ValueUnionType,
};

/// Trait for matching incoming types for deserialization
pub trait Matcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        Err(())
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        Err(())
    }

    fn match_str(&self, _: &str) -> Result<DefId, ()> {
        Err(())
    }
}

/// match any number
pub struct NumberMatcher(pub DefId);

impl Matcher for NumberMatcher {
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

impl Matcher for StringMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string")
    }

    fn match_str(&self, _: &str) -> Result<DefId, ()> {
        Ok(self.0)
    }
}

pub struct UnionMatcher<'e> {
    pub value_union_type: &'e ValueUnionType,
    pub registry: SerdeRegistry<'e>,
}

impl<'e> Matcher for UnionMatcher<'e> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let missing = Missing {
            items: self
                .value_union_type
                .discriminators
                .iter()
                .map(|discriminator| -> Box<dyn Display> {
                    Box::new(String::from(
                        self.registry
                            .make_processor(discriminator.operator_id)
                            .current
                            .typename(),
                    ))
                })
                .collect(),
            logic_op: LogicOp::Or,
        };

        write!(f, "{missing}")
    }

    fn match_u64(&self, _: u64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsNumber)
    }

    fn match_i64(&self, _: i64) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsNumber)
    }

    fn match_str(&self, _: &str) -> Result<DefId, ()> {
        self.match_discriminant(Discriminant::IsString)
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

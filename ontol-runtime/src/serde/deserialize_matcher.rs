use std::fmt::Display;

use crate::{discriminator::Discriminant, DefId};

use super::{
    deserialize::{LogicOp, Missing},
    MapType, SerdeOperator, SerdeRegistry, ValueUnionDiscriminator, ValueUnionType,
};

/// Trait for matching incoming types for deserialization
pub trait ValueMatcher {
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

    fn match_map(&self) -> Result<MapMatcher, ()> {
        Err(())
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

pub struct UnionMatcher<'e> {
    pub value_union_type: &'e ValueUnionType,
    pub registry: SerdeRegistry<'e>,
}

impl<'e> ValueMatcher for UnionMatcher<'e> {
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
            registry: self.registry,
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
    registry: SerdeRegistry<'e>,
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
            .registry
            .make_processor(discriminator.operator_id)
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

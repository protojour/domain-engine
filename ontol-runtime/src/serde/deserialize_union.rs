use std::fmt::Display;

use serde::de::Unexpected;
use smartstring::alias::String;

use crate::{
    discriminator::{Discriminant, VariantDiscriminator},
    value::Value,
    DefId,
};

use super::{
    deserialize::{LogicOp, Missing},
    SerdeRegistry, ValueUnionType,
};

pub(super) struct UnionVisitor<'e> {
    pub value_union_type: &'e ValueUnionType,
    pub registry: SerdeRegistry<'e>,
}

impl<'e, 'de> serde::de::Visitor<'de> for UnionVisitor<'e> {
    type Value = (Value, DefId);

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

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let discriminator = self
            .match_discriminator(Discriminant::IsNumber)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok((
            Value::Number(
                v.try_into()
                    .map_err(|_| serde::de::Error::custom(format!("u64 overflow")))?,
            ),
            discriminator.result_type,
        ))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let discriminator = self
            .match_discriminator(Discriminant::IsNumber)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok((Value::Number(v), discriminator.result_type))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let discriminator = self
            .match_discriminator(Discriminant::IsString)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Str(v), &self))?;

        Ok((Value::String(v.into()), discriminator.result_type))
    }
}

impl<'e> UnionVisitor<'e> {
    fn match_discriminator(&self, discriminant: Discriminant) -> Result<&VariantDiscriminator, ()> {
        for discriminator in &self.value_union_type.discriminators {
            if discriminator.discriminator.discriminant == discriminant {
                return Ok(&discriminator.discriminator);
            }
        }

        Err(())
    }
}

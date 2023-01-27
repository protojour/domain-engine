use std::{collections::HashMap, fmt::Display};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::Value;

use super::{MapType, SerdeOperator, SerdeProcessor, SerdeProperty, SerdeRegistry};

#[derive(Clone, Copy)]
struct PropertySet<'s>(&'s IndexMap<String, SerdeProperty>);

struct NumberVisitor;

struct StringVisitor;

struct MapTypeVisitor<'e> {
    map_type: &'e MapType,
    registry: SerdeRegistry<'e>,
}

impl<'e, 'de> serde::de::DeserializeSeed<'de> for SerdeProcessor<'e> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.current {
            SerdeOperator::Unit => {
                panic!("This should not be used");
            }
            SerdeOperator::Number => {
                serde::de::Deserializer::deserialize_i64(deserializer, NumberVisitor)
            }
            SerdeOperator::String => {
                serde::de::Deserializer::deserialize_str(deserializer, StringVisitor)
            }
            SerdeOperator::ValueType(value_type) => self
                .registry
                .make_processor(value_type.property.operator_id)
                .deserialize(deserializer),
            SerdeOperator::MapType(map_type) => serde::de::Deserializer::deserialize_map(
                deserializer,
                MapTypeVisitor {
                    map_type,
                    registry: self.registry,
                },
            ),
        }
    }
}

impl<'de> serde::de::Visitor<'de> for NumberVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "number")
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(v.into()))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(v.into()))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(v.into()))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(v.try_into().map_err(|_| {
            serde::de::Error::custom(format!("u64 overflow"))
        })?))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(v))
    }
}

impl<'de> serde::de::Visitor<'de> for StringVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(v.into()))
    }
}

impl<'e, 'de> serde::de::Visitor<'de> for MapTypeVisitor<'e> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.map_type.typename)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut attributes = HashMap::with_capacity(self.map_type.properties.len());

        while let Some(serde_property) =
            map.next_key_seed(PropertySet(&self.map_type.properties))?
        {
            let attribute_value =
                map.next_value_seed(self.registry.make_processor(serde_property.operator_id))?;

            attributes.insert(serde_property.property_id, attribute_value);
        }

        if attributes.len() < self.map_type.properties.len() {
            let missing_keys = Missing {
                items: self
                    .map_type
                    .properties
                    .iter()
                    .filter(|(_, property)| !attributes.contains_key(&property.property_id))
                    .map(|(key, _)| -> Box<dyn Display> { Box::new(key.clone()) })
                    .collect(),
                logic_op: LogicOp::And,
            };

            return Err(serde::de::Error::custom(format!(
                "missing properties, expected {missing_keys}"
            )));
        }

        Ok(Value::Compound(attributes))
    }
}

impl<'s, 'de> serde::de::DeserializeSeed<'de> for PropertySet<'s> {
    type Value = SerdeProperty;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::Deserializer::deserialize_str(deserializer, self)
    }
}

impl<'s, 'm, 'de> serde::de::Visitor<'de> for PropertySet<'s> {
    type Value = SerdeProperty;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match self.0.get(v) {
            Some(serde_property) => Ok(*serde_property),
            None => {
                // TODO: This error message could be improved to suggest valid fields.
                // see OneOf in serde (this is a private struct)
                Err(serde::de::Error::custom(format!(
                    "unknown property `{}`",
                    v
                )))
            }
        }
    }
}

struct Missing {
    items: Vec<Box<dyn Display>>,
    logic_op: LogicOp,
}

impl Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.items.as_slice() {
            [] => panic!("BUG: Nothing is missing!"),
            [single] => write!(f, "`{}`", single),
            [a, b] => write!(f, "`{}` {} `{}`", a, self.logic_op.name(), b),
            _ => {
                match self.logic_op {
                    LogicOp::And => write!(f, "all of ")?,
                }
                let mut iter = self.items.iter().peekable();
                while let Some(next) = iter.next() {
                    write!(f, "`{}`", next)?;
                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }

                Ok(())
            }
        }
    }
}

enum LogicOp {
    And,
}

impl LogicOp {
    fn name(&self) -> &'static str {
        match self {
            Self::And => "and",
        }
    }
}

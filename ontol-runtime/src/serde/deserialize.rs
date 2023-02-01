use std::{collections::HashMap, fmt::Display};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{value::Value, DefId};

use super::{
    deserialize_union::UnionVisitor, MapType, SerdeOperator, SerdeProcessor, SerdeProperty,
    SerdeRegistry,
};

#[derive(Clone, Copy)]
struct PropertySet<'s>(&'s IndexMap<String, SerdeProperty>);

struct NumberVisitor(DefId);

struct StringVisitor(DefId);

struct MapTypeVisitor<'e> {
    map_type: &'e MapType,
    registry: SerdeRegistry<'e>,
}

impl<'e, 'de> serde::de::DeserializeSeed<'de> for SerdeProcessor<'e> {
    type Value = (Value, DefId);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.current {
            SerdeOperator::Unit => {
                panic!("This should not be used");
            }
            SerdeOperator::Number(def_id) => {
                serde::de::Deserializer::deserialize_i64(deserializer, NumberVisitor(*def_id))
            }
            SerdeOperator::String(def_id) => {
                serde::de::Deserializer::deserialize_str(deserializer, StringVisitor(*def_id))
            }
            SerdeOperator::ValueType(value_type) => {
                let (value, _value_def_id) = self
                    .registry
                    .make_processor(value_type.property.operator_id)
                    .deserialize(deserializer)?;

                Ok((value, value_type.type_def_id))
            }
            SerdeOperator::ValueUnionType(value_union_type) => {
                serde::de::Deserializer::deserialize_any(
                    deserializer,
                    UnionVisitor {
                        value_union_type,
                        registry: self.registry,
                    },
                )
            }
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
    type Value = (Value, DefId);

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "number")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok((
            Value::Number(
                v.try_into()
                    .map_err(|_| serde::de::Error::custom(format!("u64 overflow")))?,
            ),
            self.0,
        ))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok((Value::Number(v), self.0))
    }
}

impl<'de> serde::de::Visitor<'de> for StringVisitor {
    type Value = (Value, DefId);

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok((Value::String(v.into()), self.0))
    }
}

impl<'e, 'de> serde::de::Visitor<'de> for MapTypeVisitor<'e> {
    type Value = (Value, DefId);

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
            let (attribute_value, _def_id) =
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

        Ok((Value::Compound(attributes), self.map_type.type_def_id))
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

pub(super) struct Missing {
    pub items: Vec<Box<dyn Display>>,
    pub logic_op: LogicOp,
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
                    LogicOp::Or => write!(f, "any of ")?,
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

pub enum LogicOp {
    And,
    Or,
}

impl LogicOp {
    fn name(&self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
        }
    }
}

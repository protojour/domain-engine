use std::{collections::HashMap, fmt::Display};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{binding::Bindings, Value};

use super::{MapType, SerdeOperator, SerdeOperatorKind, SerdeProperty};

#[derive(Clone, Copy)]
struct PropertySet<'s, 'm>(&'s IndexMap<String, SerdeProperty<'m>>);

struct NumberVisitor;

struct StringVisitor;

struct MapTypeVisitor<'e, 'm> {
    map_type: &'m MapType<'m>,
    bindings: &'e Bindings<'m>,
}

impl<'e, 'm, 'de> serde::de::DeserializeSeed<'de> for SerdeOperator<'e, 'm> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.kind {
            SerdeOperatorKind::Number => {
                serde::de::Deserializer::deserialize_i64(deserializer, NumberVisitor)
            }
            SerdeOperatorKind::String => {
                serde::de::Deserializer::deserialize_str(deserializer, StringVisitor)
            }
            SerdeOperatorKind::ValueType(value_type) => SerdeOperator {
                kind: value_type.property.kind,
                bindings: self.bindings,
            }
            .deserialize(deserializer),
            SerdeOperatorKind::MapType(map_type) => serde::de::Deserializer::deserialize_map(
                deserializer,
                MapTypeVisitor {
                    map_type,
                    bindings: self.bindings,
                },
            ),
            SerdeOperatorKind::Recursive(def_id) => {
                let kind = match self.bindings.serde_operator_kinds.get(&def_id) {
                    Some(Some(kind)) => kind,
                    _ => panic!("Could not resolve recursive serde operator"),
                };
                SerdeOperator {
                    kind,
                    bindings: self.bindings,
                }
                .deserialize(deserializer)
            }
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

impl<'e, 'm, 'de> serde::de::Visitor<'de> for MapTypeVisitor<'e, 'm> {
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
            let attribute_value = map.next_value_seed(SerdeOperator {
                kind: serde_property.kind,
                bindings: self.bindings,
            })?;

            attributes.insert(serde_property.property_id, attribute_value);
        }

        if attributes.len() < self.map_type.properties.len() {
            let missing_keys = OneOf(
                self.map_type
                    .properties
                    .iter()
                    .filter(|(_, property)| !attributes.contains_key(&property.property_id))
                    .map(|(key, _)| -> Box<dyn Display> { Box::new(key.clone()) })
                    .collect(),
            );

            return Err(serde::de::Error::custom(format!(
                "missing properties, expected {missing_keys}"
            )));
        }

        Ok(Value::Compound(attributes))
    }
}

impl<'s, 'm, 'de> serde::de::DeserializeSeed<'de> for PropertySet<'s, 'm> {
    type Value = SerdeProperty<'m>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::Deserializer::deserialize_str(deserializer, self)
    }
}

impl<'s, 'm, 'de> serde::de::Visitor<'de> for PropertySet<'s, 'm> {
    type Value = SerdeProperty<'m>;

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

struct OneOf(Vec<Box<dyn Display>>);

impl Display for OneOf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.len() {
            0 => panic!("OneOf empty"),
            1 => write!(f, "`{}`", self.0[0]),
            2 => write!(f, "`{}` or `{}`", self.0[0], self.0[1]),
            _ => {
                write!(f, "one of ")?;
                let mut iter = self.0.iter().peekable();
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

use std::{collections::HashMap, fmt::Debug};

use smartstring::alias::String;

use crate::{relation::PropertyId, Value};

#[derive(Clone, Copy, Debug)]
pub struct SerdeOperator<'m>(pub(crate) &'m SerdeOperatorKind<'m>);

#[derive(Debug)]
pub enum SerdeOperatorKind<'m> {
    Number,
    String,
    // A type with just one anonymous property
    ValueType(ValueType<'m>),
    // A type with
    MapType(MapType<'m>),
}

#[derive(Debug)]
pub struct ValueType<'m> {
    pub typename: String,
    pub property: SerdeProperty<'m>,
}

#[derive(Debug)]
pub struct MapType<'m> {
    pub typename: String,
    pub properties: HashMap<String, SerdeProperty<'m>>,
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeProperty<'m> {
    pub property_id: PropertyId,
    pub operator: SerdeOperator<'m>,
}

#[derive(Clone, Copy)]
struct PropertySet<'s, 'm>(&'s HashMap<String, SerdeProperty<'m>>);

struct NumberVisitor;

/// Visitor accepting all strings
struct StringVisitor;

struct MapTypeVisitor<'m>(&'m MapType<'m>);

impl<'m, 'de> serde::de::DeserializeSeed<'de> for SerdeOperator<'m> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.0 {
            SerdeOperatorKind::Number => {
                serde::de::Deserializer::deserialize_i64(deserializer, NumberVisitor)
            }
            SerdeOperatorKind::String => {
                serde::de::Deserializer::deserialize_str(deserializer, StringVisitor)
            }
            SerdeOperatorKind::ValueType(value_type) => {
                value_type.property.operator.deserialize(deserializer)
            }
            SerdeOperatorKind::MapType(map_type) => {
                serde::de::Deserializer::deserialize_map(deserializer, MapTypeVisitor(map_type))
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

impl<'m, 'de> serde::de::Visitor<'de> for MapTypeVisitor<'m> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.0.typename)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut attributes = HashMap::with_capacity(self.0.properties.len());

        while let Some(serde_property) = map.next_key_seed(PropertySet(&self.0.properties))? {
            let attribute_value = map.next_value_seed(serde_property.operator)?;

            attributes.insert(serde_property.property_id, attribute_value);
        }

        if attributes.len() < self.0.properties.len() {
            let missing_keys = self
                .0
                .properties
                .iter()
                .filter(|(_, property)| attributes.contains_key(&property.property_id))
                .map(|(key, _)| key)
                .collect::<Vec<_>>();

            return Err(serde::de::Error::custom(if missing_keys.is_empty() {
                format!("missing properties `{missing_keys:?}`")
            } else {
                panic!("BUG: No missing properties")
            }));
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

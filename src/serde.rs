use std::collections::HashMap;

use smartstring::alias::String;

use crate::{relation::PropertyId, Value};

pub struct Serder<'e>(&'e SerdeOperator<'e>);

enum SerdeOperator<'e> {
    Number,
    String,
    // A type with just one anonymous property
    ValueType(ValueType<'e>),
    // A type with
    MapType(MapType<'e>),
}

struct ValueType<'e> {
    pub typename: String,
    pub property: SerdeProperty<'e>,
}

struct MapType<'e> {
    pub typename: String,
    pub properties: HashMap<&'e str, SerdeProperty<'e>>,
}

#[derive(Clone, Copy)]
struct PropertySet<'s, 'e>(&'s HashMap<&'e str, SerdeProperty<'e>>);

#[derive(Clone, Copy)]
struct SerdeProperty<'e> {
    property_id: PropertyId,
    operator: &'e SerdeOperator<'e>,
}

struct NumberVisitor;

/// Visitor accepting all strings
struct StringVisitor;

struct MapTypeVisitor<'e>(&'e MapType<'e>);

impl<'e, 'de> serde::de::DeserializeSeed<'de> for Serder<'e> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.0 {
            SerdeOperator::Number => {
                serde::de::Deserializer::deserialize_i64(deserializer, NumberVisitor)
            }
            SerdeOperator::String => {
                serde::de::Deserializer::deserialize_str(deserializer, StringVisitor)
            }
            SerdeOperator::ValueType(value_type) => {
                Serder(value_type.property.operator).deserialize(deserializer)
            }
            SerdeOperator::MapType(map_type) => {
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
        write!(f, "type {}", self.0.typename)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut attributes = HashMap::with_capacity(self.0.properties.len());

        while let Some(serde_property) = map.next_key_seed(PropertySet(&self.0.properties))? {
            let attribute_value = map.next_value_seed(Serder(&serde_property.operator))?;

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

impl<'s, 'e, 'de> serde::de::DeserializeSeed<'de> for PropertySet<'s, 'e> {
    type Value = SerdeProperty<'e>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::Deserializer::deserialize_str(deserializer, self)
    }
}

impl<'s, 'e, 'de> serde::de::Visitor<'de> for PropertySet<'s, 'e> {
    type Value = SerdeProperty<'e>;

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

use std::{collections::HashMap, fmt::Display};

use indexmap::IndexMap;
use serde::de::{DeserializeSeed, Unexpected};
use smartstring::alias::String;

use crate::{value::Value, DefId};

use super::{
    deserialize_matcher::{
        ConstantStringMatcher, MapMatchError, NumberMatcher, StringMatcher, UnionMatcher,
        ValueMatcher,
    },
    MapType, SerdeOperator, SerdeProcessor, SerdeProperty, SerdeRegistry,
};

#[derive(Clone, Copy)]
struct PropertySet<'s>(&'s IndexMap<String, SerdeProperty>);

struct MatcherVisitor<'e, M> {
    matcher: M,
    registry: SerdeRegistry<'e>,
}

struct MapTypeVisitor<'e> {
    tmp_values: IndexMap<String, serde_value::Value>,
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
            SerdeOperator::Number(def_id) => serde::de::Deserializer::deserialize_i64(
                deserializer,
                MatcherVisitor {
                    matcher: NumberMatcher(*def_id),
                    registry: self.registry,
                },
            ),
            SerdeOperator::String(def_id) => serde::de::Deserializer::deserialize_str(
                deserializer,
                MatcherVisitor {
                    matcher: StringMatcher(*def_id),
                    registry: self.registry,
                },
            ),
            SerdeOperator::StringConstant(lit, def_id) => serde::de::Deserializer::deserialize_str(
                deserializer,
                MatcherVisitor {
                    matcher: ConstantStringMatcher {
                        literal: &lit,
                        def_id: *def_id,
                    },
                    registry: self.registry,
                },
            ),
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
                    MatcherVisitor {
                        matcher: UnionMatcher {
                            value_union_type,
                            registry: self.registry,
                        },
                        registry: self.registry,
                    },
                )
            }
            SerdeOperator::MapType(map_type) => serde::de::Deserializer::deserialize_map(
                deserializer,
                MapTypeVisitor {
                    tmp_values: IndexMap::new(),
                    map_type,
                    registry: self.registry,
                },
            ),
        }
    }
}

impl<'e, 'de, M: ValueMatcher> serde::de::Visitor<'de> for MatcherVisitor<'e, M> {
    type Value = (Value, DefId);

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.matcher.expecting(f)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let def_id = self
            .matcher
            .match_u64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok((
            Value::Number(
                v.try_into()
                    .map_err(|_| serde::de::Error::custom(format!("u64 overflow")))?,
            ),
            def_id,
        ))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let def_id = self
            .matcher
            .match_i64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok((Value::Number(v), def_id))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let def_id = self
            .matcher
            .match_str(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Str(v), &self))?;

        Ok((Value::String(v.into()), def_id))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let map_matcher = self
            .matcher
            .match_map()
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Map, &self))?;

        // Because serde is stream-oriented, we need to start storing temporary values
        // until we can recognize the type
        let mut tmp_values: IndexMap<String, serde_value::Value> = Default::default();

        while let Some(key) = map.next_key::<String>()? {
            match map_matcher.match_property(&key) {
                Ok(map_type) => {
                    let value: serde_value::Value = map.next_value()?;
                    tmp_values.insert(key, value);

                    // delegate to actual map type visitor
                    return MapTypeVisitor {
                        tmp_values,
                        map_type,
                        registry: self.registry,
                    }
                    .visit_map(map);
                }
                Err(MapMatchError::Indecisive) => {}
            }

            let value: serde_value::Value = map.next_value()?;

            match map_matcher.match_attribute(&key, &value) {
                Ok(map_type) => {
                    tmp_values.insert(key, value);

                    // delegate to actual map type visitor
                    return MapTypeVisitor {
                        tmp_values,
                        map_type,
                        registry: self.registry,
                    }
                    .visit_map(map);
                }
                Err(MapMatchError::Indecisive) => {
                    tmp_values.insert(key, value);
                }
            }
        }

        // FIXME: better error message?
        Err(serde::de::Error::custom(format!("invalid type")))
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

        // first parse tmp values, if any
        for (key, value) in self.tmp_values {
            let serde_property = PropertySet(&self.map_type.properties).visit_str(&key)?;
            let value_deserializer: serde_value::ValueDeserializer<A::Error> =
                serde_value::ValueDeserializer::new(value);

            let (attribute_value, _def_id) = self
                .registry
                .make_processor(serde_property.operator_id)
                .deserialize(value_deserializer)?;

            attributes.insert(serde_property.property_id, attribute_value);
        }

        // parse rest of map
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

impl<'s, 'de> serde::de::Visitor<'de> for PropertySet<'s> {
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

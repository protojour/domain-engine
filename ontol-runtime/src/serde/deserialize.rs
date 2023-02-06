use std::collections::HashMap;

use indexmap::IndexMap;
use serde::de::{DeserializeSeed, Unexpected};
use smartstring::alias::String;
use tracing::warn;

use crate::{
    env::Env,
    format_utils::{DoubleQuote, LogicOp, Missing},
    value::{Data, Value},
};

use super::{
    deserialize_matcher::{
        ArrayMatcher, ConstantStringMatcher, ExpectingMatching, IntMatcher, RangeArrayMatcher,
        StringMatcher, TupleMatcher, UnionMatcher, ValueMatcher,
    },
    MapType, SerdeOperator, SerdeProcessor, SerdeProperty,
};

#[derive(Clone, Copy)]
struct PropertySet<'s>(&'s IndexMap<String, SerdeProperty>);

struct MatcherVisitor<'e, M> {
    matcher: M,
    env: &'e Env,
}

struct MapTypeVisitor<'e> {
    buffered_attrs: Vec<(String, serde_value::Value)>,
    map_type: &'e MapType,
    env: &'e Env,
}

impl<'e, 'de> serde::de::DeserializeSeed<'de> for SerdeProcessor<'e> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.operator {
            SerdeOperator::Unit => {
                panic!("This should not be used");
            }
            SerdeOperator::Int(def_id) => serde::de::Deserializer::deserialize_i64(
                deserializer,
                MatcherVisitor {
                    matcher: IntMatcher(*def_id),
                    env: self.env,
                },
            ),
            SerdeOperator::Number(def_id) => serde::de::Deserializer::deserialize_i64(
                deserializer,
                MatcherVisitor {
                    matcher: IntMatcher(*def_id),
                    env: self.env,
                },
            ),
            SerdeOperator::String(def_id) => serde::de::Deserializer::deserialize_str(
                deserializer,
                MatcherVisitor {
                    matcher: StringMatcher(*def_id),
                    env: self.env,
                },
            ),
            SerdeOperator::StringConstant(lit, def_id) => serde::de::Deserializer::deserialize_str(
                deserializer,
                MatcherVisitor {
                    matcher: ConstantStringMatcher {
                        literal: &lit,
                        def_id: *def_id,
                    },
                    env: self.env,
                },
            ),
            SerdeOperator::Tuple(elems, def_id) => serde::de::Deserializer::deserialize_seq(
                deserializer,
                MatcherVisitor {
                    matcher: TupleMatcher {
                        elements: elems,
                        def_id: *def_id,
                    },
                    env: self.env,
                },
            ),
            SerdeOperator::Array(element_def_id, element_operator_id) => {
                serde::de::Deserializer::deserialize_seq(
                    deserializer,
                    MatcherVisitor {
                        matcher: ArrayMatcher {
                            element_def_id: *element_def_id,
                            element_operator_id: *element_operator_id,
                        },
                        env: self.env,
                    },
                )
            }
            SerdeOperator::RangeArray(element_def_id, range, element_operator_id) => {
                serde::de::Deserializer::deserialize_seq(
                    deserializer,
                    MatcherVisitor {
                        matcher: RangeArrayMatcher {
                            element_def_id: *element_def_id,
                            range: range.clone(),
                            element_operator_id: *element_operator_id,
                        },
                        env: self.env,
                    },
                )
            }
            SerdeOperator::ValueType(value_type) => {
                let typed_value = self
                    .env
                    .new_serde_processor(value_type.inner_operator_id)
                    .deserialize(deserializer)?;

                Ok(typed_value)
            }
            SerdeOperator::ValueUnionType(value_union_type) => {
                serde::de::Deserializer::deserialize_any(
                    deserializer,
                    MatcherVisitor {
                        matcher: UnionMatcher {
                            value_union_type,
                            env: self.env,
                        },
                        env: self.env,
                    },
                )
            }
            SerdeOperator::MapType(map_type) => serde::de::Deserializer::deserialize_map(
                deserializer,
                MapTypeVisitor {
                    buffered_attrs: Default::default(),
                    map_type,
                    env: self.env,
                },
            ),
        }
    }
}

impl<'e, 'de, M: ValueMatcher> serde::de::Visitor<'de> for MatcherVisitor<'e, M> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.matcher.expecting(f)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_unit()
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Unit, &self))?;

        Ok(Value {
            data: Data::Unit,
            type_def_id,
        })
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_u64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok(Value {
            data: Data::Int(
                v.try_into()
                    .map_err(|_| serde::de::Error::custom(format!("u64 overflow")))?,
            ),
            type_def_id,
        })
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_i64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok(Value {
            data: Data::Int(v),
            type_def_id,
        })
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_str(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Str(v), &self))?;

        Ok(Value {
            data: Data::String(v.into()),
            type_def_id,
        })
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let type_def_id = self
            .matcher
            .match_seq()
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Seq, &self))?;

        let mut index = 0;
        let mut output = vec![];

        loop {
            let processor = match self.matcher.match_seq_element(index) {
                Some(operator_id) => self.env.new_serde_processor(operator_id),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(Value {
                        data: Data::Vec(output),
                        type_def_id,
                    });
                }
            };

            match seq.next_element_seed(processor)? {
                Some(typed_value) => {
                    output.push(typed_value);
                    index += 1;
                }
                None => {
                    return match self.matcher.match_seq_end(index) {
                        Ok(_) => Ok(Value {
                            data: Data::Vec(output),
                            type_def_id,
                        }),
                        Err(_) => Err(serde::de::Error::invalid_length(index, &self)),
                    }
                }
            }
        }
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let map_matcher = self
            .matcher
            .match_map()
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Map, &self))?;

        // Because serde is stream-oriented, we need to start storing attributes
        // into a buffer until the type can be properly recognized
        let mut buffered_attrs: Vec<(String, serde_value::Value)> = Default::default();

        let map_type = loop {
            let property = map.next_key::<String>()?.ok_or_else(|| {
                // key/property was None, i.e. the whole map was read
                // without being able to recognize the type
                serde::de::Error::custom(format!(
                    "invalid map value, expected {}",
                    ExpectingMatching(&self.matcher)
                ))
            })?;

            let value: serde_value::Value = map.next_value()?;

            if let Ok(map_type) = map_matcher.match_attribute(&property, &value) {
                buffered_attrs.push((property, value));
                break map_type;
            }

            buffered_attrs.push((property, value));
        };

        // delegate to the real map visitor
        MapTypeVisitor {
            buffered_attrs,
            map_type,
            env: self.env,
        }
        .visit_map(map)
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

        // first parse buffered attributes, if any
        for (serde_key, serde_value) in self.buffered_attrs {
            let serde_property = PropertySet(&self.map_type.properties).visit_str(&serde_key)?;
            let typed_value = self
                .env
                .new_serde_processor(serde_property.operator_id)
                .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

            attributes.insert(serde_property.relation_id, typed_value);
        }

        // parse rest of map
        while let Some(serde_property) =
            map.next_key_seed(PropertySet(&self.map_type.properties))?
        {
            let typed_value =
                map.next_value_seed(self.env.new_serde_processor(serde_property.operator_id))?;

            attributes.insert(serde_property.relation_id, typed_value);
        }

        if attributes.len() < self.map_type.properties.len() {
            warn!("Missing attributes: {attributes:?}");

            let missing_keys = Missing {
                items: self
                    .map_type
                    .properties
                    .iter()
                    .filter(|(_, property)| !attributes.contains_key(&property.relation_id))
                    .map(|(key, _)| DoubleQuote(key))
                    .collect(),
                logic_op: LogicOp::And,
            };

            return Err(serde::de::Error::custom(format!(
                "missing properties, expected {missing_keys}"
            )));
        }

        Ok(Value {
            data: Data::Map(attributes),
            type_def_id: self.map_type.type_def_id,
        })
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

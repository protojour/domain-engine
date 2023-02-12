use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::de::{DeserializeSeed, Unexpected};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    env::Env,
    format_utils::{DoubleQuote, LogicOp, Missing},
    serde::EDGE_PROPERTY,
    value::{Attribute, Data, Value},
    DefId,
};

use super::{
    deserialize_matcher::{
        ConstantStringMatcher, ExpectingMatching, IntMatcher, SequenceMatcher, StringMatcher,
        UnionMatcher, UnitMatcher, ValueMatcher,
    },
    MapType, SerdeOperator, SerdeOperatorId, SerdeProcessor, SerdeProperty,
};

enum MapKey {
    Property(SerdeProperty),
    RelParams(SerdeOperatorId),
}

struct MatcherVisitor<'e, M> {
    matcher: M,
    env: &'e Env,
}

struct MapTypeVisitor<'e> {
    buffered_attrs: Vec<(String, serde_value::Value)>,
    map_type: &'e MapType,
    rel_params_operator_id: Option<SerdeOperatorId>,
    env: &'e Env,
}

#[derive(Clone, Copy)]
struct PropertySet<'s> {
    properties: &'s IndexMap<String, SerdeProperty>,
    rel_params_operator_id: Option<SerdeOperatorId>,
}

impl<'s> PropertySet<'s> {
    fn new(
        properties: &'s IndexMap<String, SerdeProperty>,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        Self {
            properties,
            rel_params_operator_id,
        }
    }
}

impl<'e> SerdeProcessor<'e> {
    fn matcher_visitor_no_params<M: ValueMatcher>(self, matcher: M) -> MatcherVisitor<'e, M> {
        assert!(
            self.rel_params_operator_id.is_none(),
            "rel_params_operator_id should be None for {:?}",
            self.value_operator
        );

        MatcherVisitor {
            matcher,
            env: self.env,
        }
    }
}

/// The serde implementation deserializes Attributes instead of Values.
///
/// The reason for this is parameterized relationships.
///
/// In JSON, the parameters of a parameterized relationships are _inlined_
/// inside the attribute value, with the special map key `_edge`.
///
/// This is also the reason that only map types may be related through parameterized relationships.
/// Other types only support unparameterized relationships.
impl<'e, 'de> serde::de::DeserializeSeed<'de> for SerdeProcessor<'e> {
    type Value = Attribute;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.value_operator {
            SerdeOperator::Unit => serde::de::Deserializer::deserialize_unit(
                deserializer,
                self.matcher_visitor_no_params(UnitMatcher),
            ),
            SerdeOperator::Int(def_id) => serde::de::Deserializer::deserialize_i64(
                deserializer,
                self.matcher_visitor_no_params(IntMatcher(*def_id)),
            ),
            SerdeOperator::Number(def_id) => serde::de::Deserializer::deserialize_i64(
                deserializer,
                self.matcher_visitor_no_params(IntMatcher(*def_id)),
            ),
            SerdeOperator::String(def_id) => serde::de::Deserializer::deserialize_str(
                deserializer,
                self.matcher_visitor_no_params(StringMatcher(*def_id)),
            ),
            SerdeOperator::StringConstant(literal, def_id) => {
                serde::de::Deserializer::deserialize_str(
                    deserializer,
                    self.matcher_visitor_no_params(ConstantStringMatcher {
                        literal,
                        def_id: *def_id,
                    }),
                )
            }
            SerdeOperator::Sequence(ranges, def_id) => serde::de::Deserializer::deserialize_seq(
                deserializer,
                MatcherVisitor {
                    matcher: SequenceMatcher {
                        ranges,
                        range_cursor: 0,
                        repetition_cursor: 0,
                        def_id: *def_id,
                        edge_operator_id: self.rel_params_operator_id,
                    },
                    env: self.env,
                },
            ),
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
                            rel_params_operator_id: self.rel_params_operator_id,
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
                    rel_params_operator_id: self.rel_params_operator_id,
                    env: self.env,
                },
            ),
        }
    }
}

impl<'e, 'de, M: ValueMatcher> serde::de::Visitor<'de> for MatcherVisitor<'e, M> {
    type Value = Attribute;

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

        Ok(Attribute::with_unit_params(Value {
            data: Data::Unit,
            type_def_id,
        }))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_u64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok(Attribute::with_unit_params(Value {
            data: Data::Int(
                v.try_into()
                    .map_err(|_| serde::de::Error::custom("u64 overflow".to_string()))?,
            ),
            type_def_id,
        }))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_i64(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok(Attribute::with_unit_params(Value {
            data: Data::Int(v),
            type_def_id,
        }))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let type_def_id = self
            .matcher
            .match_str(v)
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Str(v), &self))?;

        Ok(Attribute::with_unit_params(Value {
            data: Data::String(v.into()),
            type_def_id,
        }))
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let seq_type_def_id = self
            .matcher
            .match_seq()
            .map_err(|_| serde::de::Error::invalid_type(Unexpected::Seq, &self))?;

        let mut len = 0;
        let mut output = vec![];

        loop {
            let processor = match self.matcher.match_next_seq_element() {
                Some(element_match) => self.env.new_serde_processor_parameterized(
                    element_match.element_operator_id,
                    element_match.rel_params_operator_id,
                ),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(Attribute::with_unit_params(Value {
                        data: Data::Sequence(output),
                        type_def_id: seq_type_def_id,
                    }));
                }
            };

            match seq.next_element_seed(processor)? {
                Some(attribute) => {
                    output.push(attribute);
                    len += 1;
                }
                None => {
                    return match self.matcher.match_seq_end() {
                        Ok(_) => Ok(Attribute::with_unit_params(Value {
                            data: Data::Sequence(output),
                            type_def_id: seq_type_def_id,
                        })),
                        Err(_) => Err(serde::de::Error::invalid_length(len, &self)),
                    };
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
            rel_params_operator_id: map_matcher.edge_operator_id,
            env: self.env,
        }
        .visit_map(map)
    }
}

impl<'e, 'de> serde::de::Visitor<'de> for MapTypeVisitor<'e> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.map_type.typename)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut attributes = BTreeMap::new();
        let mut rel_params = Value::unit();

        let mut n_mandatory_properties = 0;

        // first parse buffered attributes, if any
        for (serde_key, serde_value) in self.buffered_attrs {
            match PropertySet::new(&self.map_type.properties, self.rel_params_operator_id)
                .visit_str(&serde_key)?
            {
                MapKey::RelParams(operator_id) => {
                    let Attribute { value, .. } = self
                        .env
                        .new_serde_processor(operator_id)
                        .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                    rel_params = value;
                }
                MapKey::Property(serde_property) => {
                    let Attribute { rel_params, value } = self
                        .env
                        .new_serde_processor_parameterized(
                            serde_property.value_operator_id,
                            serde_property.rel_params_operator_id,
                        )
                        .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                    if !serde_property.optional {
                        n_mandatory_properties += 1;
                    }

                    attributes.insert(serde_property.property_id, Attribute { rel_params, value });
                }
            }
        }

        // parse rest of map
        while let Some(map_key) = map.next_key_seed(PropertySet::new(
            &self.map_type.properties,
            self.rel_params_operator_id,
        ))? {
            match map_key {
                MapKey::RelParams(operator_id) => {
                    let Attribute { value, .. } =
                        map.next_value_seed(self.env.new_serde_processor(operator_id))?;

                    rel_params = value;
                }
                MapKey::Property(serde_property) => {
                    let attribute =
                        map.next_value_seed(self.env.new_serde_processor_parameterized(
                            serde_property.value_operator_id,
                            serde_property.rel_params_operator_id,
                        ))?;

                    if !serde_property.optional {
                        n_mandatory_properties += 1;
                    }

                    attributes.insert(serde_property.property_id, attribute);
                }
            }
        }

        if n_mandatory_properties < self.map_type.n_mandatory_properties
            || (rel_params.is_unit() != self.rel_params_operator_id.is_none())
        {
            debug!(
                "Missing attributes. Rel params match: {}, {}",
                rel_params.is_unit(),
                self.rel_params_operator_id.is_none()
            );
            for attr in &attributes {
                debug!("    attr {:?}", attr.0);
            }
            for prop in &self.map_type.properties {
                debug!(
                    "    prop {:?} '{}' optional={}",
                    prop.1.property_id, prop.0, prop.1.optional
                );
            }

            let mut items: Vec<DoubleQuote<String>> = self
                .map_type
                .properties
                .iter()
                .filter(|(_, property)| {
                    !property.optional && !attributes.contains_key(&property.property_id)
                })
                .map(|(key, _)| DoubleQuote(key.clone()))
                .collect();

            if self.rel_params_operator_id.is_some() && rel_params.type_def_id == DefId::unit() {
                items.push(DoubleQuote(EDGE_PROPERTY.into()));
            }

            debug!("items len: {}", items.len());

            let missing_keys = Missing {
                items,
                logic_op: LogicOp::And,
            };

            return Err(serde::de::Error::custom(format!(
                "missing properties, expected {missing_keys}"
            )));
        }

        Ok(Attribute {
            value: Value {
                data: Data::Map(attributes),
                type_def_id: self.map_type.type_def_id,
            },
            rel_params,
        })
    }
}

impl<'s, 'de> serde::de::DeserializeSeed<'de> for PropertySet<'s> {
    type Value = MapKey;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::Deserializer::deserialize_str(deserializer, self)
    }
}

impl<'s, 'de> serde::de::Visitor<'de> for PropertySet<'s> {
    type Value = MapKey;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v {
            EDGE_PROPERTY => {
                if let Some(operator_id) = self.rel_params_operator_id {
                    Ok(MapKey::RelParams(operator_id))
                } else {
                    Err(serde::de::Error::custom(
                        "`_edge` property not accepted here",
                    ))
                }
            }
            _ => {
                match self.properties.get(v) {
                    Some(serde_property) => Ok(MapKey::Property(*serde_property)),
                    None => {
                        // TODO: This error message could be improved to suggest valid fields.
                        // see OneOf in serde (this is a private struct)
                        Err(serde::de::Error::custom(format!("unknown property `{v}`")))
                    }
                }
            }
        }
    }
}

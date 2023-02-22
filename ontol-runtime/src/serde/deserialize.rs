use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{
    de::{DeserializeSeed, Error, MapAccess, SeqAccess, Unexpected, Visitor},
    Deserializer,
};
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
        CapturingStringPatternMatcher, ConstantStringMatcher, ExpectingMatching, IntMatcher,
        SequenceMatcher, StringMatcher, StringPatternMatcher, UnionMatcher, UnitMatcher,
        ValueMatcher,
    },
    MapType, SerdeOperator, SerdeOperatorId, SerdeProcessor, SerdeProperty,
};

enum MapKey {
    Property(SerdeProperty),
    RelParams(SerdeOperatorId),
}

pub(super) struct MatcherVisitor<'e, M> {
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
    fn assert_no_rel_params(&self) {
        assert!(
            self.rel_params_operator_id.is_none(),
            "rel_params_operator_id should be None for {:?}",
            self.value_operator
        );
    }
}

trait IntoVisitor: ValueMatcher + Sized {
    fn into_visitor(self, processor: SerdeProcessor) -> MatcherVisitor<Self> {
        MatcherVisitor {
            matcher: self,
            env: processor.env,
        }
    }

    fn into_visitor_no_params(self, processor: SerdeProcessor) -> MatcherVisitor<Self> {
        processor.assert_no_rel_params();
        self.into_visitor(processor)
    }
}

impl<M: ValueMatcher + Sized> IntoVisitor for M {}

/// The serde implementation deserializes Attributes instead of Values.
///
/// The reason for this is parameterized relationships.
///
/// In JSON, the parameters of a parameterized relationships are _inlined_
/// inside the attribute value, with the special map key `_edge`.
///
/// This is also the reason that only map types may be related through parameterized relationships.
/// Other types only support unparameterized relationships.
impl<'e, 'de> DeserializeSeed<'de> for SerdeProcessor<'e> {
    type Value = Attribute;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match self.value_operator {
            SerdeOperator::Unit => {
                deserializer.deserialize_unit(UnitMatcher.into_visitor_no_params(self))
            }
            SerdeOperator::Int(def_id) => {
                deserializer.deserialize_i64(IntMatcher(*def_id).into_visitor_no_params(self))
            }
            SerdeOperator::Number(def_id) => {
                deserializer.deserialize_i64(IntMatcher(*def_id).into_visitor_no_params(self))
            }
            SerdeOperator::String(def_id) => deserializer.deserialize_str(
                StringMatcher {
                    def_id: *def_id,
                    env: self.env,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::StringConstant(literal, def_id) => deserializer.deserialize_str(
                ConstantStringMatcher {
                    literal,
                    def_id: *def_id,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::StringPattern(def_id) => deserializer.deserialize_str(
                StringPatternMatcher {
                    pattern: self.env.string_patterns.get(def_id).unwrap(),
                    def_id: *def_id,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::CapturingStringPattern(def_id) => deserializer.deserialize_str(
                CapturingStringPatternMatcher {
                    pattern: self.env.string_patterns.get(def_id).unwrap(),
                    def_id: *def_id,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::Sequence(ranges, def_id) => deserializer.deserialize_seq(
                SequenceMatcher::new(ranges, *def_id, self.rel_params_operator_id)
                    .into_visitor(self),
            ),
            SerdeOperator::ValueType(value_type) => {
                let typed_value = self
                    .env
                    .new_serde_processor(value_type.inner_operator_id)
                    .deserialize(deserializer)?;

                Ok(typed_value)
            }
            SerdeOperator::ValueUnionType(value_union_type) => deserializer.deserialize_any(
                UnionMatcher {
                    value_union_type,
                    rel_params_operator_id: self.rel_params_operator_id,
                    env: self.env,
                }
                .into_visitor(self),
            ),
            SerdeOperator::MapType(map_type) => deserializer.deserialize_map(MapTypeVisitor {
                buffered_attrs: Default::default(),
                map_type,
                rel_params_operator_id: self.rel_params_operator_id,
                env: self.env,
            }),
        }
    }
}

impl<'e, 'de, M: ValueMatcher> Visitor<'de> for MatcherVisitor<'e, M> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.matcher.expecting(f)
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        let value = self
            .matcher
            .match_unit()
            .map_err(|_| Error::invalid_type(Unexpected::Unit, &self))?;

        Ok(Attribute::with_unit_params(value))
    }

    fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
        let type_def_id = self
            .matcher
            .match_u64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok(Attribute::with_unit_params(Value {
            data: Data::Int(
                v.try_into()
                    .map_err(|_| Error::custom("u64 overflow".to_string()))?,
            ),
            type_def_id,
        }))
    }

    fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
        let type_def_id = self
            .matcher
            .match_i64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok(Attribute::with_unit_params(Value {
            data: Data::Int(v),
            type_def_id,
        }))
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        let value = self
            .matcher
            .match_str(v)
            .map_err(|_| Error::invalid_type(Unexpected::Str(v), &self))?;

        Ok(Attribute::with_unit_params(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut sequence_matcher = self
            .matcher
            .match_sequence()
            .map_err(|_| Error::invalid_type(Unexpected::Seq, &self))?;

        let mut output = vec![];

        loop {
            let processor = match sequence_matcher.match_next_seq_element() {
                Some(element_match) => self.env.new_serde_processor_parameterized(
                    element_match.element_operator_id,
                    element_match.rel_params_operator_id,
                ),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(Attribute::with_unit_params(Value {
                        data: Data::Sequence(output),
                        type_def_id: sequence_matcher.type_def_id,
                    }));
                }
            };

            match seq.next_element_seed(processor)? {
                Some(attribute) => {
                    output.push(attribute);
                }
                None => {
                    return match sequence_matcher.match_seq_end() {
                        Ok(_) => Ok(Attribute::with_unit_params(Value {
                            data: Data::Sequence(output),
                            type_def_id: sequence_matcher.type_def_id,
                        })),
                        Err(_) => Err(Error::invalid_length(output.len(), &self)),
                    };
                }
            }
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let map_matcher = self
            .matcher
            .match_map()
            .map_err(|_| Error::invalid_type(Unexpected::Map, &self))?;

        // Because serde is stream-oriented, we need to start storing attributes
        // into a buffer until the type can be properly recognized
        let mut buffered_attrs: Vec<(String, serde_value::Value)> = Default::default();

        let map_type = loop {
            let property = match map.next_key::<String>()? {
                Some(property) => property,
                None => match map_matcher.match_fallback() {
                    Ok(map_type) => {
                        break map_type;
                    }
                    Err(_) => {
                        return Err(Error::custom(format!(
                            "invalid map value, expected {}",
                            ExpectingMatching(&self.matcher)
                        )));
                    }
                },
            };

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

impl<'e, 'de> Visitor<'de> for MapTypeVisitor<'e> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.map_type.typename)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
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

            return Err(Error::custom(format!(
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

impl<'s, 'de> DeserializeSeed<'de> for PropertySet<'s> {
    type Value = MapKey;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'s, 'de> Visitor<'de> for PropertySet<'s> {
    type Value = MapKey;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        match v {
            EDGE_PROPERTY => {
                if let Some(operator_id) = self.rel_params_operator_id {
                    Ok(MapKey::RelParams(operator_id))
                } else {
                    Err(Error::custom("`_edge` property not accepted here"))
                }
            }
            _ => {
                match self.properties.get(v) {
                    Some(serde_property) => Ok(MapKey::Property(*serde_property)),
                    None => {
                        // TODO: This error message could be improved to suggest valid fields.
                        // see OneOf in serde (this is a private struct)
                        Err(Error::custom(format!("unknown property `{v}`")))
                    }
                }
            }
        }
    }
}

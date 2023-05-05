use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{
    de::{DeserializeSeed, Error, MapAccess, SeqAccess, Unexpected, Visitor},
    Deserializer,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    format_utils::{DoubleQuote, LogicOp, Missing},
    proc::{NParams, Procedure},
    serde::{deserialize_matcher::MapMatchResult, EDGE_PROPERTY},
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};

use super::{
    deserialize_matcher::{
        BoolMatcher, CapturingStringPatternMatcher, ConstantStringMatcher, ExpectingMatching,
        IntMatcher, MapMatchKind, SequenceMatcher, StringMatcher, StringPatternMatcher,
        UnionMatcher, UnitMatcher, ValueMatcher,
    },
    operator::{FilteredVariants, SerdeOperator, SerdeProperty},
    processor::SerdeProcessor,
    SerdeOperatorId, StructOperator,
};

enum MapKey {
    Property(SerdeProperty),
    RelParams(SerdeOperatorId),
    Id(SerdeOperatorId),
}

struct DeserializedMap {
    attributes: BTreeMap<PropertyId, Attribute>,
    id: Option<Value>,
    rel_params: Value,
}

pub(super) struct MatcherVisitor<'e, M> {
    processor: SerdeProcessor<'e>,
    matcher: M,
}

struct StructVisitor<'e> {
    processor: SerdeProcessor<'e>,
    buffered_attrs: Vec<(String, serde_value::Value)>,
    struct_op: &'e StructOperator,
    rel_params_operator_id: Option<SerdeOperatorId>,
}

#[derive(Clone, Copy)]
struct SpecialOperatorIds<'s> {
    rel_params: Option<SerdeOperatorId>,
    id: Option<(&'s str, SerdeOperatorId)>,
}

#[derive(Clone, Copy)]
struct PropertySet<'s> {
    properties: &'s IndexMap<String, SerdeProperty>,
    special_operator_ids: SpecialOperatorIds<'s>,
}

impl<'s> PropertySet<'s> {
    fn new(
        properties: &'s IndexMap<String, SerdeProperty>,
        special_operator_ids: SpecialOperatorIds<'s>,
    ) -> Self {
        Self {
            properties,
            special_operator_ids,
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
            processor,
            matcher: self,
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
            SerdeOperator::False(def_id) => deserializer
                .deserialize_bool(BoolMatcher::False(*def_id).into_visitor_no_params(self)),
            SerdeOperator::True(def_id) => deserializer
                .deserialize_bool(BoolMatcher::True(*def_id).into_visitor_no_params(self)),
            SerdeOperator::Bool(def_id) => deserializer
                .deserialize_bool(BoolMatcher::Bool(*def_id).into_visitor_no_params(self)),
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
                    env: self.env,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::CapturingStringPattern(def_id) => deserializer.deserialize_str(
                CapturingStringPatternMatcher {
                    pattern: self.env.string_patterns.get(def_id).unwrap(),
                    def_id: *def_id,
                    env: self.env,
                }
                .into_visitor_no_params(self),
            ),
            SerdeOperator::DynamicSequence => {
                Err(Error::custom("Cannot deserialize dynamic sequence"))
            }
            SerdeOperator::RelationSequence(seq_op) => deserializer.deserialize_seq(
                SequenceMatcher::new(
                    &seq_op.ranges,
                    seq_op.def_variant.def_id,
                    self.rel_params_operator_id,
                )
                .into_visitor(self),
            ),
            SerdeOperator::ConstructorSequence(seq_op) => deserializer.deserialize_seq(
                SequenceMatcher::new(
                    &seq_op.ranges,
                    seq_op.def_variant.def_id,
                    self.rel_params_operator_id,
                )
                .into_visitor(self),
            ),
            SerdeOperator::ValueType(value_op) => {
                let mut typed_attribute = self
                    .narrow(value_op.inner_operator_id)
                    .deserialize(deserializer)?;

                typed_attribute.value.type_def_id = value_op.def_variant.def_id;

                Ok(typed_attribute)
            }
            SerdeOperator::Union(union_op) => match union_op.variants(self.mode, self.level) {
                FilteredVariants::Single(operator_id) => {
                    self.narrow(operator_id).deserialize(deserializer)
                }
                FilteredVariants::Multi(variants) => deserializer.deserialize_any(
                    UnionMatcher {
                        typename: union_op.typename(),
                        variants,
                        rel_params_operator_id: self.rel_params_operator_id,
                        env: self.env,
                        mode: self.mode,
                        level: self.level,
                    }
                    .into_visitor(self),
                ),
            },

            SerdeOperator::PrimaryId(_name, _inner_operator_id) => {
                //deserializer.deserialize_map()
                todo!()
            }
            SerdeOperator::Struct(struct_op) => deserializer.deserialize_map(StructVisitor {
                processor: self,
                buffered_attrs: Default::default(),
                struct_op,
                rel_params_operator_id: self.rel_params_operator_id,
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
        Ok(self
            .matcher
            .match_unit()
            .map_err(|_| Error::invalid_type(Unexpected::Unit, &self))?
            .into())
    }

    fn visit_bool<E: Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(self
            .matcher
            .match_bool(v)
            .map_err(|_| Error::invalid_type(Unexpected::Bool(v), &self))?
            .into())
    }

    fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
        let type_def_id = self
            .matcher
            .match_u64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Unsigned(v), &self))?;

        Ok(Value {
            data: Data::Int(
                v.try_into()
                    .map_err(|_| Error::custom("u64 overflow".to_string()))?,
            ),
            type_def_id,
        }
        .into())
    }

    fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
        let type_def_id = self
            .matcher
            .match_i64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Signed(v), &self))?;

        Ok(Value {
            data: Data::Int(v),
            type_def_id,
        }
        .into())
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(self
            .matcher
            .match_str(v)
            .map_err(|_| Error::invalid_type(Unexpected::Str(v), &self))?
            .into())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut sequence_matcher = self
            .matcher
            .match_sequence()
            .map_err(|_| Error::invalid_type(Unexpected::Seq, &self))?;

        let mut output = vec![];

        loop {
            let processor = match sequence_matcher.match_next_seq_element() {
                Some(element_match) => self.processor.narrow_with_rel(
                    element_match.element_operator_id,
                    element_match.rel_params_operator_id,
                ),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(Value {
                        data: Data::Sequence(output),
                        type_def_id: sequence_matcher.type_def_id,
                    }
                    .into());
                }
            };

            match seq.next_element_seed(processor)? {
                Some(attribute) => {
                    output.push(attribute);
                }
                None => {
                    return if sequence_matcher.match_seq_end().is_ok() {
                        Ok(Value {
                            data: Data::Sequence(output),
                            type_def_id: sequence_matcher.type_def_id,
                        }
                        .into())
                    } else {
                        Err(Error::invalid_length(output.len(), &self))
                    };
                }
            }
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        debug!("visit map\n");
        let mut matcher = self
            .matcher
            .match_map()
            .map_err(|_| Error::invalid_type(Unexpected::Map, &self))?;

        // Because serde is stream-oriented, we need to start storing attributes
        // into a buffer until the type can be properly recognized
        let mut buffered_attrs: Vec<(String, serde_value::Value)> = Default::default();

        let map_match = loop {
            let property = match map.next_key::<String>()? {
                Some(property) => property,
                None => match matcher.match_fallback() {
                    MapMatchResult::Match(map_match) => {
                        break map_match;
                    }
                    MapMatchResult::Indecisive(_) => {
                        return Err(Error::custom(format!(
                            "invalid map value, expected {}",
                            ExpectingMatching(&self.matcher)
                        )));
                    }
                },
            };

            let value: serde_value::Value = map.next_value()?;

            match matcher.match_attribute(&property, &value) {
                MapMatchResult::Match(map_match) => {
                    buffered_attrs.push((property, value));
                    break map_match;
                }
                MapMatchResult::Indecisive(next_matcher) => {
                    matcher = next_matcher;
                }
            }

            buffered_attrs.push((property, value));
        };

        debug!("matched map: {map_match:?} buffered attrs: {buffered_attrs:?}");

        // delegate to the real map visitor
        match map_match.kind {
            MapMatchKind::StructType(struct_op) => StructVisitor {
                processor: self.processor,
                buffered_attrs,
                struct_op,
                rel_params_operator_id: map_match.rel_params_operator_id,
            }
            .visit_map(map),
            MapMatchKind::IdType(name, serde_operator_id) => {
                let deserialized_map = deserialize_map(
                    self.processor,
                    map,
                    buffered_attrs,
                    &IndexMap::default(),
                    0,
                    SpecialOperatorIds {
                        rel_params: map_match.rel_params_operator_id,
                        id: Some((name, serde_operator_id)),
                    },
                )?;
                let id = deserialized_map
                    .id
                    .ok_or_else(|| Error::custom("missing _id attribute".to_string()))?;

                Ok(Attribute {
                    value: id,
                    rel_params: deserialized_map.rel_params,
                })
            }
        }
    }
}

impl<'e, 'de> Visitor<'de> for StructVisitor<'e> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.struct_op.typename)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let type_def_id = self.struct_op.def_variant.def_id;
        let deserialized_map = deserialize_map(
            self.processor,
            map,
            self.buffered_attrs,
            &self.struct_op.properties,
            self.struct_op.n_mandatory_properties,
            SpecialOperatorIds {
                rel_params: self.rel_params_operator_id,
                id: None,
            },
        )?;
        Ok(Attribute {
            value: Value {
                data: Data::Struct(deserialized_map.attributes),
                type_def_id,
            },
            rel_params: deserialized_map.rel_params,
        })
    }
}

fn deserialize_map<'e, 'de, A: MapAccess<'de>>(
    processor: SerdeProcessor<'e>,
    mut map: A,
    buffered_attrs: Vec<(String, serde_value::Value)>,
    properties: &IndexMap<String, SerdeProperty>,
    expected_mandatory_properties: usize,
    special_operator_ids: SpecialOperatorIds,
) -> Result<DeserializedMap, A::Error> {
    let mut attributes = BTreeMap::new();
    let mut rel_params = Value::unit();
    let mut id = None;

    let mut n_mandatory_properties = 0;

    // first parse buffered attributes, if any
    for (serde_key, serde_value) in buffered_attrs {
        match PropertySet::new(properties, special_operator_ids).visit_str(&serde_key)? {
            MapKey::RelParams(operator_id) => {
                let Attribute { value, .. } = processor
                    .new_child(operator_id)
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                rel_params = value;
            }
            MapKey::Id(operator_id) => {
                let Attribute { value, .. } = processor
                    .new_child(operator_id)
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;
                id = Some(value);
            }
            MapKey::Property(serde_property) => {
                let Attribute { rel_params, value } = processor
                    .new_child_with_rel(
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
    while let Some(map_key) =
        map.next_key_seed(PropertySet::new(properties, special_operator_ids))?
    {
        match map_key {
            MapKey::RelParams(operator_id) => {
                let Attribute { value, .. } =
                    map.next_value_seed(processor.new_child(operator_id))?;

                rel_params = value;
            }
            MapKey::Id(operator_id) => {
                let Attribute { value, .. } =
                    map.next_value_seed(processor.new_child(operator_id))?;

                id = Some(value);
            }
            MapKey::Property(serde_property) => {
                let attribute = map.next_value_seed(processor.new_child_with_rel(
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

    if n_mandatory_properties < expected_mandatory_properties {
        // Generate default values if missing
        for (_, property) in properties {
            if let Some(default_const_proc_address) = property.default_const_proc_address {
                if !property.optional && !attributes.contains_key(&property.property_id) {
                    let mut mapping_vm = processor.env.new_mapper();
                    let value = mapping_vm.eval(
                        Procedure {
                            address: default_const_proc_address,
                            n_params: NParams(0),
                        },
                        [],
                    );

                    // BUG: No support for rel_params:
                    attributes.insert(property.property_id, value.into());
                    n_mandatory_properties += 1;
                }
            }
        }
    }

    if n_mandatory_properties < expected_mandatory_properties
        || (rel_params.is_unit() != special_operator_ids.rel_params.is_none())
    {
        debug!(
            "Missing attributes. Rel params match: {}, {}",
            rel_params.is_unit(),
            special_operator_ids.rel_params.is_none()
        );
        for attr in &attributes {
            debug!("    attr {:?}", attr.0);
        }
        for prop in properties {
            debug!(
                "    prop {:?} '{}' optional={}",
                prop.1.property_id, prop.0, prop.1.optional
            );
        }

        let mut items: Vec<DoubleQuote<String>> = properties
            .iter()
            .filter(|(_, property)| {
                !property.optional && !attributes.contains_key(&property.property_id)
            })
            .map(|(key, _)| DoubleQuote(key.clone()))
            .collect();

        if special_operator_ids.rel_params.is_some() && rel_params.type_def_id == DefId::unit() {
            items.push(DoubleQuote(EDGE_PROPERTY.into()));
        }

        debug!("    items len: {}", items.len());

        let missing_keys = Missing {
            items,
            logic_op: LogicOp::And,
        };

        return Err(Error::custom(format!(
            "missing properties, expected {missing_keys}"
        )));
    }

    Ok(DeserializedMap {
        attributes,
        id,
        rel_params,
    })
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
                if let Some(operator_id) = self.special_operator_ids.rel_params {
                    Ok(MapKey::RelParams(operator_id))
                } else {
                    Err(Error::custom("`_edge` property not accepted here"))
                }
            }
            _ => {
                if let Some((property_name, operator_id)) = self.special_operator_ids.id {
                    if v == property_name {
                        return Ok(MapKey::Id(operator_id));
                    }
                }

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

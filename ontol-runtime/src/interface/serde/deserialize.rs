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
    interface::serde::{deserialize_matcher::MapMatchResult, EDGE_PROPERTY},
    value::{Attribute, Data, PropertyId, Value},
    value_generator::ValueGenerator,
    vm::proc::{NParams, Procedure},
    DefId,
};

use super::{
    deserialize_matcher::{
        BooleanMatcher, CapturingTextPatternMatcher, ConstantStringMatcher, ExpectingMatching,
        MapMatchKind, NumberMatcher, SequenceMatcher, StringMatcher, TextPatternMatcher,
        UnionMatcher, UnitMatcher, ValueMatcher,
    },
    operator::{FilteredVariants, SerdeOperator, SerdeProperty},
    processor::{
        ProcessorMode, ProcessorProfile, RecursionLimitError, ScalarFormat, SerdeProcessor,
        SubProcessorContext,
    },
    SerdeOperatorAddr, StructOperator,
};

enum MapKey {
    Property(SerdeProperty),
    RelParams(SerdeOperatorAddr),
    Id(SerdeOperatorAddr),
    Ignored,
}

struct DeserializedMap {
    attributes: BTreeMap<PropertyId, Attribute>,
    id: Option<Value>,
    rel_params: Value,
}

pub(super) struct MatcherVisitor<'on, 'p, M> {
    processor: SerdeProcessor<'on, 'p>,
    matcher: M,
}

struct StructVisitor<'on, 'p> {
    processor: SerdeProcessor<'on, 'p>,
    buffered_attrs: Vec<(String, serde_value::Value)>,
    struct_op: &'on StructOperator,
    ctx: SubProcessorContext,
}

#[derive(Clone, Copy)]
struct SpecialAddrs<'s> {
    rel_params: Option<SerdeOperatorAddr>,
    id: Option<(&'s str, SerdeOperatorAddr)>,
}

#[derive(Clone, Copy)]
struct PropertySet<'a> {
    properties: &'a IndexMap<String, SerdeProperty>,
    special_addrs: SpecialAddrs<'a>,
    processor_mode: ProcessorMode,
    processor_profile: &'a ProcessorProfile,
    parent_property_id: Option<PropertyId>,
}

impl<'a> PropertySet<'a> {
    fn new(
        properties: &'a IndexMap<String, SerdeProperty>,
        special_addrs: SpecialAddrs<'a>,
        processor_mode: ProcessorMode,
        processor_profile: &'a ProcessorProfile,
        parent_property_id: Option<PropertyId>,
    ) -> Self {
        Self {
            properties,
            special_addrs,
            processor_mode,
            processor_profile,
            parent_property_id,
        }
    }
}

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    fn assert_no_rel_params(&self) {
        assert!(
            self.ctx.rel_params_addr.is_none(),
            "rel_params_addr should be None for {:?}",
            self.value_operator
        );
    }
}

trait IntoVisitor<'on, 'p>: ValueMatcher + Sized {
    fn into_visitor(self, processor: SerdeProcessor<'on, 'p>) -> MatcherVisitor<'on, 'p, Self> {
        MatcherVisitor {
            processor,
            matcher: self,
        }
    }

    fn into_visitor_no_params(
        self,
        processor: SerdeProcessor<'on, 'p>,
    ) -> MatcherVisitor<'on, 'p, Self> {
        processor.assert_no_rel_params();
        self.into_visitor(processor)
    }
}

impl<'on, 'p, M: ValueMatcher + Sized> IntoVisitor<'on, 'p> for M {}

/// The serde implementation deserializes Attributes instead of Values.
///
/// The reason for this is parameterized relationships.
///
/// In JSON, the parameters of a parameterized relationships are _inlined_
/// inside the attribute value, with the special map key `_edge`.
///
/// This is also the reason that only map types may be related through parameterized relationships.
/// Other types only support unparameterized relationships.
impl<'on, 'p, 'de> DeserializeSeed<'de> for SerdeProcessor<'on, 'p> {
    type Value = Attribute;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match (self.value_operator, self.scalar_format()) {
            (SerdeOperator::Unit, _) => {
                deserializer.deserialize_unit(UnitMatcher.into_visitor_no_params(self))
            }
            (SerdeOperator::False(def_id), _) => deserializer
                .deserialize_bool(BooleanMatcher::False(*def_id).into_visitor_no_params(self)),
            (SerdeOperator::True(def_id), _) => deserializer
                .deserialize_bool(BooleanMatcher::True(*def_id).into_visitor_no_params(self)),
            (SerdeOperator::Boolean(def_id), _) => deserializer
                .deserialize_bool(BooleanMatcher::Boolean(*def_id).into_visitor_no_params(self)),
            (SerdeOperator::I64(def_id, range), ScalarFormat::DomainTransparent) => deserializer
                .deserialize_i64(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::I64(def_id, range), ScalarFormat::RawText) => deserializer
                .deserialize_str(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::I32(def_id, range), ScalarFormat::DomainTransparent) => deserializer
                .deserialize_i32(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::I32(def_id, range), ScalarFormat::RawText) => deserializer
                .deserialize_str(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::F64(def_id, range), ScalarFormat::DomainTransparent) => deserializer
                .deserialize_f64(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::F64(def_id, range), ScalarFormat::RawText) => deserializer
                .deserialize_str(
                    NumberMatcher {
                        def_id: *def_id,
                        range: range.clone(),
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::String(def_id), _) => deserializer.deserialize_str(
                StringMatcher {
                    def_id: *def_id,
                    ontology: self.ontology,
                }
                .into_visitor_no_params(self),
            ),
            (SerdeOperator::StringConstant(literal, def_id), _) => deserializer.deserialize_str(
                ConstantStringMatcher {
                    literal,
                    def_id: *def_id,
                }
                .into_visitor_no_params(self),
            ),
            (SerdeOperator::TextPattern(def_id), _) => deserializer.deserialize_str(
                TextPatternMatcher {
                    pattern: self.ontology.text_patterns.get(def_id).unwrap(),
                    def_id: *def_id,
                    ontology: self.ontology,
                }
                .into_visitor_no_params(self),
            ),
            (SerdeOperator::CapturingTextPattern(def_id), scalar_format) => deserializer
                .deserialize_str(
                    CapturingTextPatternMatcher {
                        pattern: self.ontology.text_patterns.get(def_id).unwrap(),
                        def_id: *def_id,
                        ontology: self.ontology,
                        scalar_format,
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::DynamicSequence, _) => {
                Err(Error::custom("Cannot deserialize dynamic sequence"))
            }
            (SerdeOperator::RelationSequence(seq_op), _) => deserializer.deserialize_seq(
                SequenceMatcher::new(&seq_op.ranges, seq_op.def.def_id, self.ctx)
                    .into_visitor(self),
            ),
            (SerdeOperator::ConstructorSequence(seq_op), _) => deserializer.deserialize_seq(
                SequenceMatcher::new(&seq_op.ranges, seq_op.def.def_id, self.ctx)
                    .into_visitor(self),
            ),
            (SerdeOperator::Alias(value_op), _) => {
                let mut typed_attribute =
                    self.narrow(value_op.inner_addr).deserialize(deserializer)?;

                typed_attribute.value.type_def_id = value_op.def.def_id;

                Ok(typed_attribute)
            }
            (SerdeOperator::Union(union_op), _) => match union_op.variants(self.mode, self.level) {
                FilteredVariants::Single(addr) => self.narrow(addr).deserialize(deserializer),
                FilteredVariants::Union(variants) => deserializer.deserialize_any(
                    UnionMatcher {
                        typename: union_op.typename(),
                        variants,
                        ontology: self.ontology,
                        ctx: self.ctx,
                        mode: self.mode,
                        level: self.level,
                    }
                    .into_visitor(self),
                ),
            },
            (SerdeOperator::IdSingletonStruct(_name, _inner_addr), _) => {
                //deserializer.deserialize_map()
                todo!()
            }
            (SerdeOperator::Struct(struct_op), _) => deserializer.deserialize_map(StructVisitor {
                processor: self,
                buffered_attrs: Default::default(),
                struct_op,
                ctx: self.ctx,
            }),
        }
    }
}

impl<'on, 'p, 'de, M: ValueMatcher> Visitor<'de> for MatcherVisitor<'on, 'p, M> {
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
            .match_boolean(v)
            .map_err(|_| Error::invalid_type(Unexpected::Bool(v), &self))?
            .into())
    }

    fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(self
            .matcher
            .match_u64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Unsigned(v), &self))?
            .into())
    }

    fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(self
            .matcher
            .match_i64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Signed(v), &self))?
            .into())
    }

    fn visit_f32<E: Error>(self, v: f32) -> Result<Self::Value, E> {
        let double: f64 = v.into();
        // FIXME: Should have a matcher for f32
        Ok(self
            .matcher
            .match_f64(double)
            .map_err(|_| Error::invalid_type(Unexpected::Float(double), &self))?
            .into())
    }

    fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(self
            .matcher
            .match_f64(v)
            .map_err(|_| Error::invalid_type(Unexpected::Float(v), &self))?
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
                Some(element_match) => self
                    .processor
                    .narrow_with_context(element_match.element_addr, element_match.ctx),
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
                ctx: map_match.ctx,
            }
            .visit_map(map),
            MapMatchKind::IdType(name, addr) => {
                let deserialized_map = deserialize_map(
                    self.processor,
                    map,
                    buffered_attrs,
                    &IndexMap::default(),
                    0,
                    SpecialAddrs {
                        rel_params: map_match.ctx.rel_params_addr,
                        id: Some((name, addr)),
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

impl<'on, 'p, 'de> Visitor<'de> for StructVisitor<'on, 'p> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.struct_op.typename)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let type_def_id = self.struct_op.def.def_id;
        let deserialized_map = deserialize_map(
            self.processor,
            map,
            self.buffered_attrs,
            &self.struct_op.properties,
            self.struct_op
                .required_count(self.processor.mode, self.processor.ctx.parent_property_id),
            SpecialAddrs {
                rel_params: self.ctx.rel_params_addr,
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

fn deserialize_map<'on, 'p, 'de, A: MapAccess<'de>>(
    processor: SerdeProcessor<'on, 'p>,
    mut map: A,
    buffered_attrs: Vec<(String, serde_value::Value)>,
    properties: &IndexMap<String, SerdeProperty>,
    expected_required_count: usize,
    special_addrs: SpecialAddrs,
) -> Result<DeserializedMap, A::Error> {
    let mut attributes = BTreeMap::new();
    let mut rel_params = Value::unit();
    let mut id = None;

    let mut observed_required_count = 0;

    // first parse buffered attributes, if any
    for (serde_key, serde_value) in buffered_attrs {
        match PropertySet::new(
            properties,
            special_addrs,
            processor.mode,
            processor.profile,
            processor.ctx.parent_property_id,
        )
        .visit_str(&serde_key)?
        {
            MapKey::RelParams(addr) => {
                let Attribute { value, .. } = processor
                    .new_child(addr)
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                rel_params = value;
            }
            MapKey::Id(addr) => {
                let Attribute { value, .. } = processor
                    .new_child(addr)
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;
                id = Some(value);
            }
            MapKey::Property(serde_property) => {
                let Attribute { rel_params, value } = processor
                    .new_child_with_context(
                        serde_property.value_addr,
                        SubProcessorContext {
                            parent_property_id: Some(serde_property.property_id),
                            parent_property_flags: serde_property.flags,
                            rel_params_addr: serde_property.rel_params_addr,
                        },
                    )
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                if !serde_property.is_optional() {
                    observed_required_count += 1;
                }

                attributes.insert(serde_property.property_id, Attribute { rel_params, value });
            }
            MapKey::Ignored => {}
        }
    }

    // parse rest of map
    while let Some(map_key) = map.next_key_seed(PropertySet::new(
        properties,
        special_addrs,
        processor.mode,
        processor.profile,
        processor.ctx.parent_property_id,
    ))? {
        match map_key {
            MapKey::RelParams(addr) => {
                let Attribute { value, .. } = map.next_value_seed(
                    processor
                        .new_child(addr)
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                rel_params = value;
            }
            MapKey::Id(addr) => {
                let Attribute { value, .. } = map.next_value_seed(
                    processor
                        .new_child(addr)
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                id = Some(value);
            }
            MapKey::Property(serde_property) => {
                let attribute = map.next_value_seed(
                    processor
                        .new_child_with_context(
                            serde_property.value_addr,
                            SubProcessorContext {
                                parent_property_id: Some(serde_property.property_id),
                                parent_property_flags: serde_property.flags,
                                rel_params_addr: serde_property.rel_params_addr,
                            },
                        )
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                if !serde_property.is_optional() {
                    observed_required_count += 1;
                }

                attributes.insert(serde_property.property_id, attribute);
            }
            MapKey::Ignored => {
                let _value: serde_value::Value = map.next_value()?;
            }
        }
    }

    if observed_required_count < expected_required_count {
        // Generate default values if missing
        for (_, property) in properties {
            // Only _default values_ are handled in the deserializer:
            if let Some(ValueGenerator::DefaultProc(address)) = property.value_generator {
                if !property.is_optional() && !attributes.contains_key(&property.property_id) {
                    let procedure = Procedure {
                        address,
                        n_params: NParams(0),
                    };
                    let value = processor.ontology.new_vm(procedure).run([]).unwrap();

                    // BUG: No support for rel_params:
                    attributes.insert(property.property_id, value.into());
                    observed_required_count += 1;
                }
            }
        }
    }

    if observed_required_count < expected_required_count
        || (rel_params.is_unit() != special_addrs.rel_params.is_none())
    {
        debug!(
            "Missing attributes(mode={:?}). Rel params match: {}, special_rel: {} parent_relationship: {:?}",
            processor.mode,
            rel_params.is_unit(),
            special_addrs.rel_params.is_none(),
            processor.ctx.parent_property_id,
        );
        for attr in &attributes {
            debug!("    attr {:?}", attr.0);
        }
        for prop in properties {
            debug!(
                "    prop {:?}('{}') {:?} visible={} optional={}",
                prop.1.property_id,
                prop.0,
                prop.1.flags,
                prop.1
                    .filter(processor.mode, processor.ctx.parent_property_id)
                    .is_some(),
                prop.1.is_optional()
            );
        }

        let mut items: Vec<DoubleQuote<String>> = properties
            .iter()
            .filter(|(_, property)| {
                property
                    .filter(processor.mode, processor.ctx.parent_property_id)
                    .is_some()
                    && !property.is_optional()
                    && !attributes.contains_key(&property.property_id)
            })
            .map(|(key, _)| DoubleQuote(key.clone()))
            .collect();

        if special_addrs.rel_params.is_some() && rel_params.type_def_id == DefId::unit() {
            items.push(DoubleQuote(EDGE_PROPERTY.into()));
        }

        debug!("    items len: {}", items.len());

        let missing_keys = Missing {
            items,
            logic_op: LogicOp::And,
        };

        return Err(serde::de::Error::custom(format!(
            "missing properties, expected {missing_keys}"
        )));
    }

    Ok(DeserializedMap {
        attributes,
        id,
        rel_params,
    })
}

impl<'a, 'de> DeserializeSeed<'de> for PropertySet<'a> {
    type Value = MapKey;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'a, 'de> Visitor<'de> for PropertySet<'a> {
    type Value = MapKey;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        match v {
            EDGE_PROPERTY => {
                if let Some(addr) = self.special_addrs.rel_params {
                    Ok(MapKey::RelParams(addr))
                } else {
                    Err(Error::custom("`_edge` property not accepted here"))
                }
            }
            _ => {
                if let Some((property_name, addr)) = self.special_addrs.id {
                    if v == property_name {
                        return Ok(MapKey::Id(addr));
                    }
                }

                if Some(v) == self.processor_profile.overridden_id_property_key {
                    for (_, prop) in self.properties {
                        if prop.is_entity_id() {
                            return Ok(MapKey::Property(*prop));
                        }
                    }

                    return Err(Error::custom(format!("unknown property `{v}`")));
                }

                if self.processor_profile.ignored_property_keys.contains(&v) {
                    return Ok(MapKey::Ignored);
                }

                let serde_property = self.properties.get(v).ok_or_else(|| {
                    // TODO: This error message could be improved to suggest valid fields.
                    // see OneOf in serde (this is a private struct)
                    Error::custom(format!("unknown property `{v}`"))
                })?;

                if serde_property
                    .filter(self.processor_mode, self.parent_property_id)
                    .is_some()
                {
                    Ok(MapKey::Property(*serde_property))
                } else if serde_property.is_read_only()
                    && !matches!(self.processor_mode, ProcessorMode::Read)
                {
                    Err(Error::custom(format!("property `{v}` is read-only")))
                } else {
                    Err(Error::custom(format!(
                        "property `{v}` not available in this context"
                    )))
                }
            }
        }
    }
}

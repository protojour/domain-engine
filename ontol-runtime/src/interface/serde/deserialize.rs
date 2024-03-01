use indexmap::IndexMap;
use serde::{
    de::{DeserializeSeed, Error, MapAccess, SeqAccess, Unexpected, Visitor},
    Deserializer,
};
use smartstring::alias::String;
use tracing::trace;

use crate::{
    interface::serde::{
        deserialize_id::IdSingletonStructVisitor,
        deserialize_matcher::MapMatchResult,
        deserialize_struct::{deserialize_struct, SpecialAddrs, StructVisitor},
        operator::SerdeStructFlags,
    },
    sequence::Sequence,
    value::{Attribute, Serial, Value},
};

use super::{
    deserialize_matcher::{
        BooleanMatcher, CapturingTextPatternMatcher, ConstantStringMatcher, ExpectingMatching,
        MapMatchKind, NumberMatcher, SequenceMatcher, StringMatcher, TextPatternMatcher,
        UnionMatcher, UnitMatcher, ValueMatcher,
    },
    deserialize_patch::GraphqlPatchVisitor,
    operator::{AppliedVariants, SerdeOperator},
    processor::{ProcessorMode, ScalarFormat, SerdeProcessor},
};

pub(super) struct MatcherVisitor<'on, 'p, M> {
    processor: SerdeProcessor<'on, 'p>,
    matcher: M,
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
            (SerdeOperator::Serial(def_id), _) => deserializer.deserialize_str(
                NumberMatcher::<Serial> {
                    def_id: *def_id,
                    range: None,
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
            (SerdeOperator::RelationSequence(seq_op), _) => match (self.mode, seq_op.to_entity) {
                (ProcessorMode::GraphqlUpdate, true)
                    if !self.level.is_global_root() && self.level.is_local_root() =>
                {
                    deserializer.deserialize_map(GraphqlPatchVisitor {
                        entity_sequence_processor: self,
                        inner_addr: seq_op.ranges[0].addr,
                        type_def_id: seq_op.def.def_id,
                        ctx: self.ctx,
                    })
                }
                _ => deserializer.deserialize_seq(
                    SequenceMatcher::new(&seq_op.ranges, seq_op.def.def_id, self.ctx)
                        .into_visitor(self),
                ),
            },
            (SerdeOperator::ConstructorSequence(seq_op), _) => deserializer.deserialize_seq(
                SequenceMatcher::new(&seq_op.ranges, seq_op.def.def_id, self.ctx)
                    .into_visitor(self),
            ),
            (SerdeOperator::Alias(value_op), _) => {
                let mut typed_attribute =
                    self.narrow(value_op.inner_addr).deserialize(deserializer)?;

                *typed_attribute.val.type_def_id_mut() = value_op.def.def_id;

                Ok(typed_attribute)
            }
            (SerdeOperator::Union(union_op), _) => {
                match union_op.applied_variants(self.mode, self.level) {
                    AppliedVariants::Unambiguous(addr) => {
                        self.narrow(addr).deserialize(deserializer)
                    }
                    AppliedVariants::OneOf(variants) => deserializer.deserialize_any(
                        UnionMatcher {
                            typename: union_op.typename(),
                            variants,
                            ontology: self.ontology,
                            ctx: self.ctx,
                            profile: self.profile,
                            mode: self.mode,
                            level: self.level,
                        }
                        .into_visitor(self),
                    ),
                }
            }
            (SerdeOperator::IdSingletonStruct(name, inner_addr), _) => deserializer
                .deserialize_map(IdSingletonStructVisitor {
                    processor: self,
                    property_name: name,
                    inner_addr: *inner_addr,
                    ontology: self.ontology,
                }),
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

        let mut attrs = vec![];

        loop {
            let processor = match sequence_matcher.match_next_seq_element() {
                Some(element_match) => self
                    .processor
                    .narrow_with_context(element_match.element_addr, element_match.ctx),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(
                        Value::Sequence(Sequence::new(attrs), sequence_matcher.type_def_id).into(),
                    );
                }
            };

            match seq.next_element_seed(processor)? {
                Some(attribute) => {
                    attrs.push(attribute);
                }
                None => {
                    return if sequence_matcher.match_seq_end().is_ok() {
                        Ok(
                            Value::Sequence(Sequence::new(attrs), sequence_matcher.type_def_id)
                                .into(),
                        )
                    } else {
                        Err(Error::invalid_length(attrs.len(), &self))
                    };
                }
            }
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        trace!("visit map\n");
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
                    trace!("matched attribute \"{property}\"");
                    buffered_attrs.push((property, value));
                    break map_match;
                }
                MapMatchResult::Indecisive(next_matcher) => {
                    matcher = next_matcher;
                }
            }

            buffered_attrs.push((property, value));
        };

        trace!("matched map: {map_match:?} buffered attrs: {buffered_attrs:?}");

        // delegate to the real struct visitor
        match map_match.kind {
            MapMatchKind::StructType(struct_op) => StructVisitor {
                processor: self.processor,
                buffered_attrs,
                struct_op,
                ctx: map_match.ctx,
            }
            .visit_map(map),
            MapMatchKind::IdType(name, addr) => {
                let deserialized_map = deserialize_struct(
                    map,
                    buffered_attrs,
                    self.processor,
                    &IndexMap::default(),
                    SerdeStructFlags::empty(),
                    0,
                    SpecialAddrs {
                        rel_params: map_match.ctx.rel_params_addr,
                        id: Some((name, addr)),
                    },
                )?;
                let id = deserialized_map
                    .id
                    .ok_or_else(|| Error::custom("missing identifier attribute".to_string()))?;

                Ok(Attribute {
                    rel: deserialized_map.rel_params,
                    val: id,
                })
            }
        }
    }
}

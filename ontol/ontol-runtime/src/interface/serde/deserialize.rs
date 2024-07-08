use std::{ops::ControlFlow, slice};

use serde::{
    de::{DeserializeSeed, Error, MapAccess, SeqAccess, Unexpected, Visitor},
    Deserializer,
};
use smallvec::{smallvec, SmallVec};
use tracing::{debug, trace, trace_span, warn};

use crate::{
    attr::{Attr, AttrMatrix},
    debug::OntolDebug,
    interface::serde::{
        deserialize_id::IdSingletonStructVisitor,
        deserialize_struct::{PossibleProps, StructDeserializer, StructVisitor},
        matcher::map_matcher::MapMatchMode,
        operator::SerdeStructFlags,
    },
    sequence::{IndexSetBuilder, ListBuilder, SequenceBuilder, WithCapacity},
    value::{Serial, Value, ValueTag},
};

use super::{
    deserialize_patch::GraphqlPatchVisitor,
    matcher::{
        primitive_matchers::{BooleanMatcher, NumberMatcher, UnitMatcher},
        sequence_matcher::{SequenceKind, SequenceRangesMatcher},
        text_matchers::{
            CapturingTextPatternMatcher, ConstantStringMatcher, StringMatcher, TextPatternMatcher,
        },
        union_matcher::UnionMatcher,
        ExpectingMatching, ValueMatcher,
    },
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
            self.value_operator.debug(self.ontology)
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
    type Value = Attr;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match (self.value_operator, self.scalar_format()) {
            (SerdeOperator::AnyPlaceholder, _) => {
                warn!("Deserialization of AnyPlaceholder");
                Err(Error::custom("unknown type"))
            }
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
            (SerdeOperator::String(def_id), _) => deserializer
                .deserialize_str(StringMatcher { def_id: *def_id }.into_visitor_no_params(self)),
            (SerdeOperator::StringConstant(constant, def_id), _) => deserializer.deserialize_str(
                ConstantStringMatcher {
                    constant: &self.ontology[*constant],
                    def_id: *def_id,
                }
                .into_visitor_no_params(self),
            ),
            (SerdeOperator::TextPattern(def_id), _) => deserializer.deserialize_str(
                TextPatternMatcher {
                    pattern: self.ontology.data.text_patterns.get(def_id).unwrap(),
                    def_id: *def_id,
                    ontology: self.ontology,
                }
                .into_visitor_no_params(self),
            ),
            (SerdeOperator::CapturingTextPattern(def_id), scalar_format) => deserializer
                .deserialize_str(
                    CapturingTextPatternMatcher {
                        pattern: self.ontology.data.text_patterns.get(def_id).unwrap(),
                        def_id: *def_id,
                        ontology: self.ontology,
                        scalar_format,
                    }
                    .into_visitor_no_params(self),
                ),
            (SerdeOperator::DynamicSequence, _) => {
                Err(Error::custom("Cannot deserialize dynamic sequence"))
            }
            (SerdeOperator::RelationList(seq_op), _) => match (self.mode, seq_op.to_entity) {
                (ProcessorMode::GraphqlUpdate, true)
                    if !self.level.is_global_root() && self.level.is_local_root() =>
                {
                    deserializer.deserialize_map(GraphqlPatchVisitor {
                        entity_sequence_processor: self,
                    })
                }
                _ => deserializer.deserialize_seq(
                    SequenceRangesMatcher::new(
                        slice::from_ref(&seq_op.range),
                        SequenceKind::AttrMatrixList,
                        seq_op.def.def_id,
                        self.ctx,
                    )
                    .into_visitor(self),
                ),
            },
            (SerdeOperator::RelationIndexSet(seq_op), _) => match (self.mode, seq_op.to_entity) {
                (ProcessorMode::GraphqlUpdate, true)
                    if !self.level.is_global_root() && self.level.is_local_root() =>
                {
                    deserializer.deserialize_map(GraphqlPatchVisitor {
                        entity_sequence_processor: self,
                    })
                }
                _ => deserializer.deserialize_seq(
                    SequenceRangesMatcher::new(
                        slice::from_ref(&seq_op.range),
                        SequenceKind::AttrMatrixIndexSet,
                        seq_op.def.def_id,
                        self.ctx,
                    )
                    .into_visitor(self),
                ),
            },
            (SerdeOperator::ConstructorSequence(seq_op), _) => deserializer.deserialize_seq(
                SequenceRangesMatcher::new(
                    &seq_op.ranges,
                    SequenceKind::ValueList,
                    seq_op.def.def_id,
                    self.ctx,
                )
                .into_visitor(self),
            ),
            (SerdeOperator::Alias(value_op), _) => {
                let mut typed_attribute =
                    self.narrow(value_op.inner_addr).deserialize(deserializer)?;

                match &mut typed_attribute {
                    Attr::Unit(value) => {
                        value.tag_mut().set_def_id(value_op.def.def_id);
                    }
                    _ => panic!("can't change type"),
                }

                Ok(typed_attribute)
            }
            (SerdeOperator::Union(union_op), _) => {
                debug!("deserialize union {:?}", union_op.union_def().def_id);
                match union_op.applied_deserialize_variants(self.mode, self.level) {
                    AppliedVariants::Unambiguous(addr) => {
                        self.narrow(addr).deserialize(deserializer)
                    }
                    AppliedVariants::OneOf(possible_variants) => {
                        debug!("  OneOf {:?}", possible_variants.debug(self.ontology));

                        deserializer.deserialize_any(
                            UnionMatcher {
                                typename: union_op.typename(),
                                possible_variants,
                                ontology: self.ontology,
                                ctx: self.ctx,
                                profile: self.profile,
                                mode: self.mode,
                                level: self.level,
                            }
                            .into_visitor(self),
                        )
                    }
                }
            }
            (SerdeOperator::IdSingletonStruct(_, name, inner_addr), _) => deserializer
                .deserialize_map(IdSingletonStructVisitor {
                    processor: self,
                    property_name: &self.ontology[*name],
                    inner_addr: *inner_addr,
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
    type Value = Attr;

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

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        let mut sequence_matcher = self
            .matcher
            .match_sequence()
            .map_err(|_| Error::invalid_type(Unexpected::Seq, &self))?;

        let cap = seq.size_hint().unwrap_or(0);
        let attr = match sequence_matcher.kind {
            SequenceKind::AttrMatrixIndexSet => self.deserialize_sequence(
                seq,
                &mut sequence_matcher,
                MakeMatrix::<IndexSetBuilder<Value>>::new(cap),
            )?,
            SequenceKind::AttrMatrixList => self.deserialize_sequence(
                seq,
                &mut sequence_matcher,
                MakeMatrix::<ListBuilder<Value>>::new(cap),
            )?,
            SequenceKind::ValueList => {
                let type_def_id = sequence_matcher.type_def_id;
                self.deserialize_sequence(
                    seq,
                    &mut sequence_matcher,
                    MakeValueList {
                        builder: ListBuilder::with_capacity(cap),
                        tag: type_def_id.into(),
                    },
                )?
            }
        };

        Ok(attr)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        trace!("visit map\n");
        let mut map_matcher = self
            .matcher
            .match_map()
            .map_err(|_| Error::invalid_type(Unexpected::Map, &self))?;

        // Because serde is stream-oriented, we need to start storing attributes
        // into a buffer until the type can be properly recognized
        let mut buffered_attrs: Vec<(Box<str>, serde_value::Value)> = Default::default();

        let match_ok = loop {
            let property = match map.next_key::<Box<str>>()? {
                Some(property) => property,
                None => match map_matcher.consume_end(&buffered_attrs) {
                    Ok(match_ok) => {
                        break match_ok;
                    }
                    Err(()) => {
                        return Err(Error::custom(format!(
                            "invalid type, expected {}",
                            ExpectingMatching(&self.matcher)
                        )));
                    }
                },
            };

            buffered_attrs.push((property, map.next_value()?));

            match map_matcher.consume_next_attr(&buffered_attrs) {
                ControlFlow::Break(match_ok) => {
                    trace!(
                        "matched attribute \"{prop}\"",
                        prop = buffered_attrs.last().unwrap().0
                    );
                    break match_ok;
                }
                ControlFlow::Continue(()) => {}
            }
        };

        trace!(
            "matched map: {m:?} buffered attrs: {buffered_attrs:?}",
            m = match_ok.debug(self.processor.ontology)
        );

        // delegate to the real struct visitor
        match match_ok.mode {
            MapMatchMode::Struct(addr, struct_op) => {
                let _entered =
                    trace_span!("match_ok", addr = addr.0, id = ?struct_op.def.def_id).entered();

                let output = StructDeserializer::new(
                    struct_op.def.def_id,
                    self.processor,
                    PossibleProps::Any(&struct_op.properties),
                    struct_op.flags,
                    self.processor.level,
                )
                .with_required_props_bitset(struct_op.required_props_bitset(
                    self.processor.mode,
                    self.processor.ctx.parent_property_id,
                    self.processor.profile.flags,
                ))
                .with_rel_params_addr(match_ok.ctx.rel_params_addr)
                .deserialize_struct(buffered_attrs, map)?;

                if output.resolved_to_id {
                    let mut id = output.id.unwrap();
                    *(id.tag_mut()) = id.tag().with_is_update(match_ok.ctx.is_update);

                    Ok(Attr::unit_or_tuple(id, output.rel_params))
                } else {
                    Ok(Attr::unit_or_tuple(
                        Value::Struct(
                            Box::new(output.attributes),
                            ValueTag::from(struct_op.def.def_id)
                                .with_is_update(match_ok.ctx.is_update),
                        ),
                        output.rel_params,
                    ))
                }
            }
            MapMatchMode::EntityId(entity_id, name_constant, addr) => {
                let output = StructDeserializer::new(
                    entity_id,
                    self.processor,
                    PossibleProps::IdSingleton {
                        name: &self.processor.ontology[name_constant],
                        addr,
                    },
                    SerdeStructFlags::empty(),
                    self.processor.level,
                )
                .with_rel_params_addr(match_ok.ctx.rel_params_addr)
                .deserialize_struct(buffered_attrs, map)?;

                let id = output
                    .id
                    .ok_or_else(|| Error::custom("missing identifier attribute".to_string()))?;

                Ok(Attr::unit_or_tuple(id, output.rel_params))
            }
        }
    }
}

impl<'on, 'p, 'de, M: ValueMatcher> MatcherVisitor<'on, 'p, M> {
    fn deserialize_sequence<A: SeqAccess<'de>, MK: MakeVectorAttr>(
        &self,
        mut seq: A,
        sequence_matcher: &mut SequenceRangesMatcher<'on>,
        mut make_attr: MK,
    ) -> Result<Attr, A::Error> {
        loop {
            let processor = match sequence_matcher.match_next_seq_element() {
                Some(element_match) => self
                    .processor
                    .narrow_with_context(element_match.element_addr, element_match.ctx),
                None => {
                    // note: if there are more elements to deserialize,
                    // serde will automatically generate a 'trailing characters' error after returning:
                    return Ok(make_attr.into_attr());
                }
            };

            match seq.next_element_seed(processor)? {
                Some(attribute) => {
                    make_attr
                        .try_push(attribute)
                        .map_err(serde::de::Error::custom)?;
                }
                None => {
                    return if sequence_matcher.match_seq_end().is_ok() {
                        Ok(make_attr.into_attr())
                    } else {
                        Err(Error::invalid_length(make_attr.len(), self))
                    };
                }
            }
        }
    }
}

trait MakeVectorAttr {
    fn try_push(&mut self, attr: Attr) -> Result<(), String>;
    fn len(&self) -> usize;
    fn into_attr(self) -> Attr;
}

struct MakeMatrix<B> {
    capacity: usize,
    builder_tuple: SmallVec<B, 1>,
}

impl<B: WithCapacity> MakeMatrix<B> {
    fn new(cap: usize) -> Self {
        Self {
            capacity: cap,
            builder_tuple: smallvec![B::with_capacity(cap)],
        }
    }
}

impl<B> MakeVectorAttr for MakeMatrix<B>
where
    B: SequenceBuilder<Value> + WithCapacity,
{
    fn try_push(&mut self, attr: Attr) -> Result<(), String> {
        let tuple = attr.into_tuple().expect("matrix in matrix");

        self.builder_tuple.resize_with(tuple.len(), || {
            <B as WithCapacity>::with_capacity(self.capacity)
        });

        for (builder, element) in self.builder_tuple.iter_mut().zip(tuple.into_iter()) {
            builder
                .try_push(element)
                .map_err(|dupl| format!("{dupl}"))?;
        }

        Ok(())
    }

    fn len(&self) -> usize {
        if self.builder_tuple.is_empty() {
            0
        } else {
            self.builder_tuple[0].cur_len()
        }
    }

    fn into_attr(self) -> Attr {
        Attr::Matrix(AttrMatrix {
            columns: self.builder_tuple.into_iter().map(|t| t.build()).collect(),
        })
    }
}

struct MakeValueList<B> {
    builder: B,
    tag: ValueTag,
}

impl<B> MakeVectorAttr for MakeValueList<B>
where
    B: SequenceBuilder<Value>,
{
    fn try_push(&mut self, attr: Attr) -> Result<(), String> {
        let value = attr
            .into_unit()
            .ok_or_else(|| "expected unit value".to_string())?;
        self.builder
            .try_push(value)
            .map_err(|dupl| format!("{dupl}"))?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.builder.cur_len()
    }

    fn into_attr(self) -> Attr {
        Attr::Unit(Value::Sequence(self.builder.build(), self.tag))
    }
}

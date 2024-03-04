use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serializer,
};
use smartstring::alias::String;
use std::fmt::Write;
use tracing::{trace, warn};

use crate::{
    cast::Cast,
    interface::serde::{
        operator::AppliedVariants,
        processor::{RecursionLimitError, ScalarFormat},
    },
    smart_format,
    text_pattern::{FormatPattern, TextPatternConstantPart},
    value::{Attribute, FormatValueAsText, Value},
    DefId,
};

use super::{
    operator::{SequenceRange, SerdeOperator, SerdeStructFlags},
    processor::{ProcessorProfileFlags, SerdeProcessor, SpecialProperty, SubProcessorContext},
    serialize_raw::RawProxy,
    StructOperator, EDGE_PROPERTY,
};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

const UNIT_ATTR: Attribute = Attribute {
    rel: Value::unit(),
    val: Value::unit(),
};

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    /// Serialize a value using this processor.
    pub fn serialize_value<S: Serializer>(
        &self,
        value: &Value,
        rel_params: Option<&Value>,
        serializer: S,
    ) -> Res<S> {
        trace!(
            "serializing op={:?}",
            self.ontology.debug(self.value_operator)
        );

        match (self.value_operator, self.scalar_format()) {
            (SerdeOperator::Unit, _) => {
                cast_ref::<()>(value);
                serializer.serialize_unit()
            }
            (SerdeOperator::False(_), _) => serializer.serialize_bool(false),
            (SerdeOperator::True(_), _) => serializer.serialize_bool(true),
            (SerdeOperator::Boolean(_), _) => serializer.serialize_bool(*cast_ref::<bool>(value)),
            (SerdeOperator::I64(..), ScalarFormat::DomainTransparent) => match value {
                Value::I64(int, _) => serializer.serialize_i64(*int),
                Value::F64(f, _) => serializer.serialize_i64((*f).round() as i64),
                other => panic!("BUG: Serialize expected number, got {other:?}"),
            },
            (SerdeOperator::I32(..), ScalarFormat::DomainTransparent) => {
                let int_i64: i64 = match value {
                    Value::I64(int, _) => *int,
                    Value::F64(f, _) => (*f).round() as i64,
                    other => panic!("BUG: Serialize expected number, got {other:?}"),
                };

                serializer.serialize_i32(int_i64.try_into().map_err(|err| {
                    S::Error::custom(smart_format!("overflow when converting to i32: {err:?}"))
                })?)
            }
            (SerdeOperator::F64(..), ScalarFormat::DomainTransparent) => match value {
                Value::I64(num, _) => serializer.serialize_f64(*num as f64),
                Value::F64(f, _) => serializer.serialize_f64(*f),
                other => panic!("BUG: Serialize expected number, got {other:?}"),
            },
            (SerdeOperator::Serial(def_id), ScalarFormat::DomainTransparent) => {
                self.serialize_as_text_formatted(value, *def_id, serializer)
            }
            (
                SerdeOperator::I32(def_id, ..)
                | SerdeOperator::I64(def_id, ..)
                | SerdeOperator::F64(def_id, ..)
                | SerdeOperator::Serial(def_id, ..),
                ScalarFormat::RawText,
            ) => self.serialize_as_text_formatted(value, *def_id, serializer),
            (
                SerdeOperator::String(def_id)
                | SerdeOperator::StringConstant(_, def_id)
                | SerdeOperator::TextPattern(def_id),
                _,
            ) => match value {
                Value::Text(s, _) => serializer.serialize_str(s),
                _ => self.serialize_as_text_formatted(value, *def_id, serializer),
            },
            (
                SerdeOperator::CapturingTextPattern(pattern_def_id),
                ScalarFormat::DomainTransparent,
            ) => self.serialize_as_pattern_formatted(value, *pattern_def_id, serializer),
            (SerdeOperator::CapturingTextPattern(pattern_def_id), ScalarFormat::RawText) => {
                self.serialize_pattern_as_raw_text(value, *pattern_def_id, serializer)
            }
            (SerdeOperator::DynamicSequence, _) => match value {
                Value::Sequence(seq, _) => self.serialize_dynamic_sequence(&seq.attrs, serializer),
                _ => panic!("Not a sequence"),
            },
            (SerdeOperator::RelationSequence(seq_op), _) => {
                self.serialize_sequence(cast_ref::<Vec<_>>(value), &seq_op.ranges, serializer)
            }
            (SerdeOperator::ConstructorSequence(seq_op), _) => {
                self.serialize_sequence(cast_ref::<Vec<_>>(value), &seq_op.ranges, serializer)
            }
            (SerdeOperator::Alias(value_op), _) => self
                .narrow(value_op.inner_addr)
                .serialize_value(value, rel_params, serializer),
            (SerdeOperator::Union(union_op), _) => {
                match union_op.applied_variants(self.mode, self.level) {
                    AppliedVariants::Unambiguous(addr) => self
                        .narrow(addr)
                        .serialize_value(value, rel_params, serializer),
                    AppliedVariants::OneOf(possible_variants) => {
                        let variant = possible_variants
                            .into_iter()
                            .find(|variant| value.type_def_id() == variant.serde_def.def_id);

                        if let Some(variant) = variant {
                            let processor = self.narrow(variant.addr);
                            trace!(
                                "serializing union variant with {:?} {processor:}",
                                variant.addr
                            );

                            processor.serialize_value(value, rel_params, serializer)
                        } else {
                            panic!(
                                "Discriminator not found while serializing union type {:?}: {:#?}",
                                value.type_def_id(),
                                possible_variants.into_iter().collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
            (SerdeOperator::IdSingletonStruct(_, name_constant, inner_addr), _) => {
                let mut map = serializer.serialize_map(Some(1 + option_len(&rel_params)))?;
                map.serialize_entry(
                    &self.ontology[*name_constant],
                    &Proxy {
                        value,
                        rel_params: None,
                        processor: self
                            .new_child(*inner_addr)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;
                self.serialize_rel_params::<S>(rel_params, &mut map)?;

                map.end()
            }
            (SerdeOperator::Struct(struct_op), _) => {
                self.serialize_struct(struct_op, rel_params, value, serializer)
            }
        }
    }

    fn serialize_as_text_formatted<S: Serializer>(
        &self,
        value: &Value,
        operator_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            FormatValueAsText {
                value,
                type_def_id: operator_def_id,
                ontology: self.ontology
            }
        )
        .map_err(|_| S::Error::custom("Conversion to text failed"))?;

        serializer.serialize_str(&buf)
    }

    fn serialize_as_pattern_formatted<S: Serializer>(
        &self,
        value: &Value,
        pattern_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        let pattern = &self.ontology.text_patterns.get(&pattern_def_id).unwrap();
        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            FormatPattern {
                pattern,
                value,
                ontology: self.ontology
            }
        )
        .map_err(|_| S::Error::custom("Failed to serialize capturing text pattern"))?;

        serializer.serialize_str(&buf)
    }

    fn serialize_pattern_as_raw_text<S: Serializer>(
        &self,
        value: &Value,
        pattern_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        match value {
            Value::Struct(attrs, _) => {
                let pattern = &self.ontology.text_patterns.get(&pattern_def_id).unwrap();
                let mut pattern_property = None;

                for part in &pattern.constant_parts {
                    if let TextPatternConstantPart::Property(property) = part {
                        if pattern_property.is_some() {
                            return self.serialize_as_pattern_formatted(
                                value,
                                pattern_def_id,
                                serializer,
                            );
                        }

                        pattern_property = Some(property);
                    }
                }

                match pattern_property {
                    Some(pattern_property) => {
                        let attr = attrs.get(&pattern_property.property_id).ok_or_else(|| {
                            S::Error::custom("property not present in pattern struct")
                        })?;

                        self.serialize_as_text_formatted(
                            &attr.val,
                            pattern_property.type_def_id,
                            serializer,
                        )
                    }
                    None => self.serialize_as_text_formatted(value, pattern_def_id, serializer),
                }
            }
            _ => self.serialize_as_text_formatted(value, pattern_def_id, serializer),
        }
    }

    fn serialize_sequence<S: Serializer>(
        &self,
        elements: &[Attribute],
        ranges: &[SequenceRange],
        serializer: S,
    ) -> Res<S> {
        let mut seq = serializer.serialize_seq(Some(elements.len()))?;

        let mut element_iter = elements.iter();

        for range in ranges {
            if let Some(finite_repetition) = range.finite_repetition {
                for _ in 0..finite_repetition {
                    let attribute = element_iter.next().unwrap();

                    seq.serialize_element(&Proxy {
                        value: &attribute.val,
                        rel_params: attribute.rel.filter_non_unit(),
                        processor: self.narrow(range.addr),
                    })?;
                }
            } else {
                for attribute in element_iter.by_ref() {
                    seq.serialize_element(&Proxy {
                        value: &attribute.val,
                        rel_params: attribute.rel.filter_non_unit(),
                        processor: self.narrow(range.addr),
                    })?;
                }
            }
        }

        seq.end()
    }

    fn serialize_dynamic_sequence<S: Serializer>(
        &self,
        elements: &[Attribute],
        serializer: S,
    ) -> Res<S> {
        let mut seq = serializer.serialize_seq(Some(elements.len()))?;

        for attr in elements {
            let def_id = attr.val.type_def_id();
            match self.ontology.get_type_info(def_id).operator_addr {
                Some(addr) => seq.serialize_element(&Proxy {
                    value: &attr.val,
                    rel_params: None,
                    processor: self.narrow(addr),
                })?,
                None => {
                    panic!("No processor found for {def_id:?}");
                }
            }
        }

        seq.end()
    }

    fn serialize_struct<S: Serializer>(
        &self,
        struct_op: &StructOperator,
        rel_params: Option<&Value>,
        value: &Value,
        serializer: S,
    ) -> Res<S> {
        let attributes = match value {
            Value::Struct(attributes, _) => attributes,
            Value::StructUpdate(attributes, _) => attributes,
            // Support for empty structs that are Unit encoded:
            Value::Unit(_) => return serializer.serialize_map(Some(0))?.end(),
            other => panic!("BUG: Serialize expected map attributes, got {other:?}"),
        };

        let mut map = serializer.serialize_map(Some(attributes.len() + option_len(&rel_params)))?;

        let overridden_id_property_key = self
            .profile
            .api
            .find_special_property_name(SpecialProperty::IdOverride);

        for (name, serde_prop) in
            struct_op.filter_properties(self.mode, self.ctx.parent_property_id, self.profile.flags)
        {
            let unit_attr = UNIT_ATTR;
            let attribute = match attributes.get(&serde_prop.property_id) {
                Some(value) => value,
                None => {
                    if serde_prop.is_optional_for(self.mode, &self.profile.flags) {
                        continue;
                    } else {
                        match &self.ontology[serde_prop.value_addr] {
                            SerdeOperator::Struct(struct_op) => {
                                if struct_op.properties.is_empty() {
                                    &unit_attr
                                } else {
                                    panic!(
                                        "While serializing value {:?} with `{}`, the expected value was a non-empty struct, but found unit",
                                        value, &self.ontology[struct_op.typename]
                                    )
                                }
                            }
                            _ => {
                                panic!(
                                    "While serializing value {:?} with `{}`, property `{}` was not found.",
                                    value, &self.ontology[struct_op.typename], name
                                )
                            }
                        }
                    }
                }
            };

            let is_entity_id = serde_prop.is_entity_id();

            let name = match (is_entity_id, overridden_id_property_key) {
                (true, Some(id_key)) => id_key,
                _ => name.as_str(),
            };

            map.serialize_entry(
                name,
                &Proxy {
                    value: &attribute.val,
                    rel_params: attribute.rel.filter_non_unit(),
                    processor: self
                        .new_child_with_context(
                            serde_prop.value_addr,
                            SubProcessorContext {
                                is_update: false,
                                parent_property_id: Some(serde_prop.property_id),
                                parent_property_flags: serde_prop.flags,
                                rel_params_addr: serde_prop.rel_params_addr,
                            },
                        )
                        .map_err(RecursionLimitError::to_ser_error)?,
                },
            )?;
        }

        self.serialize_rel_params::<S>(rel_params, &mut map)?;

        if struct_op.flags.contains(SerdeStructFlags::OPEN_DATA)
            && self
                .profile
                .flags
                .contains(ProcessorProfileFlags::SERIALIZE_OPEN_DATA)
        {
            if let Some(open_data_attr) =
                attributes.get(&self.ontology.ontol_domain_meta().open_data_property_id())
            {
                let Value::Dict(dict, _) = &open_data_attr.val else {
                    panic!("Open data must be a dict");
                };

                for (key, value) in dict.iter() {
                    if struct_op.properties.contains_key(key) {
                        warn!("Open key `{key}` is shadowed in domain. Ignoring!");
                        continue;
                    }

                    map.serialize_entry(
                        key,
                        &RawProxy::new_as_child(value, self.ontology, self.level)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    )?;
                }
            }
        }

        map.end()
    }

    fn serialize_rel_params<S: Serializer>(
        &self,
        rel_params: Option<&Value>,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), <S as Serializer>::Error> {
        match (rel_params, self.ctx.rel_params_addr) {
            (None, None) => {}
            (Some(rel_params), Some(addr)) => {
                map.serialize_entry(
                    EDGE_PROPERTY,
                    &Proxy {
                        value: rel_params,
                        rel_params: None,
                        processor: self
                            .new_child(addr)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;
            }
            (None, Some(_)) => {
                panic!("Must serialize edge params, but was not present in attribute")
            }
            (Some(rel_params), None) => {
                panic!("Attribute had rel params {rel_params:#?}, but no serializer operator available: {self:#?}")
            }
        }

        Ok(())
    }
}

fn option_len<T>(opt: &Option<T>) -> usize {
    match opt {
        Some(_) => 1,
        None => 0,
    }
}

struct Proxy<'v, 'on, 'p> {
    value: &'v Value,
    rel_params: Option<&'v Value>,
    processor: SerdeProcessor<'on, 'p>,
}

impl<'v, 'on, 'p> serde::Serialize for Proxy<'v, 'on, 'p> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        self.processor
            .serialize_value(self.value, self.rel_params, serializer)
    }
}

fn cast_ref<T>(value: &Value) -> &<Value as Cast<T>>::Ref
where
    Value: Cast<T>,
{
    value.cast_ref()
}

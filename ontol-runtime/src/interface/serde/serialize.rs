use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serializer,
};
use smartstring::alias::String;
use std::fmt::Write;
use tracing::debug;

use crate::{
    cast::Cast,
    interface::serde::processor::RecursionLimitError,
    smart_format,
    text_pattern::FormatPattern,
    value::{Attribute, Data, FormatDataAsText, Value},
};

use super::{
    operator::{FilteredVariants, SequenceRange, SerdeOperator},
    processor::{SerdeProcessor, SubProcessorContext},
    StructOperator, EDGE_PROPERTY,
};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

const UNIT_ATTR: Attribute = Attribute {
    value: Value::unit(),
    rel_params: Value::unit(),
};

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    /// Serialize a value using this processor.
    pub fn serialize_value<S: Serializer>(
        &self,
        value: &Value,
        rel_params: Option<&Value>,
        serializer: S,
    ) -> Res<S> {
        debug!("serializing op={:?}", self.value_operator);
        match self.value_operator {
            SerdeOperator::Unit => {
                cast_ref::<()>(value);
                serializer.serialize_unit()
            }
            SerdeOperator::False(_) => serializer.serialize_bool(false),
            SerdeOperator::True(_) => serializer.serialize_bool(true),
            SerdeOperator::Boolean(_) => serializer.serialize_bool(*cast_ref::<bool>(value)),
            SerdeOperator::I64(..) => match &value.data {
                Data::I64(int) => serializer.serialize_i64(*int),
                Data::F64(f) => serializer.serialize_i64((*f).round() as i64),
                other => panic!("BUG: Serialize expected number, got {other:?}"),
            },
            SerdeOperator::I32(..) => {
                let int_i64: i64 = match &value.data {
                    Data::I64(int) => *int,
                    Data::F64(f) => (*f).round() as i64,
                    other => panic!("BUG: Serialize expected number, got {other:?}"),
                };

                serializer.serialize_i32(int_i64.try_into().map_err(|err| {
                    S::Error::custom(smart_format!("overflow when converting to i32: {err:?}"))
                })?)
            }
            SerdeOperator::F64(..) => match &value.data {
                Data::I64(num) => serializer.serialize_f64(*num as f64),
                Data::F64(f) => serializer.serialize_f64(*f),
                other => panic!("BUG: Serialize expected number, got {other:?}"),
            },
            SerdeOperator::String(def_id)
            | SerdeOperator::StringConstant(_, def_id)
            | SerdeOperator::TextPattern(def_id) => match &value.data {
                Data::Text(s) => serializer.serialize_str(s),
                data => {
                    let mut buf = String::new();
                    write!(
                        &mut buf,
                        "{}",
                        FormatDataAsText {
                            data,
                            type_def_id: *def_id,
                            ontology: self.ontology
                        }
                    )
                    .map_err(|_| S::Error::custom("Conversion to text failed"))?;

                    serializer.serialize_str(&buf)
                }
            },
            SerdeOperator::CapturingTextPattern(def_id) => {
                let pattern = &self.ontology.text_patterns.get(def_id).unwrap();
                let mut buf = String::new();
                write!(
                    &mut buf,
                    "{}",
                    FormatPattern {
                        pattern,
                        data: &value.data,
                        ontology: self.ontology
                    }
                )
                .map_err(|_| S::Error::custom("Failed to serialize capturing text pattern"))?;

                serializer.serialize_str(&buf)
            }
            SerdeOperator::Dynamic => match &value.data {
                Data::Text(string) => serializer.serialize_str(string),
                data @ Data::OctetSequence(_) => {
                    let mut buf = String::new();
                    write!(
                        &mut buf,
                        "{}",
                        FormatDataAsText {
                            data,
                            type_def_id: value.type_def_id,
                            ontology: self.ontology
                        }
                    )
                    .map_err(|_| S::Error::custom("Conversion to text failed"))?;

                    serializer.serialize_str(&buf)
                }
                Data::Sequence(vec) => self.serialize_dynamic_sequence(vec, serializer),
                _ => todo!(),
            },
            SerdeOperator::RelationSequence(seq_op) => {
                self.serialize_sequence(cast_ref::<Vec<_>>(value), &seq_op.ranges, serializer)
            }
            SerdeOperator::ConstructorSequence(seq_op) => {
                self.serialize_sequence(cast_ref::<Vec<_>>(value), &seq_op.ranges, serializer)
            }
            SerdeOperator::Alias(value_op) => self
                .narrow(value_op.inner_operator_id)
                .serialize_value(value, rel_params, serializer),
            SerdeOperator::Union(union_op) => match union_op.variants(self.mode, self.level) {
                FilteredVariants::Single(id) => self
                    .narrow(id)
                    .serialize_value(value, rel_params, serializer),
                FilteredVariants::Union(variants) => {
                    let variant = variants.iter().find(|discriminator| {
                        value.type_def_id == discriminator.discriminator.serde_def.def_id
                    });

                    if let Some(variant) = variant {
                        let processor = self.narrow(variant.operator_id);
                        debug!(
                            "serializing union variant with {:?} {processor:}",
                            variant.operator_id
                        );

                        processor.serialize_value(value, rel_params, serializer)
                    } else {
                        panic!("Discriminator not found while serializing union type");
                    }
                }
            },
            SerdeOperator::IdSingletonStruct(name, inner_operator_id) => {
                let mut map = serializer.serialize_map(Some(1 + option_len(&rel_params)))?;
                map.serialize_entry(
                    name,
                    &Proxy {
                        value,
                        rel_params: None,
                        processor: self
                            .new_child(*inner_operator_id)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;
                self.serialize_rel_params::<S>(rel_params, &mut map)?;

                map.end()
            }
            SerdeOperator::Struct(struct_op) => {
                self.serialize_struct(struct_op, rel_params, value, serializer)
            }
        }
    }
}

impl<'on, 'p> SerdeProcessor<'on, 'p> {
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
                        value: &attribute.value,
                        rel_params: attribute.rel_params.filter_non_unit(),
                        processor: self.narrow(range.operator_id),
                    })?;
                }
            } else {
                for attribute in element_iter.by_ref() {
                    seq.serialize_element(&Proxy {
                        value: &attribute.value,
                        rel_params: attribute.rel_params.filter_non_unit(),
                        processor: self.narrow(range.operator_id),
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
            let def_id = attr.value.type_def_id;
            match self.ontology.get_type_info(def_id).operator_id {
                Some(operator_id) => seq.serialize_element(&Proxy {
                    value: &attr.value,
                    rel_params: None,
                    processor: self.narrow(operator_id),
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
        let attributes = match &value.data {
            Data::Struct(attributes) => attributes,
            // Support for empty structs that are Unit encoded:
            Data::Unit => return serializer.serialize_map(Some(0))?.end(),
            other => panic!("BUG: Serialize expected map attributes, got {other:?}"),
        };

        let mut map = serializer.serialize_map(Some(attributes.len() + option_len(&rel_params)))?;

        for (name, serde_prop) in
            struct_op.filter_properties(self.mode, self.ctx.parent_property_id)
        {
            let unit_attr = UNIT_ATTR;
            let attribute = match attributes.get(&serde_prop.property_id) {
                Some(value) => value,
                None => {
                    if serde_prop.is_optional() {
                        continue;
                    } else {
                        match self
                            .ontology
                            .get_serde_operator(serde_prop.value_operator_id)
                        {
                            SerdeOperator::Struct(struct_op) => {
                                if struct_op.properties.is_empty() {
                                    &unit_attr
                                } else {
                                    panic!(
                                        "While serializing value {:?} with `{}`, the expected value was a non-empty struct, but found unit",
                                        value, struct_op.typename
                                    )
                                }
                            }
                            _ => {
                                panic!(
                                    "While serializing value {:?} with `{}`, property `{}` was not found.",
                                    value, struct_op.typename, name
                                )
                            }
                        }
                    }
                }
            };

            let is_entity_id = serde_prop.is_entity_id();

            let name = match (is_entity_id, self.profile.overridden_id_property_key) {
                (true, Some(id_key)) => id_key,
                _ => name.as_str(),
            };

            map.serialize_entry(
                name,
                &Proxy {
                    value: &attribute.value,
                    rel_params: attribute.rel_params.filter_non_unit(),
                    processor: self
                        .new_child_with_context(
                            if is_entity_id && self.profile.raw_ids {
                                self.ontology.dynamic_operator_id()
                            } else {
                                serde_prop.value_operator_id
                            },
                            SubProcessorContext {
                                parent_property_id: Some(serde_prop.property_id),
                                rel_params_operator_id: serde_prop.rel_params_operator_id,
                            },
                        )
                        .map_err(RecursionLimitError::to_ser_error)?,
                },
            )?;
        }

        self.serialize_rel_params::<S>(rel_params, &mut map)?;

        map.end()
    }

    fn serialize_rel_params<S: Serializer>(
        &self,
        rel_params: Option<&Value>,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), <S as Serializer>::Error> {
        match (rel_params, self.ctx.rel_params_operator_id) {
            (None, None) => {}
            (Some(rel_params), Some(operator_id)) => {
                map.serialize_entry(
                    EDGE_PROPERTY,
                    &Proxy {
                        value: rel_params,
                        rel_params: None,
                        processor: self
                            .new_child(operator_id)
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

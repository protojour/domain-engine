use serde::ser::{SerializeMap, SerializeSeq};
use smartstring::alias::String;

use crate::{
    cast::Cast,
    value::{Attribute, Data, Value},
};

use super::{MapType, SequenceRange, SerdeOperator, SerdeProcessor, EDGE_PROPERTY};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

impl<'e> SerdeProcessor<'e> {
    /// Serialize a value using this processor.
    pub fn serialize_value<S: serde::Serializer>(
        &self,
        value: &Value,
        edge_params: Option<&Value>,
        serializer: S,
    ) -> Res<S> {
        match self.value_operator {
            SerdeOperator::Unit => {
                cast_ref::<()>(value);
                serializer.serialize_unit()
            }
            SerdeOperator::Int(_) | SerdeOperator::Number(_) => {
                self.serialize_number(value, serializer)
            }
            SerdeOperator::String(_) | SerdeOperator::StringConstant(_, _) => {
                serializer.serialize_str(cast_ref::<String>(value))
            }
            SerdeOperator::Sequence(ranges, _) => {
                self.serialize_sequence(cast_ref::<Vec<_>>(value), ranges, serializer)
            }
            SerdeOperator::ValueType(value_type) => self
                .env
                .new_serde_processor(value_type.inner_operator_id)
                .serialize_value(value, edge_params, serializer),
            SerdeOperator::ValueUnionType(value_union_type) => {
                let discriminator = value_union_type
                    .discriminators
                    .iter()
                    .find(|discriminator| {
                        value.type_def_id == discriminator.discriminator.result_type
                    });

                match discriminator {
                    Some(discriminator) => self
                        .env
                        .new_serde_processor(discriminator.operator_id)
                        .serialize_value(value, edge_params, serializer),
                    None => {
                        panic!("Discriminator not found while serializing union type");
                    }
                }
            }
            SerdeOperator::MapType(map_type) => {
                self.serialize_map(map_type, edge_params, value, serializer)
            }
        }
    }
}

impl<'e> SerdeProcessor<'e> {
    fn serialize_number<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match &value.data {
            Data::Int(num) => serializer.serialize_i64(*num),
            Data::Float(f) => serializer.serialize_f64(*f),
            other => panic!("BUG: Serialize expected number, got {other:?}"),
        }
    }

    fn serialize_sequence<S: serde::Serializer>(
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
                        edge_params: attribute.edge_params.filter_non_unit(),
                        processor: self.env.new_serde_processor_with_edge(
                            range.operator_id,
                            self.edge_operator_id,
                        ),
                    })?;
                }
            } else {
                for attribute in element_iter.by_ref() {
                    seq.serialize_element(&Proxy {
                        value: &attribute.value,
                        edge_params: attribute.edge_params.filter_non_unit(),
                        processor: self.env.new_serde_processor_with_edge(
                            range.operator_id,
                            self.edge_operator_id,
                        ),
                    })?;
                }
            }
        }

        seq.end()
    }

    fn serialize_map<S: serde::Serializer>(
        &self,
        map_type: &MapType,
        edge_params: Option<&Value>,
        value: &Value,
        serializer: S,
    ) -> Res<S> {
        let attributes = match &value.data {
            Data::Map(attributes) => attributes,
            other => panic!("BUG: Serialize expected map attributes, got {other:?}"),
        };

        let mut serialize_map = serializer.serialize_map(Some(attributes.len()))?;

        for (name, serde_prop) in &map_type.properties {
            let attribute = match attributes.get(&serde_prop.property_id) {
                Some(value) => value,
                None => {
                    if serde_prop.optional {
                        continue;
                    } else {
                        panic!(
                            "While serializing `{}`, property `{}` was not found",
                            map_type.typename, name
                        )
                    }
                }
            };

            serialize_map.serialize_entry(
                name,
                &Proxy {
                    value: &attribute.value,
                    edge_params: attribute.edge_params.filter_non_unit(),
                    processor: self.env.new_serde_processor_with_edge(
                        serde_prop.value_operator_id,
                        serde_prop.edge_operator_id,
                    ),
                },
            )?;
        }

        match (edge_params, self.edge_operator_id) {
            (None, None) => {}
            (Some(edge_params), Some(operator_id)) => {
                serialize_map.serialize_entry(
                    EDGE_PROPERTY,
                    &Proxy {
                        value: edge_params,
                        edge_params: None,
                        processor: self.env.new_serde_processor(operator_id),
                    },
                )?;
            }
            (None, Some(_)) => {
                panic!("Must serialize edge params, but attribute did not contain anything")
            }
            (Some(edge_params), None) => {
                panic!("Attribute had edge params {edge_params:?}, but no serializer operator available: {self:?}")
            }
        }

        serialize_map.end()
    }
}

struct Proxy<'v, 'e> {
    value: &'v Value,
    edge_params: Option<&'v Value>,
    processor: SerdeProcessor<'e>,
}

impl<'v, 'e> serde::Serialize for Proxy<'v, 'e> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        self.processor
            .serialize_value(self.value, self.edge_params, serializer)
    }
}

fn cast_ref<T>(value: &Value) -> &<Value as Cast<T>>::Ref
where
    Value: Cast<T>,
{
    value.cast_ref()
}

use serde::ser::{SerializeMap, SerializeSeq};

use crate::value::{Data, Value};

use super::{MapType, SerdeOperator, SerdeOperatorId, SerdeProcessor};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

impl<'e> SerdeProcessor<'e> {
    /// Serialize a value using this processor.
    pub fn serialize_value<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match self.current {
            SerdeOperator::Unit => {
                panic!("Tried to serialize unit");
            }
            SerdeOperator::Number(_) => self.serialize_number(value, serializer),
            SerdeOperator::String(_) => self.serialize_string(value, serializer),
            SerdeOperator::StringConstant(_, _) => self.serialize_string(value, serializer),
            SerdeOperator::Tuple(operator_ids, _) => {
                self.serialize_tuple(value, operator_ids, serializer)
            }
            SerdeOperator::ValueType(value_type) => self
                .registry
                .make_processor(value_type.property.operator_id)
                .serialize_value(value, serializer),
            SerdeOperator::ValueUnionType(_) => {
                panic!("Should not happen: Serialized value should be a concrete type, not a union type");
            }
            SerdeOperator::MapType(map_type) => self.serialize_map(map_type, value, serializer),
        }
    }
}

impl<'e> SerdeProcessor<'e> {
    fn serialize_number<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match &value.data {
            Data::Number(num) => serializer.serialize_i64(*num),
            other => panic!("BUG: Serialize expected number, got {other:?}"),
        }
    }

    fn serialize_string<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match &value.data {
            Data::String(string) => serializer.serialize_str(string),
            other => panic!("BUG: Serialize expected string, got {other:?}"),
        }
    }

    fn serialize_tuple<S: serde::Serializer>(
        &self,
        value: &Value,
        operator_ids: &[SerdeOperatorId],
        serializer: S,
    ) -> Res<S> {
        match &value.data {
            Data::Vec(elements) => {
                let mut seq = serializer.serialize_seq(Some(elements.len()))?;
                for (value, operator_id) in elements.iter().zip(operator_ids) {
                    seq.serialize_element(&Proxy {
                        value,
                        processor: self.registry.make_processor(*operator_id),
                    })?;
                }

                seq.end()
            }
            other => panic!("BUG: Serialize expected vector, got {other:?}"),
        }
    }

    fn serialize_map<S: serde::Serializer>(
        &self,
        map_type: &MapType,
        value: &Value,
        serializer: S,
    ) -> Res<S> {
        let attributes = match &value.data {
            Data::Map(attributes) => attributes,
            other => panic!("BUG: Serialize expected compound attributes, got {other:?}"),
        };

        let mut serialize_map = serializer.serialize_map(Some(attributes.len()))?;

        for (name, serde_prop) in &map_type.properties {
            let value = match attributes.get(&serde_prop.property_id) {
                Some(value) => value,
                None => panic!(
                    "While serializing `{}`, property `{}` was not found",
                    map_type.typename, name
                ),
            };

            serialize_map.serialize_entry(
                name,
                &Proxy {
                    value,
                    processor: self.registry.make_processor(serde_prop.operator_id),
                },
            )?;
        }
        serialize_map.end()
    }
}

struct Proxy<'v, 'e> {
    value: &'v Value,
    processor: SerdeProcessor<'e>,
}

impl<'v, 'e> serde::Serialize for Proxy<'v, 'e> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        self.processor.serialize_value(self.value, serializer)
    }
}

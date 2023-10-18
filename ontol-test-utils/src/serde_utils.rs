use std::collections::BTreeMap;

use ontol_runtime::{
    interface::serde::processor::{ProcessorLevel, ProcessorMode},
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use serde::de::DeserializeSeed;
use tracing::error;

use crate::type_binding::{TypeBinding, TEST_JSON_SCHEMA_VALIDATION};

pub struct Deserializer<'b, 'on> {
    binding: &'b TypeBinding<'on>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'b, 'on> Deserializer<'b, 'on> {
    pub fn data(&self, json: serde_json::Value) -> Result<Data, serde_json::Error> {
        let value = self.value(json)?;
        assert_eq!(value.type_def_id, self.binding.type_info.def_id);
        Ok(value.data)
    }

    pub fn data_map(
        &self,
        json: serde_json::Value,
    ) -> Result<BTreeMap<PropertyId, Attribute>, serde_json::Error> {
        let value = self.value(json)?;
        assert_eq!(value.type_def_id, self.binding.type_info.def_id);
        match value.data {
            Data::Struct(attrs) => Ok(attrs),
            other => panic!("not a map: {other:?}"),
        }
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    pub fn data_variant(&self, json: serde_json::Value) -> Result<Data, serde_json::Error> {
        let value = self.value(json)?;
        assert_ne!(value.type_def_id, self.binding.type_info.def_id);
        Ok(value.data)
    }

    pub fn value(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();

        let attribute_result = self
            .binding
            .ontology()
            .new_serde_processor(self.binding.serde_operator_id(), self.mode, self.level)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string));

        match self.binding.json_schema() {
            Some(json_schema) if TEST_JSON_SCHEMA_VALIDATION => {
                let json_schema_result = json_schema.validate(&json);

                match (attribute_result, json_schema_result) {
                    (Ok(Attribute { value, rel_params }), Ok(())) => {
                        assert_eq!(rel_params.type_def_id, DefId::unit());

                        Ok(value)
                    }
                    (Err(json_error), Err(_)) => Err(json_error),
                    (Ok(_), Err(validation_errors)) => {
                        for error in validation_errors {
                            error!("JSON schema error: {error}");
                        }
                        panic!("BUG: JSON schema did not accept input {json_string}");
                    }
                    (Err(json_error), Ok(())) => {
                        panic!(
                            "BUG: Deserializer did not accept input, but JSONSchema did: {json_error:?}. input={json_string}"
                        );
                    }
                }
            }
            _ => attribute_result.map(|Attribute { value, rel_params }| {
                assert_eq!(rel_params.type_def_id, DefId::unit());

                value
            }),
        }
    }
}

pub struct Serializer<'b, 'on> {
    binding: &'b TypeBinding<'on>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'b, 'on> Serializer<'b, 'on> {
    pub fn json(&self, value: &Value) -> serde_json::Value {
        self.serialize_json(value, false)
    }

    pub fn data_json(&self, data: &Data) -> serde_json::Value {
        self.json(&Value::new(data.clone(), self.binding.type_info.def_id))
    }

    pub fn dynamic_sequence_json(&self, value: &Value) -> serde_json::Value {
        self.serialize_json(value, true)
    }

    fn serialize_json(&self, value: &Value, dynamic_seq: bool) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        self.binding
            .ontology()
            .new_serde_processor(
                if dynamic_seq {
                    self.binding.ontology().dynamic_sequence_operator_id()
                } else {
                    self.binding.serde_operator_id()
                },
                self.mode,
                self.level,
            )
            .serialize_value(value, None, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

/// Make a deserializer for the data creation processor mode
pub fn create_de<'b, 'on>(binding: &'b TypeBinding<'on>) -> Deserializer<'b, 'on> {
    Deserializer {
        binding,
        mode: ProcessorMode::Create,
        level: ProcessorLevel::new_root(),
    }
}

/// Make a deserializer for the `Read` processor mode
pub fn read_de<'b, 'on>(binding: &'b TypeBinding<'on>) -> Deserializer<'b, 'on> {
    Deserializer {
        binding,
        mode: ProcessorMode::Read,
        level: ProcessorLevel::new_root(),
    }
}

/// Make a deserializer for the `Raw` processor mode
pub fn raw_de<'b, 'on>(binding: &'b TypeBinding<'on>) -> Deserializer<'b, 'on> {
    Deserializer {
        binding,
        mode: ProcessorMode::Raw,
        level: ProcessorLevel::new_root(),
    }
}

/// Make a serializer for the data creation processor mode
pub fn create_ser<'b, 'on>(binding: &'b TypeBinding<'on>) -> Serializer<'b, 'on> {
    Serializer {
        binding,
        mode: ProcessorMode::Create,
        level: ProcessorLevel::new_root(),
    }
}

/// Make a serializer for the `Read` processor mode
pub fn read_ser<'b, 'on>(binding: &'b TypeBinding<'on>) -> Serializer<'b, 'on> {
    Serializer {
        binding,
        mode: ProcessorMode::Read,
        level: ProcessorLevel::new_root(),
    }
}

/// Make a serializer for the `Raw` processor mode
pub fn raw_ser<'b, 'on>(binding: &'b TypeBinding<'on>) -> Serializer<'b, 'on> {
    Serializer {
        binding,
        mode: ProcessorMode::Raw,
        level: ProcessorLevel::new_root(),
    }
}

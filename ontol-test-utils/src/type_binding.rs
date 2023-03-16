use std::collections::BTreeMap;

use jsonschema::JSONSchema;
use ontol_runtime::{
    env::{Env, TypeInfo},
    json_schema::build_standalone_schema,
    serde::operator::SerdeOperatorId,
    serde::processor::{ProcessorLevel, ProcessorMode},
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use serde::de::DeserializeSeed;
use tracing::{debug, error};

use crate::TEST_PKG;

/// This test asserts that JSON schemas accept the same things that
/// ONTOL's own deserializer does.
///
/// Currently we can't run these in the test suite because of missing implementation in the validator:
/// https://github.com/Stranger6667/jsonschema-rs/issues/288
const TEST_JSON_SCHEMA_VALIDATION: bool = false;

pub struct TypeBinding<'e> {
    pub type_info: TypeInfo,
    json_schema: JSONSchema,
    env: &'e Env,
}

impl<'e> TypeBinding<'e> {
    pub fn new(env: &'e Env, type_name: &str) -> Self {
        let domain = env.find_domain(&TEST_PKG).unwrap();
        let def_id = domain
            .type_names
            .get(type_name)
            .unwrap_or_else(|| panic!("type name not found: `{type_name}`"));
        let type_info = domain.type_info(*def_id).clone();

        debug!(
            "TypeBinding::new `{type_name}` with {operator_id:?} create={processor:?}",
            operator_id = type_info.operator_id,
            processor = type_info.operator_id.map(|id| env.new_serde_processor(
                id,
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root
            ))
        );

        let json_schema = compile_json_schema(env, &type_info);

        Self {
            type_info,
            json_schema,
            env,
        }
    }

    pub fn env(&self) -> &Env {
        self.env
    }

    pub fn value_builder(&self, data: serde_json::Value) -> ValueBuilder<'_, 'e> {
        ValueBuilder {
            binding: self,
            value: Value::unit(),
        }
        .data(data)
    }

    pub fn entity_builder(
        &self,
        id: serde_json::Value,
        data: serde_json::Value,
    ) -> ValueBuilder<'_, 'e> {
        ValueBuilder {
            binding: self,
            value: Value::unit(),
        }
        .id(id)
        .data(data)
    }

    fn serde_operator_id(&self) -> SerdeOperatorId {
        self.type_info.operator_id.expect("No serde operator id")
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.env
            .new_serde_processor(
                self.serde_operator_id(),
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .find_property(prop)
    }

    pub fn deserialize_data(&self, json: serde_json::Value) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_eq!(value.type_def_id, self.type_info.def_id);
        Ok(value.data)
    }

    pub fn deserialize_data_map(
        &self,
        json: serde_json::Value,
    ) -> Result<BTreeMap<PropertyId, Attribute>, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_eq!(value.type_def_id, self.type_info.def_id);
        match value.data {
            Data::Map(map) => Ok(map),
            other => panic!("not a map: {other:?}"),
        }
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    pub fn deserialize_data_variant(
        &self,
        json: serde_json::Value,
    ) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_ne!(value.type_def_id, self.type_info.def_id);
        Ok(value.data)
    }

    pub fn deserialize_value(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();

        let attribute_result = self
            .env
            .new_serde_processor(
                self.serde_operator_id(),
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string));

        if TEST_JSON_SCHEMA_VALIDATION {
            let json_schema_result = self.json_schema.validate(&json);

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
        } else {
            attribute_result.map(|Attribute { value, rel_params }| {
                assert_eq!(rel_params.type_def_id, DefId::unit());

                value
            })
        }
    }

    pub fn serialize_data_json(&self, data: &Data) -> serde_json::Value {
        self.serialize_json(&Value::new(data.clone(), self.type_info.def_id))
    }

    pub fn serialize_json(&self, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        self.env
            .new_serde_processor(
                self.serde_operator_id(),
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .serialize_value(value, None, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

fn compile_json_schema(env: &Env, type_info: &TypeInfo) -> JSONSchema {
    let standalone_schema = build_standalone_schema(env, type_info, ProcessorMode::Create).unwrap();

    debug!(
        "outputted json schema: {}",
        serde_json::to_string_pretty(&standalone_schema).unwrap()
    );

    JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .compile(&serde_json::to_value(&standalone_schema).unwrap())
        .unwrap()
}

#[derive(Clone)]
pub struct ValueBuilder<'t, 'e> {
    binding: &'t TypeBinding<'e>,
    value: Value,
}

impl<'t, 'e> From<ValueBuilder<'t, 'e>> for Value {
    fn from(b: ValueBuilder<'t, 'e>) -> Self {
        b.value
    }
}

impl<'t, 'e> From<ValueBuilder<'t, 'e>> for Attribute {
    fn from(b: ValueBuilder<'t, 'e>) -> Attribute {
        b.to_unit_attribute()
    }
}

impl<'t, 'e> ValueBuilder<'t, 'e> {
    pub fn relationship(self, name: &str, attribute: Attribute) -> Self {
        let property_id = self.binding.find_property(name).expect("unknown property");
        self.merge_attribute(property_id, attribute)
    }

    pub fn to_unit_attribute(self) -> Attribute {
        self.value.to_attribute(Value::unit())
    }

    pub fn to_attribute(self, rel_params: impl Into<Value>) -> Attribute {
        self.value.to_attribute(rel_params.into())
    }

    fn data(mut self, json: serde_json::Value) -> Self {
        let value = self.binding.deserialize_value(json).unwrap();
        match (&mut self.value.data, value) {
            (Data::Unit, value) => {
                self.value = value;
            }
            (
                Data::Map(map_a),
                Value {
                    data: Data::Map(map_b),
                    ..
                },
            ) => {
                map_a.extend(map_b);
            }
            (a, b) => panic!("Unable to merge {a:?} and {b:?}"),
        }
        self
    }

    fn id(self, json: serde_json::Value) -> Self {
        let entity_info = self
            .binding
            .type_info
            .entity_info
            .as_ref()
            .expect("Not an entity!");
        let id = self
            .binding
            .env
            .new_serde_processor(
                entity_info.id_operator_id,
                None,
                ProcessorMode::Create,
                ProcessorLevel::Root,
            )
            .deserialize(&mut serde_json::Deserializer::from_str(
                &serde_json::to_string(&json).unwrap(),
            ))
            .unwrap();

        self.merge_attribute(PropertyId::subject(entity_info.id_relation_id), id)
    }

    fn merge_attribute(mut self, property_id: PropertyId, attribute: Attribute) -> Self {
        match &mut self.value.data {
            Data::Map(map) => {
                map.insert(property_id, attribute);
            }
            Data::Unit => self.value.data = Data::Map([(property_id, attribute)].into()),
            other => {
                panic!("Value data was not a map/unit, but {other:?}.")
            }
        }
        self
    }
}

pub trait ToSequence {
    fn to_sequence_attribute(self, ty: &TypeBinding) -> Attribute;
}

impl ToSequence for Vec<Attribute> {
    fn to_sequence_attribute(self, ty: &TypeBinding) -> Attribute {
        Value {
            data: Data::Sequence(self),
            type_def_id: ty.type_info.def_id,
        }
        .to_attribute(Value::unit())
    }
}

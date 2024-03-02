use fnv::FnvHashMap;
use ontol_runtime::{
    interface::serde::processor::{
        ProcessorLevel, ProcessorMode, ProcessorProfile, ProcessorProfileApi,
        ProcessorProfileFlags, SpecialProperty,
    },
    value::{Attribute, PropertyId, Value},
    DefId,
};
use serde::de::DeserializeSeed;
use tracing::error;

use crate::type_binding::{TypeBinding, TEST_JSON_SCHEMA_VALIDATION};

/// A trait used in JSON tests.
/// using this trait one can pass a serde_json::Value or a static JSON str.
///
/// The advantage of passing a static str is that then the order of object properties is strictly defined.
///
/// The deserializers under test could be sensitive to the order of JSON properties due to subtle bugs,
/// which is what we want to test.
pub trait JsonConvert {
    fn to_json_string(&self) -> String;
    fn to_json_value(&self) -> serde_json::Value;
}

impl JsonConvert for serde_json::Value {
    fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn to_json_value(&self) -> serde_json::Value {
        self.clone()
    }
}

impl<'a> JsonConvert for &'a str {
    fn to_json_string(&self) -> String {
        self.to_string()
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::from_str(self).unwrap()
    }
}

pub struct SerdeHelper<'b, 'on, 'p> {
    binding: &'b TypeBinding<'on>,
    mode: ProcessorMode,
    level: ProcessorLevel,
    profile: ProcessorProfile<'p>,
}

impl<'b, 'on, 'p> SerdeHelper<'b, 'on, 'p> {
    pub fn with_profile(self, profile: ProcessorProfile<'p>) -> Self {
        Self { profile, ..self }
    }

    pub fn enable_open_data(self) -> Self {
        Self {
            profile: ProcessorProfile {
                flags: self.profile.flags
                    | ProcessorProfileFlags::SERIALIZE_OPEN_DATA
                    | ProcessorProfileFlags::DESERIALIZE_OPEN_DATA,
                ..self.profile
            },
            ..self
        }
    }

    #[track_caller]
    pub fn to_value(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let value = self.to_value_nocheck(json)?;
        assert_eq!(value.type_def_id(), self.binding.type_info.def_id);
        Ok(value)
    }

    #[track_caller]
    pub fn to_value_map(
        &self,
        json: serde_json::Value,
    ) -> Result<FnvHashMap<PropertyId, Attribute>, serde_json::Error> {
        let value = self.to_value_nocheck(json)?;
        assert_eq!(value.type_def_id(), self.binding.type_info.def_id);
        match value {
            Value::Struct(attrs, _) => Ok(*attrs),
            other => panic!("not a map: {other:?}"),
        }
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    #[track_caller]
    pub fn to_value_variant(&self, json: impl JsonConvert) -> Result<Value, serde_json::Error> {
        let value = self.to_value_nocheck(json)?;
        assert_ne!(value.type_def_id(), self.binding.type_info.def_id);
        Ok(value)
    }

    /// Deserialize to value, do not run type_def_id assert checks
    pub fn to_value_nocheck(&self, json: impl JsonConvert) -> Result<Value, serde_json::Error> {
        let json_string = json.to_json_string();

        let attribute_result = self
            .binding
            .ontology()
            .new_serde_processor(self.binding.serde_operator_addr(), self.mode)
            .with_level(self.level)
            .with_profile(&self.profile)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string));

        match self.binding.json_schema() {
            Some(json_schema) if TEST_JSON_SCHEMA_VALIDATION => {
                let json_schema_result =
                    json_schema
                        .validate(&json.to_json_value())
                        .map_err(|validation_errors| {
                            for error in validation_errors {
                                error!("JSON schema error: {error}");
                            }
                        });

                match (attribute_result, json_schema_result) {
                    (Ok(Attribute { rel, val }), Ok(())) => {
                        assert_eq!(rel.type_def_id(), DefId::unit());

                        Ok(val)
                    }
                    (Err(json_error), Err(_)) => Err(json_error),
                    (Ok(_), Err(_schema_error)) => {
                        panic!("BUG: JSON schema did not accept input {json_string}");
                    }
                    (Err(json_error), Ok(())) => {
                        panic!(
                            "BUG: Deserializer did not accept input, but JSONSchema did: {json_error:?}. input={json_string}"
                        );
                    }
                }
            }
            _ => attribute_result.map(|Attribute { rel, val }| {
                assert_eq!(rel.type_def_id(), DefId::unit());

                val
            }),
        }
    }

    pub fn as_json(&self, value: &Value) -> serde_json::Value {
        self.serialize_json(value, false)
    }

    pub fn dynamic_seq_as_json(&self, value: &Value) -> serde_json::Value {
        self.serialize_json(value, true)
    }

    fn serialize_json(&self, value: &Value, dynamic_seq: bool) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        self.binding
            .ontology()
            .new_serde_processor(
                if dynamic_seq {
                    self.binding.ontology().dynamic_sequence_operator_addr()
                } else {
                    self.binding.serde_operator_addr()
                },
                self.mode,
            )
            .with_level(self.level)
            .with_profile(&self.profile)
            .serialize_value(value, None, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

/// Make a helper for the data creation processor mode
pub fn serde_create<'b, 'on, 'p>(binding: &'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p> {
    SerdeHelper {
        binding,
        mode: ProcessorMode::Create,
        level: ProcessorLevel::new_root(),
        profile: ProcessorProfile::default(),
    }
}

/// Make a helper for the `Read` processor mode
pub fn serde_read<'b, 'on, 'p>(binding: &'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p> {
    SerdeHelper {
        binding,
        mode: ProcessorMode::Read,
        level: ProcessorLevel::new_root(),
        profile: ProcessorProfile::default(),
    }
}

/// Make a helper for the `Raw` processor mode
pub fn serde_raw<'b, 'on, 'p>(binding: &'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p> {
    SerdeHelper {
        binding,
        mode: ProcessorMode::Raw,
        level: ProcessorLevel::new_root(),
        profile: ProcessorProfile::default(),
    }
}

pub fn serde_raw_tree_only<'b, 'on, 'p>(binding: &'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p> {
    SerdeHelper {
        binding,
        mode: ProcessorMode::RawTreeOnly,
        level: ProcessorLevel::new_root(),
        profile: ProcessorProfile::default(),
    }
}

pub struct ProcessorProfileTestPlugin {
    pub prop_overrides: FnvHashMap<&'static str, SpecialProperty>,
    pub annotations: FnvHashMap<serde_value::Value, DefId>,
}

impl ProcessorProfileApi for ProcessorProfileTestPlugin {
    fn lookup_special_property(&self, key: &str) -> Option<SpecialProperty> {
        self.prop_overrides.get(key).cloned()
    }

    fn find_special_property_name(&self, prop: SpecialProperty) -> Option<&str> {
        for (key, candidate) in &self.prop_overrides {
            if *candidate == prop {
                return Some(key);
            }
        }

        None
    }

    fn annotate_type(&self, input: &serde_value::Value) -> Option<DefId> {
        self.annotations.get(input).cloned()
    }
}

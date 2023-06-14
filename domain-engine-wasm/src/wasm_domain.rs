use std::sync::Arc;

use ontol_faker::new_constant_fake;
use ontol_runtime::{
    env::{Env, TypeInfo},
    json_schema::build_openapi_schemas,
    serde::{
        operator::SerdeOperatorId,
        processor::{ProcessorLevel, ProcessorMode},
    },
    value::Value,
    vm::proc::Procedure,
    MapKey, PackageId,
};
use serde::de::DeserializeSeed;
use wasm_bindgen::prelude::*;

use crate::{wasm_error::WasmError, wasm_util::js_serializer};

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum JsonFormat {
    Create,
    Update,
    Select,
}

impl JsonFormat {
    pub(crate) fn to_processor_mode(self) -> ProcessorMode {
        match self {
            Self::Create => ProcessorMode::Create,
            Self::Update => ProcessorMode::Update,
            Self::Select => ProcessorMode::Select,
        }
    }
}

#[wasm_bindgen]
pub struct WasmDomain {
    pub(crate) package_id: PackageId,
    pub(crate) env: Arc<Env>,
}

#[wasm_bindgen]
impl WasmDomain {
    pub fn package_id(&self) -> u16 {
        self.package_id.0
    }

    pub fn pub_types(&self) -> Vec<JsValue> {
        let domain = self.env.find_domain(self.package_id).unwrap();
        domain
            .type_infos()
            .filter(|type_info| type_info.public)
            .map(|type_info| WasmTypeInfo {
                inner: type_info.clone(),
                env: self.env.clone(),
            })
            .map(JsValue::from)
            .collect()
    }

    pub fn serialize_json_types(&self) -> Result<String, WasmError> {
        let domain = self.env.find_domain(self.package_id).unwrap();
        let schemas = build_openapi_schemas(self.env.as_ref(), self.package_id, &domain);
        let schemas_json = serde_json::to_string_pretty(&schemas).unwrap();
        Ok(schemas_json)
    }

    pub fn serialize_yaml_types(&self) -> Result<String, WasmError> {
        let domain = self.env.find_domain(self.package_id).unwrap();
        let schemas = build_openapi_schemas(self.env.as_ref(), self.package_id, &domain);
        let schemas_yaml = serde_yaml::to_string(&schemas).unwrap();
        Ok(schemas_yaml)
    }
}

#[wasm_bindgen]
pub struct WasmTypeInfo {
    pub(crate) inner: TypeInfo,
    pub(crate) env: Arc<Env>,
}

#[wasm_bindgen]
impl WasmTypeInfo {
    pub fn name(&self) -> String {
        match &self.inner.name {
            Some(name) => name.as_str().into(),
            None => "".to_string(),
        }
    }

    pub fn domain(&self) -> Option<WasmDomain> {
        let package_id = self.inner.def_id.package_id();
        self.env.find_domain(package_id).map(|_| WasmDomain {
            package_id,
            env: self.env.clone(),
        })
    }

    pub fn new_value(&self, js_value: JsValue, format: JsonFormat) -> Result<WasmValue, WasmError> {
        let value = self
            .env
            .new_serde_processor(
                operator_id(&self.inner)?,
                None,
                format.to_processor_mode(),
                ProcessorLevel::Root,
            )
            .deserialize(serde_wasm_bindgen::Deserializer::from(js_value))
            .map(|attr| attr.value)
            .map_err(|err| WasmError::Generic(format!("Deserialization failed: {err}")))?;

        Ok(WasmValue {
            value,
            env: self.env.clone(),
        })
    }

    pub fn fake_value(&self, format: JsonFormat) -> WasmValue {
        let fake_value =
            new_constant_fake(&self.env, self.inner.def_id, format.to_processor_mode()).unwrap();

        WasmValue {
            value: fake_value,
            env: self.env.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct WasmValue {
    value: Value,
    env: Arc<Env>,
}

#[wasm_bindgen]
impl WasmValue {
    pub fn as_js(&self, format: JsonFormat) -> Result<JsValue, WasmError> {
        let type_info = self.env.get_type_info(self.value.type_def_id);

        self.env
            .new_serde_processor(
                operator_id(type_info)?,
                None,
                format.to_processor_mode(),
                ProcessorLevel::Root,
            )
            .serialize_value(&self.value, None, &js_serializer())
            .map_err(|err| WasmError::Generic(format!("Serialization failed: {err}")))
    }
}

#[wasm_bindgen]
pub struct WasmMapper {
    pub(crate) from: MapKey,
    pub(crate) to: MapKey,
    pub(crate) procedure: Procedure,
    pub(crate) env: Arc<Env>,
}

#[wasm_bindgen]
impl WasmMapper {
    pub fn from(&self) -> WasmTypeInfo {
        let type_info = self.env.get_type_info(self.from.def_id);
        WasmTypeInfo {
            inner: type_info.clone(),
            env: self.env.clone(),
        }
    }

    pub fn to(&self) -> WasmTypeInfo {
        let type_info = self.env.get_type_info(self.to.def_id);
        WasmTypeInfo {
            inner: type_info.clone(),
            env: self.env.clone(),
        }
    }

    pub fn map(&self, input: &WasmValue) -> Result<WasmValue, WasmError> {
        let proc = self.env.get_mapper_proc(self.from, self.to).unwrap();
        let mut mapper = self.env.new_vm();

        let value = mapper.eval(proc, [input.value.clone()]);

        Ok(WasmValue {
            value,
            env: self.env.clone(),
        })
    }
}

pub(crate) fn operator_id(type_info: &TypeInfo) -> Result<SerdeOperatorId, WasmError> {
    if let Some(operator_id) = type_info.operator_id {
        Ok(operator_id)
    } else {
        Err(WasmError::Generic(format!(
            "The type `{}` cannot be serialized",
            type_info.name.as_deref().unwrap_or("")
        )))
    }
}

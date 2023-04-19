use std::sync::Arc;

use ontol_faker::new_constant_fake;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{
        operator::SerdeOperatorId,
        processor::{ProcessorLevel, ProcessorMode},
    },
    value::Value,
    PackageId,
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

    pub fn fake_create_json(&self, format: JsonFormat) -> Result<JsValue, WasmError> {
        let fake_value =
            new_constant_fake(&self.env, self.inner.def_id, ProcessorMode::Create).unwrap();

        self.value_to_js(&fake_value, format)
    }

    pub(crate) fn operator_id(&self) -> Result<SerdeOperatorId, WasmError> {
        if let Some(operator_id) = self.inner.operator_id {
            Ok(operator_id)
        } else {
            Err(WasmError {
                msg: format!("The type `{}` cannot be serialized", self.name()),
            })
        }
    }

    pub(crate) fn value_from_js(
        &self,
        js_value: JsValue,
        format: JsonFormat,
    ) -> Result<Value, WasmError> {
        self.env
            .new_serde_processor(
                self.operator_id()?,
                None,
                format.to_processor_mode(),
                ProcessorLevel::Root,
            )
            .deserialize(serde_wasm_bindgen::Deserializer::from(js_value))
            .map(|attr| attr.value)
            .map_err(|err| WasmError {
                msg: format!("Deserialization failed: {err}"),
            })
    }

    pub(crate) fn value_to_js(
        &self,
        value: &Value,
        format: JsonFormat,
    ) -> Result<JsValue, WasmError> {
        self.env
            .new_serde_processor(
                self.operator_id()?,
                None,
                format.to_processor_mode(),
                ProcessorLevel::Root,
            )
            .serialize_value(value, None, &js_serializer())
            .map_err(|err| WasmError {
                msg: format!("Serialization failed: {err}"),
            })
    }
}

use ontol_faker::new_constant_fake;
use ontol_runtime::{
    interface::json_schema::build_openapi_schemas,
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{MapMeta, Ontology, TypeInfo},
    value::Value,
    vm::VmState,
    MapKey, PackageId,
};
use serde::de::DeserializeSeed;
use wasm_bindgen::prelude::*;

use crate::{
    wasm_error::{WasmError, WasmResult},
    wasm_gc::WasmWeak,
    wasm_graphql::WasmGraphqlSchema,
    wasm_util::js_serializer,
};

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum JsonFormat {
    Create,
    Read,
    Update,
    Raw,
}

impl JsonFormat {
    pub(crate) fn to_processor_mode(self) -> ProcessorMode {
        match self {
            Self::Create => ProcessorMode::Create,
            Self::Update => ProcessorMode::Update,
            Self::Read => ProcessorMode::Read,
            Self::Raw => ProcessorMode::Raw,
        }
    }
}

#[wasm_bindgen]
pub struct WasmDomain {
    pub(crate) package_id: PackageId,
    pub(crate) weak_ontology: WasmWeak<Ontology>,
}

#[wasm_bindgen]
impl WasmDomain {
    pub fn package_id(&self) -> u16 {
        self.package_id.0
    }

    pub fn unique_name(&self) -> WasmResult<String> {
        Ok(self
            .weak_ontology
            .upgrade()?
            .find_domain(self.package_id)
            .unwrap()
            .unique_name
            .as_str()
            .to_owned())
    }

    pub fn pub_types(&self) -> WasmResult<Vec<JsValue>> {
        let ontology = self.weak_ontology.upgrade()?;
        let domain = ontology.find_domain(self.package_id).unwrap();

        Ok(domain
            .type_infos()
            .filter(|type_info| type_info.public)
            .map(|type_info| WasmTypeInfo {
                inner: type_info.clone(),
                weak_ontology: self.weak_ontology.clone(),
            })
            .map(JsValue::from)
            .collect())
    }

    pub fn serialize_json_types(&self) -> WasmResult<String> {
        let ontology = self.weak_ontology.upgrade()?;
        let domain = ontology.find_domain(self.package_id).unwrap();
        let schemas = build_openapi_schemas(ontology.as_ref(), self.package_id, domain);
        let schemas_json = serde_json::to_string_pretty(&schemas).unwrap();
        Ok(schemas_json)
    }

    pub fn serialize_yaml_types(&self) -> WasmResult<String> {
        let ontology = self.weak_ontology.upgrade()?;
        let domain = ontology.find_domain(self.package_id).unwrap();
        let schemas = build_openapi_schemas(ontology.as_ref(), self.package_id, domain);
        let schemas_yaml = serde_yaml::to_string(&schemas).unwrap();
        Ok(schemas_yaml)
    }

    /// Note: deprecated. Create domain engine first.
    pub async fn create_graphql_schema(&self) -> Result<WasmGraphqlSchema, WasmError> {
        WasmGraphqlSchema::from_ontology(self.weak_ontology.clone(), self.package_id).await
    }
}

#[wasm_bindgen]
pub struct WasmTypeInfo {
    pub(crate) inner: TypeInfo,
    pub(crate) weak_ontology: WasmWeak<Ontology>,
}

#[wasm_bindgen]
impl WasmTypeInfo {
    pub fn name(&self) -> String {
        match &self.inner.name {
            Some(name) => name.as_str().into(),
            None => "".to_string(),
        }
    }

    pub fn domain(&self) -> WasmResult<Option<WasmDomain>> {
        let package_id = self.inner.def_id.package_id();
        let ontology = self.weak_ontology.upgrade()?;
        Ok(ontology.find_domain(package_id).map(|_| WasmDomain {
            package_id,
            weak_ontology: self.weak_ontology.clone(),
        }))
    }

    pub fn new_value(&self, js_value: JsValue, format: JsonFormat) -> Result<WasmValue, WasmError> {
        let ontology = self.weak_ontology.upgrade()?;
        let value = ontology
            .new_serde_processor(operator_addr(&self.inner)?, format.to_processor_mode())
            .deserialize(serde_wasm_bindgen::Deserializer::from(js_value))
            .map(|attr| attr.value)
            .map_err(|err| WasmError::Generic(format!("Deserialization failed: {err}")))?;

        Ok(WasmValue {
            value,
            weak_ontology: self.weak_ontology.clone(),
        })
    }

    pub fn fake_value(&self, format: JsonFormat) -> WasmResult<WasmValue> {
        let ontology = self.weak_ontology.upgrade()?;
        let fake_value =
            new_constant_fake(&ontology, self.inner.def_id, format.to_processor_mode()).unwrap();

        Ok(WasmValue {
            value: fake_value,
            weak_ontology: self.weak_ontology.clone(),
        })
    }
}

#[wasm_bindgen]
pub struct WasmValue {
    value: Value,
    weak_ontology: WasmWeak<Ontology>,
}

#[wasm_bindgen]
impl WasmValue {
    pub fn as_js(&self, format: JsonFormat) -> Result<JsValue, WasmError> {
        let ontology = self.weak_ontology.upgrade()?;
        let type_info = ontology.get_type_info(self.value.type_def_id);

        ontology
            .new_serde_processor(operator_addr(type_info)?, format.to_processor_mode())
            .serialize_value(&self.value, None, &js_serializer())
            .map_err(|err| WasmError::Generic(format!("Serialization failed: {err}")))
    }
}

#[wasm_bindgen]
pub struct WasmMapper {
    pub(crate) key: [MapKey; 2],
    pub(crate) map_info: MapMeta,
    pub(crate) weak_ontology: WasmWeak<Ontology>,
}

#[wasm_bindgen]
impl WasmMapper {
    pub fn from(&self) -> WasmResult<WasmTypeInfo> {
        let ontology = self.weak_ontology.upgrade()?;
        let type_info = ontology.get_type_info(self.key[0].def_id);
        Ok(WasmTypeInfo {
            inner: type_info.clone(),
            weak_ontology: self.weak_ontology.clone(),
        })
    }

    pub fn to(&self) -> WasmResult<WasmTypeInfo> {
        let ontology = self.weak_ontology.upgrade()?;
        let type_info = ontology.get_type_info(self.key[1].def_id);
        Ok(WasmTypeInfo {
            inner: type_info.clone(),
            weak_ontology: self.weak_ontology.clone(),
        })
    }

    pub fn map(&self, input: &WasmValue) -> WasmResult<WasmValue> {
        let ontology = self.weak_ontology.upgrade()?;
        let proc = ontology.get_mapper_proc(self.key).unwrap();
        let vm_state = ontology.new_vm(proc).run([input.value.clone()]);

        match vm_state {
            VmState::Complete(value) => Ok(WasmValue {
                value,
                weak_ontology: self.weak_ontology.clone(),
            }),
            VmState::Yielded(_) => Err(WasmError::Generic("ONTOL-VM yielded".into())),
        }
    }
}

pub(crate) fn operator_addr(type_info: &TypeInfo) -> Result<SerdeOperatorAddr, WasmError> {
    if let Some(addr) = type_info.operator_addr {
        Ok(addr)
    } else {
        Err(WasmError::Generic(format!(
            "The type `{}` cannot be serialized",
            type_info.name.as_deref().unwrap_or("")
        )))
    }
}

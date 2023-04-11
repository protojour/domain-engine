use domain_engine_juniper::{juniper::GraphQLError, SchemaBuildError};
use ontol_compiler::error::UnifiedCompileError;
use wasm_bindgen::JsValue;

#[derive(Debug)]
pub struct WasmError {
    pub msg: String,
}

impl Into<JsValue> for WasmError {
    fn into(self) -> JsValue {
        JsValue::from_str(&self.msg)
    }
}

impl From<UnifiedCompileError> for WasmError {
    fn from(value: UnifiedCompileError) -> Self {
        Self {
            msg: format!("{value:?}"),
        }
    }
}

impl From<SchemaBuildError> for WasmError {
    fn from(value: SchemaBuildError) -> Self {
        Self {
            msg: format!("{value:?}"),
        }
    }
}

impl From<GraphQLError> for WasmError {
    fn from(value: GraphQLError) -> Self {
        Self {
            msg: format!("{value}"),
        }
    }
}

impl From<serde_wasm_bindgen::Error> for WasmError {
    fn from(value: serde_wasm_bindgen::Error) -> Self {
        Self {
            msg: format!("{value}"),
        }
    }
}

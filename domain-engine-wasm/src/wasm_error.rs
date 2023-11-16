use domain_engine_juniper::{juniper::GraphQLError, CreateSchemaError};
use serde::Serialize;
use wasm_bindgen::JsValue;

#[derive(Debug)]
pub enum WasmError {
    Compile(JsCompileErrors),
    Generic(String),
}

pub type WasmResult<T> = Result<T, WasmError>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsCompileError {
    pub debug: String,
    pub message: String,
    pub filename: String,
    pub start_column: u32,
    pub start_line: u32,
    pub end_column: u32,
    pub end_line: u32,
}

#[derive(Debug, Serialize)]
pub struct JsCompileErrors {
    pub(crate) errors: Vec<JsCompileError>,
}

impl From<WasmError> for JsValue {
    fn from(value: WasmError) -> Self {
        match value {
            WasmError::Compile(err) => serde_wasm_bindgen::to_value(&err).unwrap(),
            WasmError::Generic(msg) => JsValue::from_str(&msg),
        }
    }
}

impl From<CreateSchemaError> for WasmError {
    fn from(value: CreateSchemaError) -> Self {
        Self::Generic(format!("{value:?}"))
    }
}

impl From<GraphQLError> for WasmError {
    fn from(value: GraphQLError) -> Self {
        Self::Generic(format!("{value}"))
    }
}

impl From<serde_wasm_bindgen::Error> for WasmError {
    fn from(value: serde_wasm_bindgen::Error) -> Self {
        Self::Generic(format!("{value}"))
    }
}

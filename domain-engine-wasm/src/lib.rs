#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::{collections::HashMap, sync::Arc};

use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources, SpannedCompileError,
};
use ontol_runtime::{
    config::{DataStoreConfig, PackageConfig},
    ontology::Ontology,
    PackageId,
};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_domain::{WasmDomain, WasmMapper};
use wasm_error::{JsCompileError, JsCompileErrors, WasmError};
use wasm_graphql::WasmGraphqlSchema;
use wasm_util::js_serializer;

pub mod wasm_domain;
pub mod wasm_error;
pub mod wasm_graphql;
mod wasm_util;

#[wasm_bindgen]
pub struct WasmOntology {
    ontology: Arc<Ontology>,
    package_id: PackageId,
    ontology_graph_js: JsValue,
}

#[wasm_bindgen]
impl WasmOntology {
    pub async fn create_graphql_schema(&self) -> Result<WasmGraphqlSchema, WasmError> {
        WasmGraphqlSchema::create(self.ontology.clone(), self.package_id).await
    }

    pub fn domains(&self) -> Vec<JsValue> {
        self.ontology
            .domains()
            .map(|(package_id, _domain)| WasmDomain {
                package_id: *package_id,
                ontology: self.ontology.clone(),
            })
            .map(JsValue::from)
            .collect()
    }

    pub fn mappers(&self) -> Vec<JsValue> {
        self.ontology
            .iter_map_meta()
            .map(|(key, map_info)| WasmMapper {
                key,
                map_info: map_info.clone(),
                ontology: self.ontology.clone(),
            })
            .map(JsValue::from)
            .collect()
    }

    /// Returns a `Uint8Array` containing the bincode serialization of the ontology.
    pub fn serialize_to_bincode(&self) -> Result<JsValue, WasmError> {
        // allocate temporary first, since Uint8Array is not dynamically resizable
        let mut bincode: Vec<u8> = Vec::new();
        self.ontology
            .try_serialize_to_bincode(&mut bincode)
            .map_err(|error| WasmError::Generic(format!("Failed to serialize: {error}")))?;

        let bincode_len: u32 = bincode
            .len()
            .try_into()
            .map_err(|_| WasmError::Generic("Bincode buffer too large".to_string()))?;

        let js_bincode = js_sys::Uint8Array::new_with_length(bincode_len);
        // copy from WASM to JS
        js_bincode.copy_from(bincode.as_slice());

        Ok(js_bincode.into())
    }

    pub fn ontology_graph_js(&self) -> JsValue {
        self.ontology_graph_js.clone()
    }
}

#[wasm_bindgen]
pub fn compile_ontol_domain(filename: String, source: String) -> Result<WasmOntology, WasmError> {
    console_error_panic_hook::set_once();

    let sources_by_name: HashMap<String, String> = [(filename.clone(), source)].into();
    let mut sources = Sources::default();
    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder = PackageGraphBuilder::new(filename.clone().into());
    let mut root_package = None;

    let topology = loop {
        match package_graph_builder
            .transition()
            .map_err(|err| convert_compile_error_to_wasm(err, &sources, &source_code_registry))?
        {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    let source_name = match &request.reference {
                        PackageReference::Named(source_name) => source_name.as_str(),
                    };

                    if source_name == filename {
                        root_package = Some(request.package_id);
                    }

                    if let Some(source_text) = sources_by_name.get(source_name) {
                        package_graph_builder.provide_package(ParsedPackage::parse(
                            request,
                            source_text,
                            PackageConfig {
                                data_store: Some(DataStoreConfig::Default),
                            },
                            &mut sources,
                            &mut source_code_registry,
                        ));
                    } else {
                        return Err(WasmError::Generic(format!(
                            "Could not load `{source_name}`"
                        )));
                    }
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, sources.clone()).with_ontol();
    compiler
        .compile_package_topology(topology)
        .map_err(|err| convert_compile_error_to_wasm(err, &sources, &source_code_registry))?;

    let ontology_graph_js = compiler.ontology_graph().serialize(&js_serializer())?;

    let ontology = Arc::new(compiler.into_ontology());

    match root_package {
        Some(package_id) => Ok(WasmOntology {
            ontology,
            package_id,
            ontology_graph_js,
        }),
        None => Err(WasmError::Generic("Did not find package".to_string())),
    }
}

#[wasm_bindgen]
pub struct WasmSources {
    sources: HashMap<String, String>,
    root: String,
}

impl Default for WasmSources {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl WasmSources {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            root: String::new(),
        }
    }

    pub fn insert(&mut self, filename: String, source: String) {
        self.sources.insert(filename, source);
    }

    pub fn set_root(&mut self, filename: String) {
        self.root = filename;
    }

    pub fn keys(&self) -> Vec<JsValue> {
        self.sources.keys().map(JsValue::from).collect()
    }

    pub fn values(&self) -> Vec<JsValue> {
        self.sources.values().map(JsValue::from).collect()
    }

    pub fn root(&self) -> JsValue {
        self.root.clone().into()
    }

    pub fn compile(&self) -> Result<WasmOntology, WasmError> {
        console_error_panic_hook::set_once();

        let sources_by_name = &self.sources;
        let mut sources = Sources::default();
        let mut source_code_registry = SourceCodeRegistry::default();
        let mut package_graph_builder = PackageGraphBuilder::new(self.root.clone().into());
        let mut root_package = None;

        let topology = loop {
            match package_graph_builder.transition().map_err(|err| {
                convert_compile_error_to_wasm(err, &sources, &source_code_registry)
            })? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = match &request.reference {
                            PackageReference::Named(source_name) => source_name.as_str(),
                        };

                        let mut is_root = false;
                        if source_name == self.root {
                            root_package = Some(request.package_id);
                            is_root = true;
                        }

                        if let Some(source_text) = sources_by_name.get(source_name) {
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                source_text,
                                PackageConfig {
                                    // apparently there should only be one data store
                                    data_store: match is_root {
                                        false => None,
                                        true => Some(DataStoreConfig::Default),
                                    },
                                },
                                &mut sources,
                                &mut source_code_registry,
                            ));
                        } else {
                            return Err(WasmError::Generic(format!(
                                "Could not load `{source_name}`"
                            )));
                        }
                    }
                }
                GraphState::Built(topology) => break topology,
            }
        };

        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, sources.clone()).with_ontol();
        compiler
            .compile_package_topology(topology)
            .map_err(|err| convert_compile_error_to_wasm(err, &sources, &source_code_registry))?;

        let ontology_graph_js = compiler.ontology_graph().serialize(&js_serializer())?;

        let ontology = Arc::new(compiler.into_ontology());

        match root_package {
            Some(package_id) => Ok(WasmOntology {
                ontology,
                package_id,
                ontology_graph_js,
            }),
            None => Err(WasmError::Generic("Did not find package".to_string())),
        }
    }
}

fn convert_compile_error_to_wasm(
    error: UnifiedCompileError,
    sources: &Sources,
    registry: &SourceCodeRegistry,
) -> WasmError {
    WasmError::Compile(JsCompileErrors {
        errors: error
            .errors
            .into_iter()
            .map(|err| convert_spanned_compile_error_to_wasm(err, sources, registry))
            .collect(),
    })
}

fn convert_spanned_compile_error_to_wasm(
    error: SpannedCompileError,
    sources: &Sources,
    registry: &SourceCodeRegistry,
) -> JsCompileError {
    let filename = sources
        .get_source(error.span.source_id)
        .unwrap()
        .name
        .to_string();
    let src = registry.registry.get(&error.span.source_id).unwrap();
    let (start_line, start_column) = locate_error(error.span.start, src);
    let (end_line, end_column) = locate_error(error.span.end, src);
    JsCompileError {
        debug: format!("{:?}", error.error),
        message: format!("{}", error.error),
        filename,
        start_line,
        start_column,
        end_line,
        end_column,
    }
}

fn locate_error(byte_pos: u32, src: &str) -> (u32, u32) {
    let byte_pos = byte_pos as usize;
    let mut cursor: usize = 0;
    for (line_index, line) in src.lines().enumerate() {
        if cursor + line.len() >= byte_pos {
            return ((line_index + 1) as u32, (byte_pos - cursor + 1) as u32);
        }
        cursor += line.len() + 1
    }
    (0, 0)
}

#[test]
fn test_locate_error() {
    assert_eq!((2, 1), locate_error(2, "a\nb"));

    assert_eq!((1, 2), locate_error(1, "a\nb"));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use wasm_bindgen_test::*;

    use crate::compile_ontol_domain;

    #[wasm_bindgen_test]
    async fn compile_domain_and_introspec_graphql() {
        let ontol = "
        pub type foo_id { fmt '' => string => . }
        pub type foo {
            rel .'prop'|id: foo_id
        }
        ";

        let wasm_ontology = compile_ontol_domain("test.on".to_string(), ontol.to_string()).unwrap();
        let graphql_schema = wasm_ontology.create_graphql_schema().await.unwrap();

        let document = "{
            __schema {
                types {
                    name
                }
            }
        }";
        let variables = serde_wasm_bindgen::to_value(&HashMap::<String, String>::new()).unwrap();

        graphql_schema
            .execute(document.to_string(), None, variables)
            .await
            .unwrap();
    }
}

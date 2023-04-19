#![allow(dead_code)]

use std::sync::Arc;

use graphql::WasmGraphqlSchema;
use ontol_compiler::{
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_runtime::{env::Env, PackageId};
use wasm_bindgen::prelude::*;
use wasm_error::WasmError;

pub mod graphql;
pub mod wasm_error;

#[wasm_bindgen]
pub struct WasmEnv {
    env: Arc<Env>,
    package_id: PackageId,
}

#[wasm_bindgen]
impl WasmEnv {
    pub fn create_graphql_schema(&self) -> Result<WasmGraphqlSchema, WasmError> {
        WasmGraphqlSchema::create(self.env.clone(), self.package_id)
    }
}

const ONTOL_ROOT: &str = "ontol_root.on";

#[wasm_bindgen]
pub fn compile_ontol_domain(ontol_source: String) -> Result<WasmEnv, WasmError> {
    let mut sources = Sources::default();
    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder = PackageGraphBuilder::new(ONTOL_ROOT.into());
    let mut package_id = None;

    let topology = loop {
        match package_graph_builder.transition()? {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    let source_name = match &request.reference {
                        PackageReference::Named(source_name) => source_name.as_str(),
                    };

                    if source_name == ONTOL_ROOT {
                        package_id = Some(request.package_id);
                        package_graph_builder.provide_package(ParsedPackage::parse(
                            request,
                            ontol_source.as_str(),
                            &mut sources,
                            &mut source_code_registry,
                        ));
                    } else {
                        return Err(WasmError {
                            msg: format!("Could not load `{source_name}`"),
                        });
                    }
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, sources).with_core();
    compiler.compile_package_topology(topology)?;
    let env = Arc::new(compiler.into_env());

    match package_id {
        Some(package_id) => Ok(WasmEnv { env, package_id }),
        None => Err(WasmError {
            msg: "Did not find package".to_string(),
        }),
    }
}

#[wasm_bindgen]
pub fn test_run_compiler() {
    let mem = Mem::default();
    let compiler = Compiler::new(&mem, Default::default());
    let _env = compiler.into_env();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use wasm_bindgen_test::*;

    use crate::compile_ontol_domain;

    #[wasm_bindgen_test]
    async fn compile_domain_and_introspec_graphql() {
        let ontol = "
        pub type foo_id { fmt '' => string => _ }
        pub type foo {
            rel foo_id identifies: _
            rel _ 'prop': int
        }
        ";

        let wasm_env = compile_ontol_domain(ontol.to_string()).unwrap();
        let graphql_schema = wasm_env.create_graphql_schema().unwrap();

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

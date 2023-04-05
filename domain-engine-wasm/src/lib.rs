#![allow(dead_code)]

use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_runtime::{env::Env, PackageId};
use wasm_bindgen::prelude::*;

#[derive(Debug)]
#[wasm_bindgen]
pub struct WasmCompileError {
    msg: String,
}

impl From<UnifiedCompileError> for WasmCompileError {
    fn from(value: UnifiedCompileError) -> Self {
        Self {
            msg: format!("{value:?}"),
        }
    }
}

#[wasm_bindgen]
pub struct WasmEnv {
    env: Env,
    package_id: PackageId,
}

const ONTOL_ROOT: &str = "ontol_root.on";

#[wasm_bindgen]
pub fn compile_ontol_domain(ontol_source: String) -> Result<WasmEnv, WasmCompileError> {
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
                        return Err(WasmCompileError {
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
    let env = compiler.into_env();

    match package_id {
        Some(package_id) => Ok(WasmEnv { env, package_id }),
        None => Err(WasmCompileError {
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
    use wasm_bindgen_test::*;

    use crate::compile_ontol_domain;

    #[wasm_bindgen_test]
    fn compile_test_domain() {
        let source = "
        pub type foo {
            rel _ 'data': string
        }
        ";

        compile_ontol_domain(source.to_string()).unwrap();
    }
}

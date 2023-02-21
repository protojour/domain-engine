#![allow(unused)]

use ontol_compiler::{
    compiler::Compiler,
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, PackageTopology, ParsedPackage},
    CompileSrc, SpannedCompileError,
};
use ontol_runtime::{env::Env, PackageId};

mod test_compile_errors;
mod test_deserialize;
mod test_entity;
mod test_eq_basic;
mod test_geojson;
mod test_serde;
mod test_string_patterns;
mod util;

/// BUG: The generated package id is really dynamic
const TEST_PKG: PackageId = PackageId(1);

const SRC_NAME: &'static str = "str";

macro_rules! assert_error_msg {
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(v) => panic!("Expected error, was Ok({v:?})"),
            Err(e) => pretty_assertions::assert_eq!($msg, format!("{e}").as_str()),
        }
    };
}

pub(crate) use assert_error_msg;

/// Assert that JSON that gets deserialized and then serialized again matches the expectation.
///
/// When passing one JSON parameter, the assertion means that input and output must match exactly.
/// With passing two JSON parameters, the left one is the input and the right one is the expected output.
macro_rules! assert_json_io_matches {
    ($binding:expr, $json:expr) => {
        assert_json_io_matches!($binding, $json, $json);
    };
    ($binding:expr, $input:expr, $expected_output:expr) => {
        let input = $input;
        let value = match $binding.deserialize_value(input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = crate::util::serialize_json($binding.env(), &value);

        pretty_assertions::assert_eq!($expected_output, output);
    };
}

pub(crate) use assert_json_io_matches;
use util::AnnotatedCompileError;

trait TestCompile: Sized {
    /// Compile
    fn compile_ok(self, validator: impl Fn(&Env));

    /// Compile, expect failure
    fn compile_fail(self) {
        self.compile_fail_then(|_| {})
    }

    /// Compile, expect failure with error closure
    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>));
}

impl TestCompile for &'static str {
    fn compile_ok(self, validator: impl Fn(&Env)) {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let package_topology = test_package_topology(&mut compiler, self);

        match ontol_compiler::compile_package_topology(&mut compiler, package_topology) {
            Ok(()) => {
                validator(&compiler.into_env());
            }
            Err(errors) => {
                let compile_src = compiler
                    .sources
                    .find_compiled_source_by_name(SRC_NAME)
                    .unwrap();

                util::diff_errors(self, compile_src.clone(), errors, "// ERROR");
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        let mut mem = Mem::default();
        let mut compiler = Compiler::new(&mut mem).with_core();
        let package_topology = test_package_topology(&mut compiler, self);

        match ontol_compiler::compile_package_topology(&mut compiler, package_topology) {
            Ok(()) => {
                panic!("Script did not fail to compile");
            }
            Err(errors) => {
                let compile_src = compiler
                    .sources
                    .find_compiled_source_by_name(SRC_NAME)
                    .unwrap();

                let annotated_errors =
                    util::diff_errors(self, compile_src.clone(), errors, "// ERROR");
                validator(annotated_errors);
            }
        }
    }
}

fn test_package_topology(compiler: &mut Compiler, source_text: &'static str) -> PackageTopology {
    let mut graph_builder = PackageGraphBuilder::default();

    loop {
        match graph_builder.transition() {
            GraphState::RequestPackages { builder, requests } => {
                graph_builder = builder;

                for request in requests {
                    match request.reference {
                        PackageReference::Root => {
                            let compile_src = compiler.sources.add(
                                request.package_id,
                                SRC_NAME.into(),
                                source_text.into(),
                            );

                            let parsed_package = ParsedPackage::parse(&compile_src);
                            graph_builder.provide_package(parsed_package);
                        }
                    }
                }
            }
            GraphState::Built(topology) => {
                return topology;
            }
        }
    }
}

#[test]
#[should_panic(expected = "it works")]
fn ok_validator_must_run() {
    "".compile_ok(|_| {
        panic!("it works");
    })
}

#[test]
#[should_panic(expected = "it works")]
fn failure_validator_must_run() {
    "; // ERROR lex error: illegal character `;`".compile_fail_then(|_| {
        panic!("it works");
    })
}

fn main() {}

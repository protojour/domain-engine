#![allow(unused)]

use std::collections::HashMap;

use ontol_compiler::{
    compiler::Compiler,
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, PackageTopology, ParsedPackage},
    SourceCodeRegistry, Sources, SpannedCompileError,
};
use ontol_runtime::{env::Env, PackageId};

mod test_compile_errors;
mod test_deserialize;
mod test_entity;
mod test_eq_basic;
mod test_geojson;
mod test_multi_package;
mod test_serde;
mod test_string_patterns;
mod util;

/// BUG: The generated package id is really dynamic
const TEST_PKG: PackageId = PackageId(1);

const ROOT_SRC_NAME: &'static str = "test_root.ont";

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
        tracing::debug!("deserialized value: {value:#?}");
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
        TestPackages::with_root(self).compile_ok(validator)
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_root(self).compile_fail_then(validator)
    }
}

#[derive(Eq, PartialEq, Hash)]
struct SourceName(pub &'static str);

impl SourceName {
    pub fn root() -> Self {
        Self(ROOT_SRC_NAME)
    }
}

struct TestPackages {
    sources_by_name: HashMap<&'static str, &'static str>,
    sources: Sources,
    source_code_registry: SourceCodeRegistry,
}

impl TestPackages {
    fn with_root(text: &'static str) -> Self {
        Self::with_sources([(SourceName::root(), text)])
    }

    pub fn with_sources(
        sources_by_name: impl IntoIterator<Item = (SourceName, &'static str)>,
    ) -> Self {
        Self {
            sources_by_name: sources_by_name
                .into_iter()
                .map(|(name, text)| (name.0, text))
                .collect(),
            sources: Default::default(),
            source_code_registry: Default::default(),
        }
    }

    fn load_topology(&mut self) -> Result<PackageTopology, UnifiedCompileError> {
        let mut package_graph_builder = PackageGraphBuilder::new(ROOT_SRC_NAME.into());

        loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = match &request.reference {
                            PackageReference::Named(source_name) => source_name.as_str(),
                        };

                        if let Some(source_text) = self.sources_by_name.get(source_name) {
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                source_text,
                                &mut self.sources,
                                &mut self.source_code_registry,
                            ));
                        }
                    }
                }
                GraphState::Built(topology) => return Ok(topology),
            }
        }
    }

    fn compile_topology(&mut self) -> Result<Env, UnifiedCompileError> {
        let package_topology = self.load_topology()?;
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, self.sources.clone()).with_core();

        match compiler.compile_package_topology(package_topology) {
            Ok(()) => Ok(compiler.into_env()),
            Err(error) => Err(error),
        }
    }
}

impl TestCompile for TestPackages {
    fn compile_ok(mut self, validator: impl Fn(&Env)) {
        match self.compile_topology() {
            Ok(env) => {
                validator(&env);
            }
            Err(error) => {
                // Show the error diff, a diff makes the test fail.
                // This makes it possible to debug the test to make it compile.
                util::diff_errors(error, &self.sources, &self.source_code_registry);

                // If there is no diff, then compile_ok() is likely the wrong thing to use
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }

    fn compile_fail_then(mut self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        match self.compile_topology() {
            Ok(env) => {
                panic!("Scripts did not fail to compile");
            }
            Err(error) => {
                let annotated_errors =
                    util::diff_errors(error, &self.sources, &self.source_code_registry);
                validator(annotated_errors);
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

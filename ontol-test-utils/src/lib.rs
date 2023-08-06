use std::{collections::HashMap, future::Future, sync::Arc};

use diagnostics::AnnotatedCompileError;
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, PackageTopology, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_runtime::{
    config::{DataStoreConfig, PackageConfig},
    ontology::Ontology,
    PackageId,
};
use type_binding::TypeBinding;

pub mod diagnostics;
pub mod serde_utils;
pub mod test_map;
pub mod type_binding;

pub const ROOT_SRC_NAME: &str = "test_root.on";

/// Workaround for `pretty_assertions::assert_eq` arguments appearing
/// in a (slightly?) unnatural order. The _expected_ expression ideally comes first,
/// in order to show the most sensible colored diff.
/// This macro makes expected and actual explicit, and supports any order by using keyword arguments.
#[macro_export]
macro_rules! expect_eq {
    (expected = $expected:expr, actual = $actual:expr $(,)?) => {
        pretty_assertions::assert_eq!($expected, $actual);
    };
    (actual = $actual:expr, expected = $expected:expr $(,)?) => {
        pretty_assertions::assert_eq!($expected, $actual);
    };
}

#[macro_export]
macro_rules! assert_error_msg {
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(v) => panic!("Expected error, was Ok({v:?})"),
            Err(e) => pretty_assertions::assert_eq!($msg, format!("{e}").trim()),
        }
    };
}

/// Assert that JSON that gets deserialized and then serialized again matches the expectation.
///
/// When passing one JSON parameter, the assertion means that input and output must match exactly.
/// With passing two JSON parameters, the left one is the input and the right one is the expected output.
#[macro_export]
macro_rules! assert_json_io_matches {
    ($binding:expr, $mode:ident, $json:tt) => {
        assert_json_io_matches!($binding, $mode, $json == $json);
    };
    ($binding:expr, Create, $input:tt == $expected_output:tt) => {
        let input = serde_json::json!($input);
        let value = match ontol_test_utils::serde_utils::create_de(&$binding).value(input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        tracing::debug!("deserialized value: {value:#?}");
        let output = ontol_test_utils::serde_utils::create_ser(&$binding).json(&value);

        pretty_assertions::assert_eq!(serde_json::json!($expected_output), output);
    };
}

#[derive(Clone)]
pub struct OntolTest {
    pub ontology: Arc<Ontology>,
    pub root_package: PackageId,
    pub compile_json_schema: bool,
    pub packages_by_source_name: HashMap<String, PackageId>,
}

impl OntolTest {
    pub fn get_package_id(&self, source_name: &str) -> PackageId {
        self.packages_by_source_name
            .get(source_name)
            .cloned()
            .unwrap_or_else(|| panic!("PackageId for `{}` not found", source_name))
    }

    /// Make new type bindings with the given type names.
    /// The type name may be written as "SourceName::Type" to specify a specific domain.
    /// A type without prefix is interpreted as the root domain/package.
    pub fn bind<const N: usize>(&self, type_names: [&str; N]) -> [TypeBinding; N] {
        type_names.map(|type_name| TypeBinding::new(self, type_name))
    }
}

#[async_trait::async_trait]
pub trait TestCompile: Sized {
    /// Compile
    fn compile_ok(self, validator: impl Fn(OntolTest)) -> OntolTest;

    /// Compile (async validator)
    async fn compile_ok_async<F: Future<Output = ()> + Send>(
        self,
        validator: impl Fn(OntolTest) -> F + Send,
    ) -> OntolTest;

    /// Compile, expect failure
    fn compile_fail(self) {
        self.compile_fail_then(|_| {})
    }

    /// Compile, expect failure with error closure
    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>));
}

#[async_trait::async_trait]
impl TestCompile for &'static str {
    fn compile_ok(self, validator: impl Fn(OntolTest)) -> OntolTest {
        TestPackages::with_root(self).compile_ok(validator)
    }

    async fn compile_ok_async<F: Future<Output = ()> + Send>(
        self,
        validator: impl Fn(OntolTest) -> F + Send,
    ) -> OntolTest {
        TestPackages::with_root(self)
            .compile_ok_async(validator)
            .await
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_root(self).compile_fail_then(validator)
    }
}

#[derive(Eq, PartialEq, Hash)]
pub struct SourceName(pub &'static str);

impl SourceName {
    pub const fn root() -> Self {
        Self(ROOT_SRC_NAME)
    }
}

pub struct TestPackages {
    sources_by_name: HashMap<&'static str, &'static str>,
    sources: Sources,
    source_code_registry: SourceCodeRegistry,
    data_store: Option<(SourceName, DataStoreConfig)>,
    packages_by_source_name: HashMap<String, PackageId>,
}

impl TestPackages {
    pub fn with_root(text: &'static str) -> Self {
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
            data_store: None,
            packages_by_source_name: Default::default(),
        }
    }

    pub fn with_data_store(mut self, name: SourceName, config: DataStoreConfig) -> Self {
        self.data_store = Some((name, config));
        self
    }

    fn load_topology(&mut self) -> Result<(PackageTopology, PackageId), UnifiedCompileError> {
        let mut package_graph_builder = PackageGraphBuilder::new(ROOT_SRC_NAME.into());
        let mut root_package = None;

        loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = match &request.reference {
                            PackageReference::Named(source_name) => source_name.as_str(),
                        };

                        self.packages_by_source_name
                            .insert(source_name.to_string(), request.package_id);

                        if source_name == ROOT_SRC_NAME {
                            root_package = Some(request.package_id);
                        }

                        let mut package_config = PackageConfig::default();

                        if let Some((db_source_name, data_store_config)) = &self.data_store {
                            if source_name == db_source_name.0 {
                                package_config.data_store = Some(data_store_config.clone());
                            }
                        }

                        if let Some(source_text) = self.sources_by_name.get(source_name) {
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                source_text,
                                package_config,
                                &mut self.sources,
                                &mut self.source_code_registry,
                            ));
                        }
                    }
                }
                GraphState::Built(topology) => return Ok((topology, root_package.unwrap())),
            }
        }
    }

    fn compile_topology(&mut self) -> Result<OntolTest, UnifiedCompileError> {
        let (package_topology, root_package) = self.load_topology()?;
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, self.sources.clone()).with_ontol();

        match compiler.compile_package_topology(package_topology) {
            Ok(()) => {
                let ontology: Ontology = {
                    let binary_ontology: Vec<u8> =
                        bincode::serialize(&compiler.into_ontology()).unwrap();
                    bincode::deserialize(&binary_ontology).unwrap()
                };

                Ok(OntolTest {
                    ontology: Arc::new(ontology),
                    root_package,
                    // NOTE: waiting on https://github.com/Stranger6667/jsonschema-rs/issues/420
                    compile_json_schema: false,
                    packages_by_source_name: self.packages_by_source_name.clone(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn compile_topology_ok(&mut self) -> OntolTest {
        match self.compile_topology() {
            Ok(ontol_test) => ontol_test,
            Err(error) => {
                // Show the error diff, a diff makes the test fail.
                // This makes it possible to debug the test to make it compile.
                diagnostics::diff_errors(error, &self.sources, &self.source_code_registry);

                // If there is no diff, then compile_ok() is likely the wrong thing to use
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }
}

#[async_trait::async_trait]
impl TestCompile for TestPackages {
    fn compile_ok(mut self, validator: impl Fn(OntolTest)) -> OntolTest {
        let ontol_test = self.compile_topology_ok();
        validator(ontol_test.clone());
        ontol_test
    }

    async fn compile_ok_async<F: Future<Output = ()> + Send>(
        mut self,
        validator: impl Fn(OntolTest) -> F + Send,
    ) -> OntolTest {
        let ontol_test = self.compile_topology_ok();
        let fut = validator(ontol_test.clone());
        fut.await;
        ontol_test
    }

    fn compile_fail_then(mut self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        match self.compile_topology() {
            Ok(_) => {
                panic!("Scripts did not fail to compile");
            }
            Err(error) => {
                let annotated_errors =
                    diagnostics::diff_errors(error, &self.sources, &self.source_code_registry);
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
    });
}

#[test]
#[should_panic(expected = "it works")]
fn failure_validator_must_run() {
    "foo // ERROR parse error: found `foo`, expected one of `use`, `type`, `with`, `rel`, `fmt`, `map`, `pub`"
        .compile_fail_then(|_| {
            panic!("it works");
        })
}

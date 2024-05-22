#![forbid(unsafe_code)]

use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    rc::Rc,
    sync::Arc,
};

use diagnostics::AnnotatedCompileError;
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    ontol_syntax::OntolTreeSyntax,
    package::{GraphState, PackageGraphBuilder, PackageReference, PackageTopology, ParsedPackage},
    SourceCodeRegistry, Sources,
};
use ontol_parser::cst_parse;
use ontol_runtime::{
    interface::{graphql::schema::GraphqlSchema, DomainInterface},
    ontology::{
        config::{DataStoreConfig, PackageConfig},
        Ontology,
    },
    property::PropertyId,
    PackageId,
};
use tracing::info;
use type_binding::TypeBinding;

pub mod diagnostics;
pub mod examples;
pub mod json_utils;
pub mod serde_helper;
pub mod test_extensions;
pub mod test_map;
pub mod type_binding;

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
    ($serde_helper:expr, $json:tt) => {
        assert_json_io_matches!($serde_helper, $json == $json);
    };
    ($serde_helper:expr, $input:tt == $expected_output:tt) => {
        let input = serde_json::json!($input);
        let value = match $serde_helper.to_value_nocheck(input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        tracing::debug!("deserialized value: {value:#?}");
        let output = $serde_helper.as_json(&value);

        pretty_assertions::assert_eq!(serde_json::json!($expected_output), output);
    };
}

#[derive(Clone)]
pub struct OntolTest {
    ontology: Arc<Ontology>,
    root_package: PackageId,
    compile_json_schema: bool,
    packages_by_source_name: HashMap<String, PackageId>,
}

impl OntolTest {
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn ontology_owned(&self) -> Arc<Ontology> {
        self.ontology.clone()
    }

    pub fn root_package(&self) -> PackageId {
        self.root_package
    }

    pub fn set_compile_json_schema(&mut self, enabled: bool) {
        self.compile_json_schema = enabled;
    }

    pub fn parse_test_ident<'s>(&self, ident: &'s str) -> (PackageId, &'s str) {
        if ident.contains('.') {
            let vector: Vec<&str> = ident.split('.').collect();
            let source_name = vector.first().unwrap();
            let local_ident = vector.get(1).unwrap();

            (self.get_package_id(source_name), local_ident)
        } else {
            (self.root_package, ident)
        }
    }

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

    pub fn prop_ids<const N: usize>(&self, props: [(&TypeBinding, &str); N]) -> [PropertyId; N] {
        props.map(|(binding, prop_name)| {
            let prop_id = binding.find_property(prop_name).unwrap();

            // Having this debug line will help debugging tests,
            // as the property name won't be used in lower-level ONTOL representations.
            info!("property {prop_id:?} => `{prop_name}`");
            prop_id
        })
    }

    /// Get the ontol_runtime GraphQL schema
    pub fn graphql_schema(&self, source_name: impl Into<SrcName>) -> &GraphqlSchema {
        self.ontology
            .domain_interfaces(self.get_package_id(&source_name.into().0))
            .iter()
            .map(|interface| match interface {
                DomainInterface::GraphQL(schema) => schema,
            })
            .next()
            .expect("GraphQL schema not found in interfaces")
    }
}

pub trait TestCompile: Sized {
    /// Compile, expect no errors, return the OntolTest as the ontology interface
    #[track_caller]
    fn compile(self) -> OntolTest;

    /// Compile, then run closure on the resulting test.
    /// This style is suitable when the ONTOL source is contained inline in the test.
    #[track_caller]
    fn compile_then(self, validator: impl Fn(OntolTest)) -> OntolTest {
        let test = self.compile();
        validator(test.clone());
        test
    }

    /// Compile, expect failure
    #[track_caller]
    fn compile_fail(self) -> Vec<AnnotatedCompileError>;

    /// Compile, expect failure with error closure
    #[track_caller]
    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>));
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SrcName(pub Cow<'static, str>);

pub const fn src_name(name: &'static str) -> SrcName {
    SrcName(Cow::Borrowed(name))
}

impl From<&'static str> for SrcName {
    fn from(value: &'static str) -> Self {
        SrcName(Cow::Borrowed(value))
    }
}

impl SrcName {
    pub fn as_str(&self) -> &str {
        self.0.borrow()
    }
}

impl Default for SrcName {
    fn default() -> Self {
        SrcName(Cow::Borrowed("test_root.on"))
    }
}

pub struct TestPackages {
    sources_by_name: HashMap<Cow<'static, str>, Cow<'static, str>>,
    root_package_names: Vec<SrcName>,
    sources: Sources,
    source_code_registry: SourceCodeRegistry,
    data_store: Option<(SrcName, DataStoreConfig)>,
    packages_by_source_name: HashMap<String, PackageId>,
    disable_ontology_serde: bool,
}

impl TestPackages {
    /// Parse a string/file with `//@` directives
    pub fn parse_multi_ontol(default_name: SrcName, contents: &str) -> Self {
        let mut sources_by_name: Vec<(SrcName, Cow<'static, str>)> = vec![];
        let mut cur_name = default_name;
        let mut cur_source = String::new();

        for (line_no, line) in contents.lines().enumerate() {
            if line.starts_with("//@") {
                if line_no > 1 {
                    sources_by_name.push((
                        std::mem::take(&mut cur_name),
                        Cow::Owned(std::mem::take(&mut cur_source)),
                    ));
                }

                let source_name = line.strip_prefix("//@ src_name=").unwrap();
                cur_name = SrcName(Cow::Owned(source_name.into()));
            } else {
                cur_source.push_str(line);
                cur_source.push('\n');
            }
        }

        sources_by_name.push((cur_name, Cow::Owned(cur_source)));

        Self::new(sources_by_name)
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only root source.
    pub fn with_sources(
        sources_by_name: impl IntoIterator<Item = (SrcName, Cow<'static, str>)>,
    ) -> Self {
        Self::new(sources_by_name.into_iter().collect())
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only root source.
    ///
    /// This version requires the ONTOL sources to be &'static str.
    pub fn with_static_sources(
        sources_by_name: impl IntoIterator<Item = (SrcName, &'static str)>,
    ) -> Self {
        Self::new(
            sources_by_name
                .into_iter()
                .map(|(name, src)| (name, Cow::Borrowed(src)))
                .collect(),
        )
    }

    fn new(sources_by_name: Vec<(SrcName, Cow<'static, str>)>) -> Self {
        let default_root = sources_by_name.first().map(|(name, _)| name.clone());

        Self {
            sources_by_name: sources_by_name
                .into_iter()
                .map(|(name, text)| (name.0.clone(), text))
                .collect(),
            root_package_names: default_root.into_iter().collect(),
            sources: Default::default(),
            source_code_registry: Default::default(),
            data_store: None,
            packages_by_source_name: Default::default(),
            disable_ontology_serde: false,
        }
    }

    /// Override the set of root packages.
    /// By default, the first source file is the only root file, and all other files will be loaded from that file.
    ///
    /// This method can configure any number of root sources which will be used to seed the package topology resolver.
    pub fn with_roots(mut self, roots: impl IntoIterator<Item = SrcName>) -> Self {
        self.root_package_names = roots.into_iter().collect();
        self
    }

    /// Set data store config for one of the sources.
    pub fn with_data_store(mut self, name: SrcName, config: DataStoreConfig) -> Self {
        self.data_store = Some((name, config));
        self
    }

    /// In normal mode, the Ontology is serialized and then deserialized again before the test
    /// is returned. This is to increase confidence that this step indeed works.
    /// All tests do this by default.
    ///
    /// This turns it off. The only plausible reason to do this is in benchmarking code,
    /// where the cost of this serde-step might not be desirable.
    pub fn bench_disable_ontology_serde(mut self) -> Self {
        self.disable_ontology_serde = true;
        self
    }

    fn load_topology(&mut self) -> Result<(PackageTopology, PackageId), UnifiedCompileError> {
        let mut package_graph_builder = PackageGraphBuilder::with_roots(
            self.root_package_names
                .iter()
                .map(|name| name.0.clone().into()),
        );
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

                        if let Some(first_root) = self.root_package_names.first() {
                            if source_name == first_root.0 {
                                root_package = Some(request.package_id);
                            }
                        }

                        let mut package_config = PackageConfig::default();

                        if let Some((db_source_name, data_store_config)) = &self.data_store {
                            if source_name == db_source_name.0 {
                                package_config.data_store = Some(data_store_config.clone());
                            }
                        }

                        if let Some(source_text) = self.sources_by_name.remove(source_name) {
                            let rc_source = Rc::new(source_text.as_ref().to_string());
                            let (flat_tree, errors) = cst_parse(&rc_source);
                            let tree = flat_tree.unflatten();

                            let parsed = ParsedPackage::new(
                                request,
                                Box::new(OntolTreeSyntax {
                                    tree,
                                    source_text: rc_source.clone(),
                                }),
                                errors,
                                package_config,
                                &mut self.sources,
                            );
                            self.source_code_registry
                                .registry
                                .insert(parsed.src.id, rc_source);

                            package_graph_builder.provide_package(parsed);
                        }
                    }
                }
                GraphState::Built(topology) => return Ok((topology, root_package.unwrap())),
            }
        }
    }

    fn compile_topology(&mut self) -> Result<OntolTest, UnifiedCompileError> {
        let mem = Mem::default();
        let (package_topology, root_package) = self.load_topology()?;

        let mut ontology =
            ontol_compiler::compile(package_topology, self.sources.clone(), &mem)?.into_ontology();

        if !self.disable_ontology_serde {
            let mut binary_ontology: Vec<u8> = Vec::new();
            ontology
                .try_serialize_to_bincode(&mut binary_ontology)
                .unwrap();
            ontology = Ontology::try_from_bincode(binary_ontology.as_slice()).unwrap();
        }

        Ok(OntolTest {
            ontology: Arc::new(ontology),
            root_package,
            // NOTE: waiting on https://github.com/Stranger6667/jsonschema-rs/issues/420
            compile_json_schema: false,
            packages_by_source_name: self.packages_by_source_name.clone(),
        })
    }

    #[track_caller]
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

impl TestCompile for TestPackages {
    fn compile(mut self) -> OntolTest {
        self.compile_topology_ok()
    }

    fn compile_fail(mut self) -> Vec<AnnotatedCompileError> {
        match self.compile_topology() {
            Ok(_) => {
                panic!("Scripts did not fail to compile");
            }
            Err(error) => {
                diagnostics::diff_errors(error, &self.sources, &self.source_code_registry)
            }
        }
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

impl TestCompile for &'static str {
    fn compile(self) -> OntolTest {
        TestPackages::with_static_sources([(SrcName::default(), self)]).compile()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_static_sources([(SrcName::default(), self)]).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_static_sources([(SrcName::default(), self)]).compile_fail_then(validator)
    }
}

impl TestCompile for String {
    fn compile(self) -> OntolTest {
        TestPackages::with_sources([(SrcName::default(), self.into())]).compile()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_sources([(SrcName::default(), self.into())]).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_sources([(SrcName::default(), self.into())]).compile_fail_then(validator)
    }
}

#[test]
#[should_panic(expected = "it works")]
fn ok_validator_must_run() {
    "".compile_then(|_| {
        panic!("it works");
    });
}

#[test]
#[should_panic(expected = "it works")]
fn failure_validator_must_run() {
    "foo // ERROR parse error: expected keyword, found symbol".compile_fail_then(|_| {
        panic!("it works");
    })
}

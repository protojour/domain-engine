#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    env,
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use def_binding::DefBinding;
use diagnostics::AnnotatedCompileError;
use fnv::FnvHashMap;
use ontol_compiler::{Compiled, error::UnifiedCompileError, mem::Mem};
use ontol_core::{ArcString, LogRef, tag::DomainIndex, url::DomainUrl};
use ontol_log::{
    analyzer::DomainAnalyzer, compile_entrypoint::SemModelCompileEntrypoint, error::SemError,
    log_model::Log, sem_model::GlobalSemModel,
};
use ontol_parser::{
    ParserError,
    basic_syntax::{OntolTreeSyntax, extract_ontol_header_data},
    cst::grammar,
    cst_parse,
    source::{SourceCodeRegistry, SourceId},
    topology::{DepGraphBuilder, DomainTopology, GraphState, ParsedDomain, WithDocs},
};
use ontol_runtime::{
    PropId,
    interface::{DomainInterface, graphql::schema::GraphqlSchema},
    ontology::{
        Ontology,
        config::{DataStoreConfig, DomainConfig},
    },
};
use ontol_syntax::{parse_syntax, syntax_view::View};
use tracing::{error, info};
use url::Url;

pub mod def_binding;
pub mod diagnostics;
pub mod json_utils;
pub mod serde_helper;
pub mod test_extensions;
pub mod test_map;

/// Workaround for `pretty_assertions::assert_eq` arguments appearing
/// in a (slightly?) unnatural order. The _expected_ expression ideally comes first,
/// in order to show the most sensible colored diff.
/// This macro makes expected and actual explicit, and supports any order by using keyword arguments.
#[macro_export]
macro_rules! expect_eq {
    (expected = $expected:expr, actual = $actual:expr $(,)?) => {
        pretty_assertions::assert_eq!($expected, $actual);
    };
    (expected = $expected:expr, actual = $actual:expr, $msg:expr) => {
        pretty_assertions::assert_eq!($expected, $actual, $msg);
    };
    (actual = $actual:expr, expected = $expected:expr $(,)?) => {
        pretty_assertions::assert_eq!($expected, $actual);
    };
    (actual = $actual:expr, expected = $expected:expr, $msg:expr) => {
        pretty_assertions::assert_eq!($expected, $actual, $msg);
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
        let attr = match $serde_helper.to_attr_nocheck(input.clone()) {
            Ok(attr) => attr,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        tracing::debug!(
            "deserialized value: {}",
            ontol_runtime::value::ValueDebug(&attr)
        );
        let output = $serde_helper.as_json(attr.as_ref());

        pretty_assertions::assert_eq!(serde_json::json!($expected_output), output);
    };
}

#[derive(Clone)]
pub struct OntolTest {
    ontology: Arc<Ontology>,
    entrypoint: DomainIndex,
    compile_json_schema: bool,
    domains_by_url: HashMap<DomainUrl, DomainIndex>,
}

impl OntolTest {
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn ontology_owned(&self) -> Arc<Ontology> {
        self.ontology.clone()
    }

    pub fn entrypoint(&self) -> DomainIndex {
        self.entrypoint
    }

    pub fn set_compile_json_schema(&mut self, enabled: bool) {
        self.compile_json_schema = enabled;
    }

    pub fn parse_test_ident<'s>(&self, ident: &'s str) -> (DomainIndex, &'s str) {
        if ident.contains('.') {
            let vector: Vec<&str> = ident.split('.').collect();
            let source_name = vector.first().unwrap();
            let local_ident = vector.get(1).unwrap();

            (self.get_domain_index(source_name), local_ident)
        } else {
            (self.entrypoint, ident)
        }
    }

    pub fn get_domain_index(&self, domain_short_name: &str) -> DomainIndex {
        for (url, domain_index) in &self.domains_by_url {
            if url.short_name() == domain_short_name {
                return *domain_index;
            }
        }

        panic!(
            "PackageId for `{}` not found in {:?}",
            domain_short_name,
            self.domains_by_url.keys()
        );
    }

    /// Make new type bindings with the given type names.
    /// The type name may be written as "SourceName::Type" to specify a specific domain.
    /// A type without prefix is interpreted as the entrypoint domain/package.
    pub fn bind<const N: usize>(&self, type_names: [&str; N]) -> [DefBinding; N] {
        type_names.map(|type_name| DefBinding::new(self, type_name))
    }

    pub fn prop_ids<const N: usize>(&self, props: [(&DefBinding, &str); N]) -> [PropId; N] {
        props.map(|(binding, prop_name)| {
            let prop_id = binding.find_property(prop_name).unwrap();

            // Having this debug line will help debugging tests,
            // as the property name won't be used in lower-level ONTOL representations.
            info!("property {prop_id:?} => `{prop_name}`");
            prop_id
        })
    }

    /// Get the ontol_runtime GraphQL schema
    pub fn graphql_schema(&self, domain_short_name: &str) -> &GraphqlSchema {
        self.ontology
            .domain_interfaces(self.get_domain_index(domain_short_name))
            .iter()
            .filter_map(|interface| match interface {
                DomainInterface::GraphQL(schema) => Some(schema),
                _ => None,
            })
            .next()
            .expect("GraphQL schema not found in interfaces")
    }
}

#[derive(Clone, Copy)]
enum CompileMode {
    Classic,
    Log,
}

impl CompileMode {
    fn from_env() -> Self {
        match env::var("DOMAIN_ENGINE_TEST_LOG") {
            Ok(value) if value == "1" => Self::Log,
            _ => Self::Classic,
        }
    }
}

pub trait TestCompile: Sized {
    /// Compile, expect no errors, return the OntolTest as the ontology interface
    #[track_caller]
    fn compile(self) -> OntolTest;

    /// Compile, expect no errors, return the OntolTest as the ontology interface
    #[track_caller]
    fn compile_log(self) -> OntolTest;

    /// Compile, then run closure on the resulting test.
    /// This style is suitable when the ONTOL source is contained inline in the test.
    #[track_caller]
    fn compile_then(self, validator: impl Fn(OntolTest)) -> OntolTest {
        let test = self.compile();
        validator(test.clone());
        test
    }

    /// Compile, then run closure on the resulting test.
    /// This style is suitable when the ONTOL source is contained inline in the test.
    #[track_caller]
    fn compile_log_then(self, validator: impl Fn(OntolTest)) -> OntolTest {
        let test = self.compile_log();
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

pub fn file_url(name: &str) -> DomainUrl {
    DomainUrl::parse(name)
}

pub fn default_file_url() -> DomainUrl {
    DomainUrl::parse("test_root.on")
}

pub fn default_short_name() -> &'static str {
    "test_root.on"
}

pub struct TestPackages {
    compile_mode: CompileMode,
    domain_sources: HashMap<DomainUrl, Arc<String>>,
    #[expect(unused)]
    log_sources: HashMap<DomainUrl, Arc<String>>,
    entrypoint_urls: Vec<DomainUrl>,
    source_code_registry: SourceCodeRegistry,
    data_store: Option<(DomainUrl, DataStoreConfig)>,
    domains_by_url: HashMap<DomainUrl, SourceId>,
    global_sem_model: Arc<Mutex<GlobalSemModel>>,
    ontol_logs: FnvHashMap<LogRef, Log>,
    log_refs: FnvHashMap<Url, LogRef>,
    next_log_ref: LogRef,
    disable_ontology_serde: bool,
}

impl TestPackages {
    /// Parse a string/file with `//@` directives
    pub fn parse_multi_ontol(default_url: DomainUrl, contents: &str) -> Self {
        let mut sources_by_url: Vec<(DomainUrl, Arc<String>)> = vec![];
        let mut cur_url = default_url;
        let mut cur_source = String::new();

        for (line_no, line) in contents.lines().enumerate() {
            if line.starts_with("//@") {
                if line_no > 1 {
                    sources_by_url.push((cur_url.clone(), std::mem::take(&mut cur_source).into()));
                }

                let source_name = line.strip_prefix("//@ src_name=").unwrap();
                cur_url = DomainUrl::parse(source_name);
            } else {
                cur_source.push_str(line);
                cur_source.push('\n');
            }
        }

        sources_by_url.push((cur_url, cur_source.into()));

        Self::new(CompileMode::from_env(), sources_by_url)
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only entrypoint source.
    pub fn with_sources(
        domain_sources: impl IntoIterator<Item = (DomainUrl, Arc<String>)>,
    ) -> Self {
        Self::new(
            CompileMode::from_env(),
            domain_sources.into_iter().collect(),
        )
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only entrypoint source.
    pub fn with_log_sources(
        domain_sources: impl IntoIterator<Item = (DomainUrl, Arc<String>)>,
    ) -> Self {
        Self::new(CompileMode::Log, domain_sources.into_iter().collect())
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only entrypoint source.
    pub fn with_static_sources(
        domain_sources: impl IntoIterator<Item = (DomainUrl, &'static str)>,
    ) -> Self {
        Self::new(
            CompileMode::from_env(),
            domain_sources
                .into_iter()
                .map(|(name, src)| (name, Arc::new(String::from(src))))
                .collect(),
        )
    }

    /// Configure with an explicit set of named sources.
    /// By default, the first one is chosen as the only entrypoint source.
    pub fn with_static_log_sources(
        domain_sources: impl IntoIterator<Item = (DomainUrl, &'static str)>,
    ) -> Self {
        Self::new(
            CompileMode::Log,
            domain_sources
                .into_iter()
                .map(|(name, src)| (name, Arc::new(String::from(src))))
                .collect(),
        )
    }

    fn new(compile_mode: CompileMode, sources: Vec<(DomainUrl, Arc<String>)>) -> Self {
        let mut domain_sources: Vec<(DomainUrl, Arc<String>)> = vec![];
        let mut log_sources: HashMap<DomainUrl, Arc<String>> = Default::default();

        for (url, source) in sources {
            if url.is_log_url() {
                log_sources.insert(url, source);
            } else {
                domain_sources.push((url, source));
            }
        }

        let default_entrypoint = domain_sources.first().map(|(url, _)| url.clone());

        Self {
            compile_mode,
            domain_sources: domain_sources
                .into_iter()
                .map(|(url, text)| (url.clone(), text))
                .collect(),
            log_sources,
            entrypoint_urls: default_entrypoint.into_iter().collect(),
            source_code_registry: Default::default(),
            data_store: None,
            domains_by_url: Default::default(),
            global_sem_model: Arc::new(Mutex::new(GlobalSemModel::default())),
            ontol_logs: Default::default(),
            log_refs: Default::default(),
            next_log_ref: LogRef(0),
            disable_ontology_serde: false,
        }
    }

    /// Override the set of entrypoint packages.
    /// By default, the first source file is the only root file, and all other files will be loaded from that file.
    ///
    /// This method can configure any number of root sources which will be used to seed the package topology resolver.
    pub fn with_entrypoints(mut self, entrypoints: impl IntoIterator<Item = DomainUrl>) -> Self {
        self.entrypoint_urls = entrypoints.into_iter().collect();
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

    fn compile_topology(&mut self) -> Result<OntolTest, UnifiedCompileError> {
        let mem = Mem::default();
        let (compiled, entrypoint) = self.compile_topology_inner(&mem)?;

        let mut domains_by_url = HashMap::default();

        for (url, source_id) in &self.domains_by_url {
            domains_by_url.insert(
                url.clone(),
                compiled.source_id_to_domain_index(*source_id).unwrap(),
            );
        }
        let entrypoint = compiled.source_id_to_domain_index(entrypoint).unwrap();

        let mut ontology = compiled.into_ontology();

        if !self.disable_ontology_serde {
            let mut binary_ontology: Vec<u8> = Vec::new();
            ontology
                .try_serialize_to_postcard(&mut binary_ontology)
                .unwrap();
            ontology = Ontology::try_from_postcard(binary_ontology.as_slice()).unwrap();
        }

        Ok(OntolTest {
            ontology: Arc::new(ontology),
            entrypoint,
            // NOTE: waiting on https://github.com/Stranger6667/jsonschema-rs/issues/420
            compile_json_schema: false,
            domains_by_url,
        })
    }

    fn compile_topology_inner<'m>(
        &mut self,
        mem: &'m Mem,
    ) -> Result<(Compiled<'m>, SourceId), UnifiedCompileError> {
        match self.compile_mode {
            CompileMode::Classic => {
                let (package_topology, entrypoint) = self.load_topology_classic()?;
                let compiled = ontol_compiler::compile(package_topology, mem)?;
                Ok((compiled, entrypoint))
            }
            CompileMode::Log => {
                let (package_topology, entrypoint) = self.load_topology_log()?;
                let compiled = ontol_compiler::compile(package_topology, mem)?;
                Ok((compiled, entrypoint))
            }
        }
    }

    fn load_topology_classic(
        &mut self,
    ) -> Result<
        (
            DomainTopology<OntolTreeSyntax<ArcString>, ParserError>,
            SourceId,
        ),
        UnifiedCompileError,
    > {
        let mut package_graph_builder =
            DepGraphBuilder::with_entrypoints(self.entrypoint_urls.iter().cloned());
        let mut entrypoint = None;

        loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        self.domains_by_url
                            .insert(request.url.clone(), request.source_id);

                        if let Some(first_entrypoint) = self.entrypoint_urls.first() {
                            if &request.url == first_entrypoint {
                                entrypoint = Some(request.source_id);
                            }
                        }

                        let mut package_config = DomainConfig::default();

                        if let Some((db_domain_url, data_store_config)) = &self.data_store {
                            if &request.url == db_domain_url {
                                package_config.data_store = Some(data_store_config.clone());
                            }
                        }

                        if let Some(source_text) = self.domain_sources.remove(&request.url) {
                            let (flat_tree, errors) = cst_parse(&source_text);
                            let tree = flat_tree.unflatten();

                            let parsed = ParsedDomain::new(
                                request,
                                OntolTreeSyntax {
                                    tree,
                                    source_text: ArcString(source_text.clone()),
                                },
                                errors,
                            );
                            self.source_code_registry.register(
                                parsed.source_id,
                                parsed.url.clone(),
                                source_text,
                            );

                            package_graph_builder.provide_domain(parsed);
                        }
                    }
                }
                GraphState::Built(topology) => return Ok((topology, entrypoint.unwrap())),
            }
        }
    }

    fn load_topology_log(
        &mut self,
    ) -> Result<
        (
            DomainTopology<SemModelCompileEntrypoint, SemError>,
            SourceId,
        ),
        UnifiedCompileError,
    > {
        let mut dep_graph_builder =
            DepGraphBuilder::with_entrypoints(self.entrypoint_urls.iter().cloned());
        let mut entrypoint = None;

        loop {
            match dep_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    dep_graph_builder = builder;

                    for request in requests {
                        self.domains_by_url
                            .insert(request.url.clone(), request.source_id);

                        let log_ref = if let Some(log_url) = request.url.log_url() {
                            *self.log_refs.entry(log_url).or_insert_with(|| {
                                let log_ref = self.next_log_ref;
                                self.next_log_ref.0 += 1;
                                log_ref
                            })
                        } else {
                            let log_ref = self.next_log_ref;
                            self.next_log_ref.0 += 1;
                            log_ref
                        };

                        if let Some(first_entrypoint) = self.entrypoint_urls.first() {
                            if &request.url == first_entrypoint {
                                entrypoint = Some(request.source_id);
                            }
                        }

                        let mut package_config = DomainConfig::default();

                        if let Some((db_domain_url, data_store_config)) = &self.data_store {
                            if &request.url == db_domain_url {
                                package_config.data_store = Some(data_store_config.clone());
                            }
                        }

                        if let Some(source_text) = self.domain_sources.remove(&request.url) {
                            let mut sem_errors: Vec<SemError> = vec![];
                            let (syntax_node, parse_errors) =
                                parse_syntax(&source_text, grammar::ontol, None);
                            sem_errors.extend(parse_errors.into_iter().map(Into::into));

                            let global_sem = self.global_sem_model.clone();
                            let mut global_sem_lock = global_sem.lock().unwrap();
                            let mut global_sem = std::mem::take(global_sem_lock.deref_mut());
                            let subdomain = global_sem.new_subdomain(log_ref);

                            let log = self.ontol_logs.entry(log_ref).or_default();

                            // Temporarily: Synthesize log ID from domain ID
                            if subdomain.0 == 0 {
                                let mut ignore_errs: Vec<ParserError> = vec![];
                                let header_data = extract_ontol_header_data(
                                    syntax_node.clone().view(),
                                    WithDocs(false),
                                    &mut ignore_errs,
                                );

                                let log_model = global_sem.get_model_mut(log_ref).unwrap();
                                log_model.set_uid(header_data.domain_id.0.clone());
                            }

                            let mut analyzer =
                                DomainAnalyzer::new(global_sem.project(log_ref, subdomain), log);

                            let errors = match analyzer.ontol(syntax_node.view()) {
                                Err(errors) => errors,
                                Ok(()) => vec![],
                            };

                            *global_sem_lock = analyzer.finish().unproject();
                            drop(global_sem_lock);

                            let parsed = ParsedDomain::new(
                                request,
                                SemModelCompileEntrypoint {
                                    model: self.global_sem_model.clone(),
                                    local_log: log_ref,
                                    subdomain,
                                },
                                errors,
                            );

                            self.source_code_registry.register(
                                parsed.source_id,
                                parsed.url.clone(),
                                source_text,
                            );

                            dep_graph_builder.provide_domain(parsed);
                        }
                    }
                }
                GraphState::Built(topology) => return Ok((topology, entrypoint.unwrap())),
            }
        }
    }

    #[track_caller]
    fn compile_topology_ok(&mut self) -> OntolTest {
        match self.compile_topology() {
            Ok(ontol_test) => ontol_test,
            Err(error) => {
                // Show the error diff, a diff makes the test fail.
                // This makes it possible to debug the test to make it compile.
                diagnostics::diff_errors(error, &self.domains_by_url, &self.source_code_registry);

                // If there is no diff, then compile_ok() is likely the wrong thing to use
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }
}

impl TestCompile for Vec<(DomainUrl, &'static str)> {
    fn compile(self) -> OntolTest {
        TestPackages::with_static_sources(self).compile_topology_ok()
    }

    fn compile_log(self) -> OntolTest {
        TestPackages::with_static_log_sources(self).compile_topology_ok()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_static_sources(self).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_static_sources(self).compile_fail_then(validator)
    }
}

impl TestCompile for Vec<(DomainUrl, Arc<String>)> {
    fn compile(self) -> OntolTest {
        TestPackages::with_sources(self).compile_topology_ok()
    }

    fn compile_log(self) -> OntolTest {
        TestPackages::with_log_sources(self).compile_topology_ok()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_sources(self).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_sources(self).compile_fail_then(validator)
    }
}

impl TestCompile for TestPackages {
    fn compile(mut self) -> OntolTest {
        self.compile_topology_ok()
    }

    fn compile_log(mut self) -> OntolTest {
        self.compile_mode = CompileMode::Log;
        self.compile_topology_ok()
    }

    fn compile_fail(mut self) -> Vec<AnnotatedCompileError> {
        match self.compile_topology() {
            Ok(_) => {
                panic!("Scripts did not fail to compile");
            }
            Err(error) => {
                error!("errors");
                diagnostics::diff_errors(error, &self.domains_by_url, &self.source_code_registry)
            }
        }
    }

    fn compile_fail_then(mut self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        match self.compile_topology() {
            Ok(_) => {
                panic!("Scripts did not fail to compile");
            }
            Err(error) => {
                let annotated_errors = diagnostics::diff_errors(
                    error,
                    &self.domains_by_url,
                    &self.source_code_registry,
                );
                validator(annotated_errors);
            }
        }
    }
}

impl TestCompile for &'static str {
    fn compile(self) -> OntolTest {
        TestPackages::with_static_sources([(default_file_url(), self)]).compile()
    }

    fn compile_log(self) -> OntolTest {
        TestPackages::with_static_log_sources([(default_file_url(), self)]).compile()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_static_sources([(default_file_url(), self)]).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_static_sources([(default_file_url(), self)]).compile_fail_then(validator)
    }
}

impl TestCompile for Arc<String> {
    fn compile(self) -> OntolTest {
        TestPackages::with_sources([(default_file_url(), self)]).compile()
    }

    fn compile_log(self) -> OntolTest {
        TestPackages::with_log_sources([(default_file_url(), self)]).compile()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_sources([(default_file_url(), self)]).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_sources([(default_file_url(), self)]).compile_fail_then(validator)
    }
}

impl TestCompile for String {
    fn compile(self) -> OntolTest {
        TestPackages::with_sources([(default_file_url(), self.into())]).compile()
    }

    fn compile_log(self) -> OntolTest {
        TestPackages::with_log_sources([(default_file_url(), self.into())]).compile()
    }

    fn compile_fail(self) -> Vec<AnnotatedCompileError> {
        TestPackages::with_sources([(default_file_url(), self.into())]).compile_fail()
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        TestPackages::with_sources([(default_file_url(), self.into())]).compile_fail_then(validator)
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

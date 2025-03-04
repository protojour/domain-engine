use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::sync::Arc;

use ontol_parser::cst_parse;
use ontol_runtime::DefId;
use ontol_runtime::DomainIndex;
use ontol_runtime::ontology::config::DomainConfig;
use ontol_runtime::ontology::domain::TopologyGeneration;
use ontol_runtime::vec_map::VecMap;
use url::Url;

use crate::NO_SPAN;
use crate::SourceCodeRegistry;
use crate::SourceSpan;
use crate::Sources;
use crate::Src;
use crate::error::CompileError;
use crate::error::UnifiedCompileError;
use crate::ontol_syntax::ArcString;
use crate::ontol_syntax::OntolSyntax;
use crate::ontol_syntax::OntolTreeSyntax;
use crate::ontol_syntax::WithDocs;

/// The compiler's loaded domains, by reference,
/// to be able to compile `use` statements
#[derive(Default)]
pub(crate) struct LoadedDomains {
    pub by_url: HashMap<DomainUrl, DefId>,
}

pub enum GraphState {
    RequestPackages {
        builder: DepGraphBuilder,
        requests: Vec<DomainRequest>,
    },
    Built(DomainTopology),
}

/// A package request originating from a source file.
/// ONTOL itself does not know how to fetch any packages.
pub struct DomainRequest {
    pub domain_index: DomainIndex,
    pub url: DomainUrl,
    generation: TopologyGeneration,
}

pub struct DomainUrlParser {
    base_url: url::Url,
}

impl Default for DomainUrlParser {
    fn default() -> Self {
        Self {
            base_url: url::Url::parse("file://").unwrap(),
        }
    }
}

impl DomainUrlParser {
    pub fn parse(&self, uri: &str) -> Result<DomainUrl, CompileError> {
        let url = url::Url::options()
            .base_url(Some(&self.base_url))
            .parse(uri)
            .map_err(|_| CompileError::InvalidDomainReference)?;

        Ok(DomainUrl(url))
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct DomainUrl(Url);

impl DomainUrl {
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    pub fn parse(name: &str) -> Self {
        DomainUrlParser::default()
            .parse(name)
            .unwrap_or_else(|_| panic!())
    }

    pub fn short_name(&self) -> &str {
        if let Some(segments) = self.0.path_segments() {
            if let Some(last) = segments.last() {
                return last;
            }
        }

        "<none>"
    }

    pub fn url(&self) -> &Url {
        &self.0
    }

    pub fn join(&self, other: &DomainUrl) -> Self {
        match other.0.scheme() {
            "file" => {
                let mut next_url = self.0.clone();

                {
                    let mut segments = next_url.path_segments_mut().unwrap();

                    segments.pop();

                    if let Some(orig_segments) = other.0.path_segments() {
                        if let Some(yo) = orig_segments.last() {
                            segments.push(yo);
                        }
                    }
                }

                Self(next_url)
            }
            _ => other.clone(),
        }
    }
}

impl From<Url> for DomainUrl {
    fn from(value: Url) -> Self {
        Self(value)
    }
}

impl From<DomainUrl> for Url {
    fn from(value: DomainUrl) -> Self {
        value.0
    }
}

impl Display for DomainUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait::async_trait]
pub trait DomainUrlResolver: Send + Sync {
    async fn resolve_domain_url(&self, url: &DomainUrl) -> Option<Arc<String>>;
}

#[async_trait::async_trait]
impl DomainUrlResolver for Vec<Box<dyn DomainUrlResolver>> {
    async fn resolve_domain_url(&self, url: &DomainUrl) -> Option<Arc<String>> {
        for resolver in self.iter() {
            if let Some(source) = resolver.resolve_domain_url(url).await {
                return Some(source);
            }
        }

        None
    }
}

pub async fn resolve_topology_async(
    entrypoints: Vec<DomainUrl>,
    ontol_sources: &mut Sources,
    source_code_registry: &mut SourceCodeRegistry,
    url_resolver: &dyn DomainUrlResolver,
) -> Result<DomainTopology, UnifiedCompileError> {
    let mut package_graph_builder = DepGraphBuilder::with_entrypoints(entrypoints);

    let topology = loop {
        let graph_state = package_graph_builder.transition()?;

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    if let Some(source_text) = url_resolver.resolve_domain_url(&request.url).await {
                        let (flat_tree, errors) = cst_parse(&source_text);

                        let parsed = ParsedDomain::new(
                            request,
                            Box::new(OntolTreeSyntax {
                                tree: flat_tree.unflatten(),
                                source_text: ArcString(source_text.clone()),
                            }),
                            errors,
                            DomainConfig::default(),
                            ontol_sources,
                        );
                        source_code_registry
                            .registry
                            .insert(parsed.src.id, source_text);
                        package_graph_builder.provide_domain(parsed);
                    } else {
                        eprintln!("Could not load `{}`", request.url);
                    }
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    Ok(topology)
}

/// A package in its parsed form.
/// The parsed form is needed to know which dependencies to request.
pub struct ParsedDomain {
    pub domain_index: DomainIndex,
    pub url: DomainUrl,
    pub generation: TopologyGeneration,
    pub config: DomainConfig,
    pub src: Src,
    pub syntax: Box<dyn OntolSyntax>,
    pub parse_errors: Vec<ontol_parser::Error>,
}

impl ParsedDomain {
    pub fn new(
        request: DomainRequest,
        syntax: Box<dyn OntolSyntax>,
        parse_errors: Vec<ontol_parser::Error>,
        config: DomainConfig,
        sources: &mut Sources,
    ) -> Self {
        let domain_index = request.domain_index;

        // TODO: Resolve local url
        let url = request.url.clone();

        let src = sources.add_source(domain_index, url.clone());

        Self {
            domain_index: src.domain_index,
            url,
            generation: request.generation,
            config,
            src,
            syntax,
            parse_errors,
        }
    }
}

/// Topological sort of the built package graph
#[derive(Default)]
pub struct DomainTopology {
    pub parsed_domains: Vec<ParsedDomain>,
}

#[derive(Debug)]
pub enum PackageGraphError {
    PackageNotFound(DomainIndex),
}

pub struct DepGraphBuilder {
    next_domain_index: DomainIndex,
    generation: TopologyGeneration,
    parsed_packages: VecMap<DomainIndex, ParsedDomain>,
    request_graph: HashMap<DomainUrl, RequestedDomain>,
}

impl DepGraphBuilder {
    /// Create an empty builder, seeded with the given root package names,
    /// which will the builder will attempt to resolve in the next transition.
    pub fn with_entrypoints(root_references: impl IntoIterator<Item = DomainUrl>) -> Self {
        let mut next_domain_index = DomainIndex::second();
        let request_graph = root_references
            .into_iter()
            .map(|domain_url| {
                let domain_index = next_domain_index;
                if next_domain_index.increase().is_err() {
                    panic!("package numbers exceeded");
                }

                (
                    domain_url,
                    RequestedDomain {
                        domain_index,
                        use_source_span: NO_SPAN,
                        requested_at_generation: TopologyGeneration(0),
                        dependencies: Default::default(),
                        found: false,
                    },
                )
            })
            .collect();

        Self {
            next_domain_index,
            generation: TopologyGeneration(0),
            parsed_packages: Default::default(),
            request_graph,
        }
    }

    /// Provide a package
    pub fn provide_domain(&mut self, mut package: ParsedDomain) {
        let mut children: HashSet<DomainUrl> = HashSet::default();

        let header_data = package
            .syntax
            .header_data(WithDocs(false), &mut package.parse_errors);

        for (reference, span) in header_data.deps {
            let url = package.url.join(&reference);

            self.request_domain(url.clone(), package.src.span(span));
            children.insert(url);
        }

        let node = self
            .request_graph
            .get_mut(&package.url)
            .expect("package not requested");
        node.found = true;
        node.dependencies.extend(children);

        self.parsed_packages.insert(package.domain_index, package);
    }

    /// Try to transition the builder into a PackageTopology.
    /// Before it is able to do that, it may request more packages.
    pub fn transition(mut self) -> Result<GraphState, UnifiedCompileError> {
        let mut requests = vec![];
        let mut load_errors = vec![];

        for (url, requested_package) in &self.request_graph {
            if !requested_package.found {
                if requested_package.requested_at_generation.0 < self.generation.0 {
                    load_errors.push(
                        CompileError::DomainNotFound(url.clone())
                            .span(requested_package.use_source_span),
                    );
                } else {
                    requests.push(DomainRequest {
                        domain_index: requested_package.domain_index,
                        url: url.clone(),
                        generation: self.generation,
                    })
                }
            }
        }

        if !load_errors.is_empty() {
            return Err(UnifiedCompileError {
                errors: load_errors,
            });
        }

        self.generation.0 += 1;

        if requests.is_empty() {
            Ok(GraphState::Built(self.topo_sort()))
        } else {
            Ok(GraphState::RequestPackages {
                builder: self,
                requests,
            })
        }
    }

    /// Finish the package graph with a topological sort of packages
    fn topo_sort(mut self) -> DomainTopology {
        let mut visited: HashSet<DomainIndex> = Default::default();
        let mut topological_sort: Vec<ParsedDomain> = vec![];

        fn traverse_graph(
            url: &DomainUrl,
            package_graph: &HashMap<DomainUrl, RequestedDomain>,
            parsed_packages: &mut VecMap<DomainIndex, ParsedDomain>,
            visited: &mut HashSet<DomainIndex>,
            topological_sort: &mut Vec<ParsedDomain>,
        ) {
            let node = package_graph.get(url).unwrap();
            if visited.contains(&node.domain_index) {
                return;
            }
            visited.insert(node.domain_index);

            for dep in &node.dependencies {
                traverse_graph(
                    dep,
                    package_graph,
                    parsed_packages,
                    visited,
                    topological_sort,
                );
            }

            let parsed_package = parsed_packages.remove(&node.domain_index).unwrap();
            topological_sort.push(parsed_package);
        }

        for url in self.request_graph.keys() {
            traverse_graph(
                url,
                &self.request_graph,
                &mut self.parsed_packages,
                &mut visited,
                &mut topological_sort,
            );
        }

        DomainTopology {
            parsed_domains: topological_sort,
        }
    }

    fn request_domain(&mut self, url: DomainUrl, use_source_span: SourceSpan) -> DomainIndex {
        match self.request_graph.entry(url) {
            Entry::Occupied(occupied) => occupied.get().domain_index,
            Entry::Vacant(vacant) => {
                let domain_index = self.next_domain_index;
                if self.next_domain_index.increase().is_err() {
                    panic!("max domain count exceeded");
                }

                vacant.insert(RequestedDomain {
                    domain_index,
                    use_source_span,
                    requested_at_generation: self.generation,
                    dependencies: Default::default(),
                    found: false,
                });
                domain_index
            }
        }
    }
}

struct RequestedDomain {
    domain_index: DomainIndex,
    use_source_span: SourceSpan,
    requested_at_generation: TopologyGeneration,
    dependencies: HashSet<DomainUrl>,
    found: bool,
}

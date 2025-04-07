//! Domain topology resolver based on syntax interpretation

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    panic::UnwindSafe,
};

use ontol_core::{
    ArcString, DomainId, TopologyGeneration,
    span::U32Span,
    tag::DomainIndex,
    url::{DomainUrl, DomainUrlResolver},
    vec_map::VecMap,
};
use tracing::info;
use ulid::Ulid;

use crate::{
    ParserError,
    basic_syntax::OntolTreeSyntax,
    cst_parse,
    source::{NO_SPAN, SourceCodeRegistry, SourceId, SourceSpan},
};

#[derive(Debug)]
pub enum TopologyError {
    DomainNotFound(DomainUrl),
    Todo(String),
}

impl TopologyError {
    #[allow(non_snake_case)]
    pub fn TODO(msg: impl Into<String>) -> Self {
        Self::Todo(msg.into())
    }
}

#[derive(Debug)]
pub struct TopologyErrors {
    pub errors: Vec<(TopologyError, SourceSpan)>,
}

pub async fn resolve_tree_syntax_topology_async(
    entrypoints: Vec<DomainUrl>,
    source_code_registry: &mut SourceCodeRegistry,
    url_resolver: &dyn DomainUrlResolver,
) -> Result<DomainTopology<OntolTreeSyntax<ArcString>>, TopologyErrors> {
    let mut graph_builder = DepGraphBuilder::with_entrypoints(entrypoints);

    let topology = loop {
        let graph_state = graph_builder.transition()?;

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                graph_builder = builder;

                for request in requests {
                    if let Some(source_text) = url_resolver.resolve_domain_url(&request.url).await {
                        let (flat_tree, errors) = cst_parse(&source_text);

                        let parsed = ParsedDomain::new(
                            request,
                            OntolTreeSyntax {
                                tree: flat_tree.unflatten(),
                                source_text: ArcString(source_text.clone()),
                            },
                            errors,
                        );
                        source_code_registry.register(
                            parsed.source_id,
                            parsed.url.clone(),
                            source_text,
                        );
                        graph_builder.provide_domain(parsed);
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

#[derive(Clone, Copy)]
pub struct WithDocs(pub bool);

/// Metadata extracted from the header section of a domain
pub struct OntolHeaderData {
    pub domain_docs: Option<String>,
    pub domain_id: (DomainId, U32Span),
    pub name: (String, U32Span),
    pub deps: Vec<(DomainUrl, U32Span)>,
}

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait ExtractHeaderData: UnwindSafe {
    fn header_data(&self, with_docs: WithDocs, errors: &mut Vec<ParserError>) -> OntolHeaderData;
}

impl OntolHeaderData {
    pub fn autogenerated() -> Self {
        let ulid = Ulid::new();
        info!("autogenerating unstable domain id `{ulid}`");
        Self {
            domain_docs: None,
            domain_id: (
                DomainId {
                    ulid,
                    stable: false,
                },
                U32Span::default(),
            ),
            name: (format!("{ulid}"), U32Span::default()),
            deps: vec![],
        }
    }
}

/// A package request originating from a source file.
/// ONTOL itself does not know how to fetch any packages.
pub struct DomainRequest {
    pub domain_index: DomainIndex,
    pub source_id: SourceId,
    pub url: DomainUrl,
    generation: TopologyGeneration,
}

/// Topological sort of the built package graph
pub struct DomainTopology<S> {
    pub parsed_domains: Vec<ParsedDomain<S>>,
}

impl<S> Default for DomainTopology<S> {
    fn default() -> Self {
        Self {
            parsed_domains: vec![],
        }
    }
}

impl<S> DomainTopology<S> {
    pub fn source_to_url_map(&self) -> BTreeMap<SourceId, DomainUrl> {
        self.parsed_domains
            .iter()
            .map(|parsed| (parsed.source_id, parsed.url.clone()))
            .collect()
    }
}

/// A domain in its parsed form.
/// The parsed form is needed to know which dependencies to request.
pub struct ParsedDomain<S> {
    pub domain_index: DomainIndex,
    pub source_id: SourceId,
    pub url: DomainUrl,
    pub generation: TopologyGeneration,
    // pub config: DomainConfig,
    // pub src: Src,
    pub syntax: S,
    pub parse_errors: Vec<ParserError>,
}

impl<S> ParsedDomain<S> {
    pub fn new(request: DomainRequest, syntax: S, parse_errors: Vec<ParserError>) -> Self {
        let source_id = request.source_id;

        // TODO: Resolve local url
        let url = request.url.clone();

        Self {
            domain_index: request.domain_index,
            source_id,
            url,
            generation: request.generation,
            syntax,
            parse_errors,
        }
    }
}

pub enum GraphState<S> {
    RequestPackages {
        builder: DepGraphBuilder<S>,
        requests: Vec<DomainRequest>,
    },
    Built(DomainTopology<S>),
}

pub struct DepGraphBuilder<S> {
    next_source_id: SourceId,
    next_domain_index: DomainIndex,
    generation: TopologyGeneration,
    parsed_packages: VecMap<SourceId, ParsedDomain<S>>,
    request_graph: HashMap<DomainUrl, RequestedDomain>,
}

impl<S: ExtractHeaderData> DepGraphBuilder<S> {
    /// Create an empty builder, seeded with the given root package names,
    /// which will the builder will attempt to resolve in the next transition.
    pub fn with_entrypoints(root_references: impl IntoIterator<Item = DomainUrl>) -> Self {
        let mut next_source_id = SourceId(1);
        let mut next_domain_index = DomainIndex::second();
        // let mut next_domain_index = DomainIndex::second();
        let request_graph = root_references
            .into_iter()
            .map(|domain_url| {
                let domain_index = next_domain_index;
                if next_domain_index.increase().is_err() {
                    panic!("domain index exceeded");
                }

                let source_id = next_source_id;
                next_source_id.0 += 1;

                (
                    domain_url,
                    RequestedDomain {
                        domain_index,
                        source_id,
                        use_source_span: NO_SPAN,
                        requested_at_generation: TopologyGeneration(0),
                        dependencies: Default::default(),
                        found: false,
                    },
                )
            })
            .collect();

        Self {
            next_source_id,
            next_domain_index,
            generation: TopologyGeneration(0),
            parsed_packages: Default::default(),
            request_graph,
        }
    }

    /// Provide a package
    pub fn provide_domain(&mut self, mut domain: ParsedDomain<S>) {
        let mut children: HashSet<DomainUrl> = HashSet::default();

        let header_data = domain
            .syntax
            .header_data(WithDocs(false), &mut domain.parse_errors);

        for (reference, span) in header_data.deps {
            let url = domain.url.join(&reference);

            self.request_domain(
                url.clone(),
                SourceSpan {
                    source_id: domain.source_id,
                    span,
                },
            );
            children.insert(url);
        }

        let node = self
            .request_graph
            .get_mut(&domain.url)
            .expect("package not requested");
        node.found = true;
        node.dependencies.extend(children);

        self.parsed_packages.insert(domain.source_id, domain);
    }

    /// Try to transition the builder into a PackageTopology.
    /// Before it is able to do that, it may request more packages.
    pub fn transition(mut self) -> Result<GraphState<S>, TopologyErrors> {
        let mut requests = vec![];
        let mut load_errors = vec![];

        for (url, requested_domain) in &self.request_graph {
            if !requested_domain.found {
                if requested_domain.requested_at_generation.0 < self.generation.0 {
                    load_errors.push((
                        TopologyError::DomainNotFound(url.clone()),
                        requested_domain.use_source_span,
                    ));
                } else {
                    requests.push(DomainRequest {
                        domain_index: requested_domain.domain_index,
                        source_id: requested_domain.source_id,
                        url: url.clone(),
                        generation: self.generation,
                    })
                }
            }
        }

        if !load_errors.is_empty() {
            return Err(TopologyErrors {
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
    fn topo_sort(mut self) -> DomainTopology<S> {
        let mut visited: HashSet<SourceId> = Default::default();
        let mut topological_sort: Vec<ParsedDomain<S>> = vec![];

        fn traverse_graph<S>(
            url: &DomainUrl,
            package_graph: &HashMap<DomainUrl, RequestedDomain>,
            parsed_packages: &mut VecMap<SourceId, ParsedDomain<S>>,
            visited: &mut HashSet<SourceId>,
            topological_sort: &mut Vec<ParsedDomain<S>>,
        ) {
            let node = package_graph.get(url).unwrap();
            if visited.contains(&node.source_id) {
                return;
            }
            visited.insert(node.source_id);

            for dep in &node.dependencies {
                traverse_graph(
                    dep,
                    package_graph,
                    parsed_packages,
                    visited,
                    topological_sort,
                );
            }

            let parsed_package = parsed_packages.remove(&node.source_id).unwrap();
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

    fn request_domain(&mut self, url: DomainUrl, use_source_span: SourceSpan) -> SourceId {
        match self.request_graph.entry(url) {
            Entry::Occupied(occupied) => occupied.get().source_id,
            Entry::Vacant(vacant) => {
                let domain_index = self.next_domain_index;
                if self.next_domain_index.increase().is_err() {
                    panic!("domain index exceeeded");
                }

                let source_id = self.next_source_id;
                self.next_source_id.0 += 1;

                vacant.insert(RequestedDomain {
                    domain_index,
                    source_id,
                    use_source_span,
                    requested_at_generation: self.generation,
                    dependencies: Default::default(),
                    found: false,
                });
                source_id
            }
        }
    }
}

struct RequestedDomain {
    domain_index: DomainIndex,
    source_id: SourceId,
    use_source_span: SourceSpan,
    requested_at_generation: TopologyGeneration,
    dependencies: HashSet<DomainUrl>,
    found: bool,
}

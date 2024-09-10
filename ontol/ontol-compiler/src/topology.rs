use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use ontol_parser::cst::inspect as insp;
use ontol_parser::cst::view::NodeView;
use ontol_parser::cst::view::NodeViewExt;
use ontol_parser::U32Span;
use ontol_runtime::ontology::config::DomainConfig;
use ontol_runtime::vec_map::VecMap;
use ontol_runtime::DefId;
use ontol_runtime::DomainIndex;

use crate::error::CompileError;
use crate::error::UnifiedCompileError;
use crate::ontol_syntax::OntolSyntax;
use crate::SourceSpan;
use crate::Sources;
use crate::Src;
use crate::NO_SPAN;

/// The compiler's loaded domains, by reference,
/// to be able to compile `use` statements
#[derive(Default)]
pub(crate) struct LoadedDomains {
    pub by_reference: HashMap<DomainReference, DefId>,
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
    pub reference: DomainReference,
}

/// The reference by which a domain is requested (file name, URL, etc)
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum DomainReference {
    Named(String),
}

/// A package in its parsed form.
/// The parsed form is needed to know which dependencies to request.
pub struct ParsedDomain {
    pub domain_index: DomainIndex,
    pub reference: DomainReference,
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

        let source_name = match &request.reference {
            DomainReference::Named(source_name) => source_name.to_string(),
        };

        let src = sources.add_source(domain_index, source_name);

        Self {
            domain_index: src.domain_index,
            reference: request.reference,
            config,
            src,
            syntax,
            parse_errors,
        }
    }
}

pub fn extract_ontol_dependencies<V: NodeView>(ontol_view: V) -> Vec<(DomainReference, U32Span)> {
    let mut deps: Vec<(DomainReference, U32Span)> = vec![];

    if let insp::Node::Ontol(ontol) = ontol_view.node() {
        for statement in ontol.statements() {
            match statement {
                insp::Statement::DomainStatement(_) => continue,
                insp::Statement::UseStatement(use_stmt) => {
                    if use_stmt
                        .ident_path()
                        .and_then(|path| path.symbols().next())
                        .is_none()
                    {
                        // avoid processing syntactically invalid statement
                        continue;
                    }

                    let Some(name) = use_stmt.name() else {
                        continue;
                    };
                    let Some(Ok(text)) = name.text() else {
                        continue;
                    };

                    let pkg_ref = DomainReference::Named(text);

                    deps.push((pkg_ref, name.0.span()));
                }
                _ => break,
            }
        }
    }

    deps
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
    generation: usize,
    parsed_packages: VecMap<DomainIndex, ParsedDomain>,
    request_graph: HashMap<DomainReference, RequestedDomain>,
}

impl DepGraphBuilder {
    /// Create an empty builder, seeded with the given root package names,
    /// which will the builder will attempt to resolve in the next transition.
    pub fn with_roots(root_package_names: impl IntoIterator<Item = String>) -> Self {
        let mut next_domain_index = DomainIndex::second();
        let package_graph = root_package_names
            .into_iter()
            .map(|package_name| {
                let domain_index = next_domain_index;
                if next_domain_index.increase().is_err() {
                    panic!("package numbers exceeded");
                }

                (
                    DomainReference::Named(package_name),
                    RequestedDomain {
                        domain_index,
                        use_source_span: NO_SPAN,
                        requested_at_generation: 0,
                        dependencies: Default::default(),
                        found: false,
                    },
                )
            })
            .collect();

        Self {
            next_domain_index,
            generation: 0,
            parsed_packages: Default::default(),
            request_graph: package_graph,
        }
    }

    /// Provide a package
    pub fn provide_domain(&mut self, package: ParsedDomain) {
        let mut children: HashSet<DomainReference> = HashSet::default();

        for (pkg_ref, span) in package.syntax.dependencies() {
            self.request_domain(pkg_ref.clone(), package.src.span(span));
            children.insert(pkg_ref);
        }

        let node = self
            .request_graph
            .get_mut(&package.reference)
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

        for (reference, requested_package) in &self.request_graph {
            if !requested_package.found {
                if requested_package.requested_at_generation < self.generation {
                    load_errors.push(
                        CompileError::PackageNotFound(reference.clone())
                            .span(requested_package.use_source_span),
                    );
                } else {
                    requests.push(DomainRequest {
                        domain_index: requested_package.domain_index,
                        reference: reference.clone(),
                    })
                }
            }
        }

        if !load_errors.is_empty() {
            return Err(UnifiedCompileError {
                errors: load_errors,
            });
        }

        self.generation += 1;

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
            reference: &DomainReference,
            package_graph: &HashMap<DomainReference, RequestedDomain>,
            parsed_packages: &mut VecMap<DomainIndex, ParsedDomain>,
            visited: &mut HashSet<DomainIndex>,
            topological_sort: &mut Vec<ParsedDomain>,
        ) {
            let node = package_graph.get(reference).unwrap();
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

        for reference in self.request_graph.keys() {
            traverse_graph(
                reference,
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

    fn request_domain(
        &mut self,
        source: DomainReference,
        use_source_span: SourceSpan,
    ) -> DomainIndex {
        match self.request_graph.entry(source) {
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
    requested_at_generation: usize,
    dependencies: HashSet<DomainReference>,
    found: bool,
}

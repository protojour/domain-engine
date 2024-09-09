use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use fnv::FnvHashMap;
use ontol_parser::cst::inspect as insp;
use ontol_parser::cst::view::NodeView;
use ontol_parser::cst::view::NodeViewExt;
use ontol_parser::U32Span;
use ontol_runtime::ontology::config::DomainConfig;
use ontol_runtime::DefId;
use ontol_runtime::DomainIndex;

use crate::error::CompileError;
use crate::error::UnifiedCompileError;
use crate::ontol_syntax::OntolSyntax;
use crate::SourceSpan;
use crate::Sources;
use crate::Src;
use crate::NO_SPAN;

/// The compiler's loaded packages
#[derive(Default)]
pub(crate) struct Packages {
    pub loaded_packages: HashMap<PackageReference, DefId>,
}

#[derive(Debug)]
pub struct Package {
    pub name: String,
}

pub enum GraphState {
    RequestPackages {
        builder: PackageGraphBuilder,
        requests: Vec<PackageRequest>,
    },
    Built(PackageTopology),
}

/// A package request originating from a source file.
/// ONTOL itself does not know how to fetch any packages.
pub struct PackageRequest {
    pub domain_index: DomainIndex,
    pub reference: PackageReference,
}

/// The reference by which a package is requested (file name, URL, etc)
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum PackageReference {
    Named(String),
}

/// A package in its parsed form.
/// The parsed form is needed to know which dependencies to request.
pub struct ParsedDomain {
    pub domain_index: DomainIndex,
    pub reference: PackageReference,
    pub config: DomainConfig,
    pub src: Src,
    pub syntax: Box<dyn OntolSyntax>,
    pub parse_errors: Vec<ontol_parser::Error>,
}

impl ParsedDomain {
    pub fn new(
        request: PackageRequest,
        syntax: Box<dyn OntolSyntax>,
        parse_errors: Vec<ontol_parser::Error>,
        config: DomainConfig,
        sources: &mut Sources,
    ) -> Self {
        let domain_index = request.domain_index;

        let source_name = match &request.reference {
            PackageReference::Named(source_name) => source_name.to_string(),
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

pub fn extract_ontol_dependentices<V: NodeView>(ontol_view: V) -> Vec<(PackageReference, U32Span)> {
    let mut deps: Vec<(PackageReference, U32Span)> = vec![];

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

                    let pkg_ref = PackageReference::Named(text);

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
pub struct PackageTopology {
    pub packages: Vec<ParsedDomain>,
}

#[derive(Debug)]
pub enum PackageGraphError {
    PackageNotFound(DomainIndex),
}

pub struct PackageGraphBuilder {
    next_domain_index: DomainIndex,
    generation: usize,
    parsed_packages: FnvHashMap<DomainIndex, ParsedDomain>,
    package_graph: HashMap<PackageReference, PackageNode>,
}

impl PackageGraphBuilder {
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
                    PackageReference::Named(package_name),
                    PackageNode {
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
            package_graph,
        }
    }

    /// Provide a package
    pub fn provide_package(&mut self, package: ParsedDomain) {
        let mut children: HashSet<PackageReference> = HashSet::default();

        for (pkg_ref, span) in package.syntax.dependencies() {
            self.request_package(pkg_ref.clone(), package.src.span(span));
            children.insert(pkg_ref);
        }

        let node = self
            .package_graph
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

        for (reference, requested_package) in &self.package_graph {
            if !requested_package.found {
                if requested_package.requested_at_generation < self.generation {
                    load_errors.push(
                        CompileError::PackageNotFound(reference.clone())
                            .span(requested_package.use_source_span),
                    );
                } else {
                    requests.push(PackageRequest {
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
    fn topo_sort(mut self) -> PackageTopology {
        let mut visited: HashSet<DomainIndex> = Default::default();
        let mut topological_sort: Vec<ParsedDomain> = vec![];

        fn traverse_graph(
            reference: &PackageReference,
            package_graph: &HashMap<PackageReference, PackageNode>,
            parsed_packages: &mut FnvHashMap<DomainIndex, ParsedDomain>,
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

        for reference in self.package_graph.keys() {
            traverse_graph(
                reference,
                &self.package_graph,
                &mut self.parsed_packages,
                &mut visited,
                &mut topological_sort,
            );
        }

        PackageTopology {
            packages: topological_sort,
        }
    }

    fn request_package(
        &mut self,
        source: PackageReference,
        use_source_span: SourceSpan,
    ) -> DomainIndex {
        match self.package_graph.entry(source) {
            Entry::Occupied(occupied) => occupied.get().domain_index,
            Entry::Vacant(vacant) => {
                let domain_index = self.next_domain_index;
                if self.next_domain_index.increase().is_err() {
                    panic!("package numbers exceeded");
                }

                vacant.insert(PackageNode {
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

struct PackageNode {
    domain_index: DomainIndex,
    use_source_span: SourceSpan,
    requested_at_generation: usize,
    dependencies: HashSet<PackageReference>,
    found: bool,
}

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use fnv::FnvHashMap;
use ontol_parser::ast;
use ontol_parser::Spanned;
use ontol_runtime::config::PackageConfig;
use ontol_runtime::DefId;
use ontol_runtime::PackageId;
use smartstring::alias::String;

use crate::error::CompileError;
use crate::error::UnifiedCompileError;
use crate::SourceCodeRegistry;
use crate::SourceSpan;
use crate::Sources;
use crate::SpannedCompileError;
use crate::Src;
use crate::NO_SPAN;

pub const ONTOL_PKG: PackageId = PackageId(0);
const ROOT_PKG: PackageId = PackageId(1);

/// The compiler's loaded packages
#[derive(Default, Debug)]
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
    pub package_id: PackageId,
    pub reference: PackageReference,
}

/// The reference by which a package is requested (file name, URL, etc)
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum PackageReference {
    Named(String),
}

/// A package in its parsed form.
/// The parsed form is needed to know which dependencies to request.
pub struct ParsedPackage {
    pub package_id: PackageId,
    pub reference: PackageReference,
    pub config: PackageConfig,
    pub src: Src,
    pub statements: Vec<Spanned<ast::Statement>>,
    pub parser_errors: Vec<ontol_parser::Error>,
}

impl ParsedPackage {
    pub fn parse(
        request: PackageRequest,
        text: &str,
        config: PackageConfig,
        sources: &mut Sources,
        source_code_registry: &mut SourceCodeRegistry,
    ) -> Self {
        let package_id = request.package_id;

        let source_name = match &request.reference {
            PackageReference::Named(source_name) => source_name.to_string(),
        };

        let src = sources.add_source(package_id, source_name);
        source_code_registry.registry.insert(src.id, text.into());

        let (statements, parser_errors) = ontol_parser::parse_statements(text);
        Self {
            package_id: src.package_id,
            reference: request.reference,
            config,
            src,
            statements,
            parser_errors,
        }
    }
}

/// Topological sort of the built package graph
pub struct PackageTopology {
    pub packages: Vec<ParsedPackage>,
}

#[derive(Debug)]
pub enum PackageGraphError {
    PackageNotFound(PackageId),
}

pub struct PackageGraphBuilder {
    next_package_id: PackageId,
    generation: usize,
    parsed_packages: FnvHashMap<PackageId, ParsedPackage>,
    package_graph: HashMap<PackageReference, PackageNode>,
}

impl PackageGraphBuilder {
    /// Create an empty builder, which should produce a request for the root package.
    pub fn new(root_package_name: String) -> Self {
        let generation = 0;
        Self {
            next_package_id: PackageId(ROOT_PKG.0 + 1),
            generation,
            parsed_packages: Default::default(),
            package_graph: [(
                PackageReference::Named(root_package_name),
                PackageNode {
                    package_id: ROOT_PKG,
                    use_source_span: NO_SPAN,
                    requested_at_generation: generation,
                    dependencies: Default::default(),
                    found: false,
                },
            )]
            .into(),
        }
    }

    /// Provide a package
    pub fn provide_package(&mut self, package: ParsedPackage) {
        let mut children: HashSet<PackageReference> = HashSet::default();

        for statement in &package.statements {
            if let (ast::Statement::Use(use_stmt), _) = statement {
                let source = PackageReference::Named(use_stmt.reference.0.clone());

                self.request_package(source.clone(), package.src.span(&use_stmt.reference.1));
                children.insert(source);
            } else {
                // all use statements are at the top of the source file (for now)
                break;
            }
        }

        let node = self
            .package_graph
            .get_mut(&package.reference)
            .expect("package not requested");
        node.found = true;
        node.dependencies.extend(children.into_iter());

        self.parsed_packages.insert(package.package_id, package);
    }

    /// Try to transition the builder into a PackageTopology.
    /// Before it is able to do that, it may request more packages.
    pub fn transition(mut self) -> Result<GraphState, UnifiedCompileError> {
        let mut requests = vec![];
        let mut load_errors = vec![];

        for (reference, requested_package) in &self.package_graph {
            if !requested_package.found {
                if requested_package.requested_at_generation < self.generation {
                    load_errors.push(SpannedCompileError {
                        error: CompileError::PackageNotFound,
                        span: requested_package.use_source_span,
                        notes: vec![],
                    });
                } else {
                    requests.push(PackageRequest {
                        package_id: requested_package.package_id,
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
        let mut visited: HashSet<PackageId> = Default::default();
        let mut topological_sort: Vec<ParsedPackage> = vec![];

        fn traverse_graph(
            reference: &PackageReference,
            package_graph: &HashMap<PackageReference, PackageNode>,
            parsed_packages: &mut FnvHashMap<PackageId, ParsedPackage>,
            visited: &mut HashSet<PackageId>,
            topological_sort: &mut Vec<ParsedPackage>,
        ) {
            let node = package_graph.get(reference).unwrap();
            if visited.contains(&node.package_id) {
                return;
            }
            visited.insert(node.package_id);

            for dep in &node.dependencies {
                traverse_graph(
                    dep,
                    package_graph,
                    parsed_packages,
                    visited,
                    topological_sort,
                );
            }

            let parsed_package = parsed_packages.remove(&node.package_id).unwrap();
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
    ) -> PackageId {
        match self.package_graph.entry(source) {
            Entry::Occupied(occupied) => occupied.get().package_id,
            Entry::Vacant(vacant) => {
                let package_id = self.next_package_id;
                self.next_package_id.0 += 1;

                vacant.insert(PackageNode {
                    package_id,
                    use_source_span,
                    requested_at_generation: self.generation,
                    dependencies: Default::default(),
                    found: false,
                });
                package_id
            }
        }
    }
}

struct PackageNode {
    package_id: PackageId,
    use_source_span: SourceSpan,
    requested_at_generation: usize,
    dependencies: HashSet<PackageReference>,
    found: bool,
}

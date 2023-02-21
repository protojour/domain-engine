use std::collections::hash_map::Entry;
use std::collections::HashMap;

use ontol_parser::ast;
use ontol_parser::Spanned;
use ontol_runtime::PackageId;
use smartstring::alias::String;

use crate::SourceCodeRegistry;
use crate::Sources;

pub const CORE_PKG: PackageId = PackageId(0);
const ROOT_PKG: PackageId = PackageId(1);

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

pub struct PackageRequest {
    pub package_id: PackageId,
    pub package_source: PackageSource,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum PackageSource {
    Root,
    Named(String),
}

pub struct ParsedPackage {
    pub package_id: PackageId,
    pub statements: Vec<Spanned<ast::Statement>>,
    pub parser_errors: Vec<ontol_parser::Error>,
}

impl ParsedPackage {
    pub fn parse(
        package_id: PackageId,
        source_name: &str,
        text: &str,
        sources: &mut Sources,
        source_code_registry: &mut SourceCodeRegistry,
    ) -> Self {
        let src = sources.add_source(package_id, source_name.into());
        source_code_registry.registry.insert(src.id, text.into());

        let (statements, parser_errors) = ontol_parser::parse_statements(text);
        Self {
            package_id: src.package_id,
            statements,
            parser_errors,
        }
    }
}

/// Topological sort of the built package graph
pub struct PackageTopology {
    pub root_package_id: PackageId,
    pub packages: Vec<ParsedPackage>,
}

#[derive(Debug)]
pub enum PackageGraphError {
    PackageNotFound(PackageId),
}

pub struct PackageGraphBuilder {
    root_package_id: PackageId,
    next_package_id: PackageId,
    generation: usize,
    parsed_packages: HashMap<PackageId, ParsedPackage>,
    requested_packages: HashMap<PackageSource, RequestedPackage>,
}

impl Default for PackageGraphBuilder {
    /// Create an empty builder, which should produce a request for the root package.
    fn default() -> Self {
        let generation = 0;
        Self {
            root_package_id: ROOT_PKG,
            next_package_id: PackageId(ROOT_PKG.0 + 1),
            generation,
            parsed_packages: Default::default(),
            requested_packages: [(
                PackageSource::Root,
                RequestedPackage {
                    package_id: ROOT_PKG,
                    requested_at_generation: generation,
                    found: false,
                },
            )]
            .into(),
        }
    }
}

impl PackageGraphBuilder {
    /// Provide a package
    pub fn provide_package(&mut self, source: &PackageSource, package: ParsedPackage) {
        let requested_package = self
            .requested_packages
            .get_mut(&source)
            .expect("package not requested");
        requested_package.found = true;

        for statement in &package.statements {
            if let (ast::Statement::Use(use_stmt), _) = statement {
                self.request_package(PackageSource::Named(use_stmt.source.0.clone()));
            }
        }

        self.parsed_packages.insert(package.package_id, package);
    }

    /// Try to transition the builder into a PackageTopology.
    /// Before it is able to do that, it may request more packages.
    pub fn transition(mut self) -> Result<GraphState, PackageGraphError> {
        let mut requests = vec![];
        for (package_source, requested_package) in &self.requested_packages {
            if !requested_package.found {
                if requested_package.requested_at_generation < self.generation {
                    return Err(PackageGraphError::PackageNotFound(
                        requested_package.package_id,
                    ));
                } else {
                    requests.push(PackageRequest {
                        package_id: requested_package.package_id,
                        package_source: package_source.clone(),
                    })
                }
            }
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
    fn topo_sort(self) -> PackageTopology {
        PackageTopology {
            root_package_id: self.root_package_id,
            packages: self.parsed_packages.into_values().collect(),
        }
    }

    fn request_package(&mut self, source: PackageSource) {
        match self.requested_packages.entry(source) {
            Entry::Occupied(_) => {}
            Entry::Vacant(vacant) => {
                let package_id = self.next_package_id;
                self.next_package_id.0 += 1;

                vacant.insert(RequestedPackage {
                    package_id,
                    requested_at_generation: self.generation,
                    found: false,
                });
            }
        }
    }
}

struct RequestedPackage {
    package_id: PackageId,
    requested_at_generation: usize,
    found: bool,
}

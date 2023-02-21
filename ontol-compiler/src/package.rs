use std::collections::HashMap;

use ontol_parser::ast;
use ontol_parser::Spanned;
use ontol_runtime::PackageId;

use crate::CompileSrc;

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
    pub reference: PackageReference,
}

pub enum PackageReference {
    Root,
}

pub struct ParsedPackage {
    pub package_id: PackageId,
    pub statements: Vec<Spanned<ast::Statement>>,
    pub parser_errors: Vec<ontol_parser::Error>,
}

impl ParsedPackage {
    pub fn parse(src: &CompileSrc) -> Self {
        let (statements, parser_errors) = ontol_parser::parse_statements(&src.text);
        Self {
            package_id: src.package,
            statements,
            parser_errors,
        }
    }
}

/// Topological sort of the built package graph
pub struct PackageTopology {
    pub packages: Vec<ParsedPackage>,
}

pub struct PackageGraphBuilder {
    packages: HashMap<PackageId, ParsedPackage>,
    requests: Vec<PackageRequest>,
}

impl Default for PackageGraphBuilder {
    /// Create an empty builder, which should produce a request for the root package.
    fn default() -> Self {
        Self {
            packages: Default::default(),
            requests: vec![PackageRequest {
                package_id: ROOT_PKG,
                reference: PackageReference::Root,
            }],
        }
    }
}

impl PackageGraphBuilder {
    /// Provide a package
    pub fn provide_package(&mut self, package: ParsedPackage) {
        self.packages.insert(package.package_id, package);
    }

    /// Try to transition the builder into a PackageTopology.
    /// Before it is able to do that, it may request more packages.
    pub fn transition(mut self) -> GraphState {
        if self.requests.is_empty() {
            GraphState::Built(self.topo_sort())
        } else {
            let requests = std::mem::take(&mut self.requests);
            GraphState::RequestPackages {
                builder: self,
                requests,
            }
        }
    }

    /// Finish the package graph with a topological sort of packages
    fn topo_sort(self) -> PackageTopology {
        PackageTopology {
            packages: self.packages.into_values().collect(),
        }
    }
}

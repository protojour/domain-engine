#![forbid(unsafe_code)]

use entity::entity_ctx::EntityCtx;
pub use error::*;
use fnv::FnvHashMap;
use std::ops::Index;

use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::Defs;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    ontology::{config::PackageConfig, ontol::TextConstant, Ontology},
    DefId, PackageId,
};
use ontology_graph::OntologyGraph;
use package::{PackageTopology, Packages};
use pattern::Patterns;
use primitive::Primitives;
use relation::Relations;
use repr::repr_ctx::ReprCtx;
pub use source::*;
use strings::Strings;
use text_patterns::{compile_all_text_patterns, TextPatterns};
use thesaurus::Thesaurus;
use tracing::debug;
use type_check::seal::SealCtx;
use types::{DefTypes, Types};

use crate::lowering::context::CstLowering;

pub mod error;
pub mod mem;
pub mod ontol_syntax;
pub mod ontology_graph;
pub mod package;
pub mod primitive;
pub mod source;

mod codegen;
mod compile_domain;
mod compiler_queries;
mod def;
mod entity;
mod hir_unify;
mod interface;
mod into_ontology;
mod lowering;
mod map;
mod map_arm_def_inference;
mod namespace;
mod ontol_domain;
mod pattern;
mod phf_build;
mod regex_util;
mod relation;
mod repr;
mod sequence;
mod strings;
mod text_patterns;
mod thesaurus;
mod type_check;
mod typed_hir;
mod types;

/// Compile an ONTOL package topology.
///
/// The compilation either completely succeeds or returns a [UnifiedCompileError].
///
/// The errors produces by the compiler mark places in source files according to the [Sources] that is passed in.
pub fn compile(
    topology: PackageTopology,
    sources: Sources,
    mem: &Mem,
) -> Result<Compiled, UnifiedCompileError> {
    let mut compiler = Compiler::new(mem, sources);
    compiler.register_ontol_domain();

    // There could be errors in the ontol domain, this is useful for development:
    compiler.check_error()?;

    for parsed_package in topology.packages {
        debug!(
            "lower {:?}: {:?}",
            parsed_package.package_id, parsed_package.reference
        );
        let source_id = compiler
            .sources
            .source_id_for_package(parsed_package.package_id)
            .expect("no source id available for package");
        let src = compiler
            .sources
            .get_source(source_id)
            .expect("no compiled source available");

        compiler.lower_and_check_next_domain(parsed_package, src)?;
    }

    execute_codegen_tasks(&mut compiler);
    compile_all_text_patterns(&mut compiler);
    compiler.relations.sort_property_tables();
    compiler.check_error()?;

    Ok(Compiled { compiler })
}

/// Value representing a successful compile.
pub struct Compiled<'m> {
    compiler: Compiler<'m>,
}

impl<'m> Compiled<'m> {
    /// Convert the compiled code into an ontol_runtime Ontology.
    pub fn into_ontology(self) -> Ontology {
        self.compiler.into_ontology_inner()
    }

    /// Get the current ontology graph (which is serde-serializable)
    pub fn ontology_graph(&self) -> OntologyGraph<'_, 'm> {
        OntologyGraph::from(&self.compiler)
    }
}

/// An ongoing compilation session
pub struct Session<'c, 'm>(&'c mut Compiler<'m>);

/// The ONTOL compiler data structure.
///
/// It consists of various data types used throughout the compilation session.
struct Compiler<'m> {
    sources: Sources,

    packages: Packages,
    package_names: Vec<(PackageId, TextConstant)>,

    namespaces: Namespaces<'m>,
    defs: Defs<'m>,
    package_def_ids: FnvHashMap<PackageId, DefId>,
    package_config_table: FnvHashMap<PackageId, PackageConfig>,
    primitives: Primitives,
    patterns: Patterns,

    strings: Strings<'m>,
    types: Types<'m>,
    def_types: DefTypes<'m>,
    relations: Relations,
    thesaurus: Thesaurus,
    repr_ctx: ReprCtx,
    seal_ctx: SealCtx,
    text_patterns: TextPatterns,
    entity_ctx: EntityCtx,

    codegen_tasks: CodegenTasks<'m>,

    errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    fn new(mem: &'m Mem, sources: Sources) -> Self {
        let mut defs = Defs::default();
        let primitives = Primitives::new(&mut defs);

        let thesaurus = Thesaurus::new(&primitives);

        Self {
            sources,
            packages: Default::default(),
            package_names: Default::default(),
            namespaces: Default::default(),
            defs,
            package_def_ids: Default::default(),
            package_config_table: Default::default(),
            primitives,
            patterns: Default::default(),
            strings: Strings::new(mem),
            types: Types::new(mem),
            def_types: Default::default(),
            relations: Relations::default(),
            thesaurus,
            repr_ctx: ReprCtx::default(),
            seal_ctx: Default::default(),
            entity_ctx: Default::default(),
            text_patterns: TextPatterns::default(),
            codegen_tasks: Default::default(),
            errors: Default::default(),
        }
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().copied().collect()
    }

    /// Check for errors and bail out of the compilation process now, if in error state.
    fn check_error(&mut self) -> Result<(), UnifiedCompileError> {
        if self.errors.errors.is_empty() {
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: std::mem::take(&mut self.errors.errors),
            })
        }
    }
}

impl<'m> AsRef<Defs<'m>> for Compiler<'m> {
    fn as_ref(&self) -> &Defs<'m> {
        &self.defs
    }
}

impl<'m> AsRef<DefTypes<'m>> for Compiler<'m> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'m> AsRef<Relations> for Compiler<'m> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}

impl<'m> Index<TextConstant> for Compiler<'m> {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.strings[index]
    }
}

/// Lower the ontol syntax to populate the compiler's data structures
pub fn lower_ontol_syntax<V: ontol_parser::cst::view::NodeView>(
    ontol_view: V,
    pkg_def_id: DefId,
    src: Src,
    session: Session,
) -> Vec<DefId> {
    use ontol_parser::cst::view::NodeViewExt;

    CstLowering::new(pkg_def_id, src, session.0)
        .lower_ontol(ontol_view.node())
        .finish()
}

impl<'m> AsMut<CompileErrors> for Compiler<'m> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        &mut self.errors
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::{fs, path::PathBuf};

    use ontol_runtime::{MapDirection, MapFlags};

    use crate::{hir_unify::unify_to_function, mem::Mem, typed_hir::TypedHir, Compiler};

    pub fn hir_parse<'m>(src: &str) -> (ontol_hir::RootNode<'m, TypedHir>, &str) {
        ontol_hir::parse::Parser::new(TypedHir)
            .parse_root(src)
            .unwrap()
    }

    #[rstest::rstest]
    #[ontol_macros::test]
    fn hir_unify(#[files("test-cases/hir-unify/down/**/*.test")] path: PathBuf) {
        let mut direction = MapDirection::Mixed;

        for comp in path.components() {
            if comp.as_os_str() == "down" {
                direction = MapDirection::Down;
            } else if comp.as_os_str() == "up" {
                direction = MapDirection::Up;
            }
        }

        assert!(matches!(direction, MapDirection::Down | MapDirection::Up));

        let contents = fs::read_to_string(path).unwrap();
        let mut without_comments = String::new();
        for line in contents.lines() {
            if !line.starts_with("//") {
                without_comments.push_str(line);
                without_comments.push('\n');
            }
        }

        let mem = Mem::default();
        let (scope, next) = hir_parse(&without_comments);
        let (expr, next) = hir_parse(next);

        let expected = next.trim();

        let output = {
            let mut compiler = Compiler::new(&mem, Default::default());
            let func =
                unify_to_function(&scope, &expr, direction, MapFlags::empty(), &mut compiler)
                    .unwrap();
            let mut output = String::new();
            write!(&mut output, "{func}").unwrap();
            output
        };

        pretty_assertions::assert_eq!(expected, output);
    }
}

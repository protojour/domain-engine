#![forbid(unsafe_code)]

use entity::entity_ctx::EntityCtx;
pub use error::*;
use fnv::FnvHashMap;
use std::ops::Index;

use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::Defs;
use lowering::cst::CstLowering;
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

pub mod error;
pub mod hir_unify;
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

/// The ONTOL compiler data structure.
///
/// It consists of various data types used throughout the compilation session.
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: Packages,
    pub(crate) package_names: Vec<(PackageId, TextConstant)>,

    pub(crate) namespaces: Namespaces<'m>,
    pub(crate) defs: Defs<'m>,
    pub(crate) package_config_table: FnvHashMap<PackageId, PackageConfig>,
    pub(crate) primitives: Primitives,
    pub(crate) patterns: Patterns,

    pub(crate) strings: Strings<'m>,
    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,
    pub(crate) thesaurus: Thesaurus,
    pub(crate) repr_ctx: ReprCtx,
    pub(crate) seal_ctx: SealCtx,
    pub(crate) text_patterns: TextPatterns,
    pub(crate) entity_ctx: EntityCtx,

    pub(crate) codegen_tasks: CodegenTasks<'m>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem, sources: Sources) -> Self {
        let mut defs = Defs::default();
        let primitives = Primitives::new(&mut defs);

        let thesaurus = Thesaurus::new(&primitives);

        Self {
            sources,
            packages: Default::default(),
            package_names: Default::default(),
            namespaces: Default::default(),
            defs,
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

    /// Entry point of all compilation: Compiles the full package topology
    pub fn compile_package_topology(
        &mut self,
        topology: PackageTopology,
    ) -> Result<(), UnifiedCompileError> {
        // There could be errors in the ontol domain, this is useful for development:
        self.check_error()?;

        for parsed_package in topology.packages {
            debug!(
                "lower {:?}: {:?}",
                parsed_package.package_id, parsed_package.reference
            );
            let source_id = self
                .sources
                .source_id_for_package(parsed_package.package_id)
                .expect("no source id available for package");
            let src = self
                .sources
                .get_source(source_id)
                .expect("no compiled source available");

            self.lower_and_check_next_domain(parsed_package, src)?;
        }

        execute_codegen_tasks(self);
        compile_all_text_patterns(self);
        self.relations.sort_property_tables();
        self.check_error()
    }

    /// Get the current ontology graph (which is serde-serializable)
    pub fn ontology_graph(&self) -> OntologyGraph<'_, 'm> {
        OntologyGraph::from(self)
    }

    /// Finish compilation, turn into runtime ontology.
    pub fn into_ontology(self) -> Ontology {
        self.into_ontology_inner()
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

    fn push_error(&mut self, error: SpannedCompileError) {
        self.errors.errors.push(error);
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
    src: Src,
    compiler: &mut Compiler,
) -> Vec<DefId> {
    use ontol_parser::cst::view::NodeViewExt;

    CstLowering::new(compiler, src)
        .lower_ontol(ontol_view.node())
        .finish()
}

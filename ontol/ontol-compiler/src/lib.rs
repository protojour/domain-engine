#![forbid(unsafe_code)]

use edge::EdgeCtx;
use entity::entity_ctx::EntityCtx;
pub use error::*;
use fnv::{FnvHashMap, FnvHashSet};
use lowering::context::LoweringOutcome;
use misc::MiscCtx;
use primitive::init_ontol_primitives;
use properties::PropCtx;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, Index},
};

use codegen::task::{execute_codegen_tasks, CodeCtx};
use def::Defs;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    ontology::{
        config::{DataStoreConfig, DomainConfig},
        domain::DomainId,
        ontol::TextConstant,
        Ontology,
    },
    resolve_path::ResolverGraph,
    DefId, DomainIndex,
};
use pattern::Patterns;
use relation::RelCtx;
use repr::repr_ctx::ReprCtx;
pub use source::*;
use strings::StringCtx;
use text_patterns::{compile_all_text_patterns, TextPatterns};
use thesaurus::Thesaurus;
use topology::{DomainTopology, LoadedDomains};
use tracing::debug;
use type_check::seal::SealCtx;
use types::{DefTypeCtx, TypeCtx};

use crate::lowering::context::CstLowering;

pub mod error;
pub mod mem;
pub mod ontol_syntax;
pub mod primitive;
pub mod source;
pub mod topology;

mod codegen;
mod compile_domain;
mod compiler_queries;
mod def;
mod edge;
mod entity;
mod fmt;
mod hir_unify;
mod interface;
mod into_ontology;
mod lowering;
mod map;
mod map_arm_def_inference;
mod misc;
mod namespace;
mod ontol_domain;
mod pattern;
mod persistence_check;
mod phf_build;
mod properties;
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
    topology: DomainTopology,
    sources: Sources,
    mem: &Mem,
) -> Result<Compiled, UnifiedCompileError> {
    let mut compiler = Compiler::new(mem, sources);
    compiler.register_ontol_domain();

    // There could be errors in the ontol domain, this is useful for development:
    compiler.check_error()?;

    for parsed_package in topology.parsed_domains {
        debug!(
            "lower {:?}: {:?}",
            parsed_package.domain_index, parsed_package.reference
        );
        let source_id = compiler
            .sources
            .source_id_for_domain(parsed_package.domain_index)
            .expect("no source id available for package");
        let src = compiler
            .sources
            .get_source(source_id)
            .expect("no compiled source available");

        compiler.lower_and_check_next_domain(parsed_package, src)?;
    }

    compiler.check_symbolic_edges();

    execute_codegen_tasks(&mut compiler);
    compile_all_text_patterns(&mut compiler);
    compiler.prop_ctx.sort_property_tables();
    compiler.persistence_check();
    compiler.check_error()?;

    compiler.domain_config_table.clear();

    for persistent_domain_index in &compiler.persistent_domains {
        compiler.domain_config_table.insert(
            *persistent_domain_index,
            DomainConfig {
                data_store: Some(DataStoreConfig::Default),
            },
        );
    }

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

    /// Overwrite the data store config of all persistence domains
    pub fn override_data_store(&mut self, config: DataStoreConfig) {
        for (_, pkg_config) in self.compiler.domain_config_table.iter_mut() {
            if pkg_config.data_store.is_some() {
                pkg_config.data_store = Some(config.clone());
            }
        }
    }
}

/// An ongoing compilation session
pub struct Session<'c, 'm>(&'c mut Compiler<'m>);

/// The ONTOL compiler data structure.
///
/// It consists of various data types used throughout the compilation session.
struct Compiler<'m> {
    sources: Sources,

    loaded: LoadedDomains,
    domain_names: Vec<(DomainIndex, TextConstant)>,

    domain_dep_graph: FnvHashMap<DomainIndex, FnvHashSet<DomainIndex>>,
    namespaces: Namespaces<'m>,
    defs: Defs<'m>,
    domain_def_ids: FnvHashMap<DomainIndex, DefId>,
    domain_ids: BTreeMap<DomainIndex, DomainId>,
    domain_config_table: FnvHashMap<DomainIndex, DomainConfig>,
    patterns: Patterns,

    str_ctx: StringCtx<'m>,
    ty_ctx: TypeCtx<'m>,
    def_ty_ctx: DefTypeCtx<'m>,
    rel_ctx: RelCtx,
    prop_ctx: PropCtx,
    edge_ctx: EdgeCtx,
    misc_ctx: MiscCtx,
    thesaurus: Thesaurus,
    repr_ctx: ReprCtx,
    seal_ctx: SealCtx,
    text_patterns: TextPatterns,
    entity_ctx: EntityCtx,

    code_ctx: CodeCtx<'m>,
    resolver_graph: ResolverGraph,

    persistent_domains: BTreeSet<DomainIndex>,

    errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    fn new(mem: &'m Mem, sources: Sources) -> Self {
        let mut defs = Defs::default();
        init_ontol_primitives(&mut defs);

        Self {
            sources,
            loaded: Default::default(),
            domain_names: Default::default(),
            domain_dep_graph: Default::default(),
            namespaces: Default::default(),
            defs,
            domain_def_ids: Default::default(),
            domain_ids: Default::default(),
            domain_config_table: Default::default(),
            patterns: Default::default(),
            str_ctx: StringCtx::new(mem),
            ty_ctx: TypeCtx::new(mem),
            def_ty_ctx: Default::default(),
            rel_ctx: RelCtx::default(),
            prop_ctx: PropCtx::default(),
            misc_ctx: MiscCtx::default(),
            edge_ctx: EdgeCtx::default(),
            thesaurus: Thesaurus::default(),
            repr_ctx: ReprCtx::default(),
            seal_ctx: Default::default(),
            entity_ctx: Default::default(),
            text_patterns: TextPatterns::default(),
            code_ctx: Default::default(),
            resolver_graph: Default::default(),
            persistent_domains: Default::default(),
            errors: Default::default(),
        }
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

impl<'m> AsRef<DefTypeCtx<'m>> for Compiler<'m> {
    fn as_ref(&self) -> &DefTypeCtx<'m> {
        &self.def_ty_ctx
    }
}

impl<'m> AsRef<RelCtx> for Compiler<'m> {
    fn as_ref(&self) -> &RelCtx {
        &self.rel_ctx
    }
}

impl<'m> Index<TextConstant> for Compiler<'m> {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.str_ctx[index]
    }
}

/// Lower the ontol syntax to populate the compiler's data structures
pub fn lower_ontol_syntax<V: ontol_parser::cst::view::NodeView>(
    ontol_view: V,
    domain_def_id: DefId,
    src: Src,
    session: Session,
) -> LoweringOutcome {
    use ontol_parser::cst::view::NodeViewExt;

    CstLowering::new(domain_def_id, src, session.0)
        .lower_ontol(ontol_view.node())
        .finish()
}

impl<'m> AsMut<CompileErrors> for Compiler<'m> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        &mut self.errors
    }
}

enum OwnedOrRef<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> Deref for OwnedOrRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(t) => t,
            Self::Borrowed(t) => t,
        }
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

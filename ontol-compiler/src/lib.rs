use codegen::{execute_codegen_tasks, CodegenTasks};
use compiler_queries::GetPropertyMeta;
use def::{DefKind, Defs, TypeDef};
use error::{CompileError, ParseError, UnifiedCompileError};

pub use error::*;
use expr::{Expr, ExprId};
use fnv::FnvHashMap;
use lowering::Lowering;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    env::{Domain, EntityInfo, Env, TypeInfo},
    serde::SerdeKey,
    DataModifier, DefId, DefVariant, PackageId,
};
use package::{PackageTopology, Packages};
use patterns::{compile_all_patterns, Patterns};
use relation::Relations;
pub use source::*;
use strings::Strings;
use tracing::debug;
use types::{DefTypes, Types};

pub mod error;
pub mod mem;
pub mod package;
pub mod serde_codegen;

mod codegen;
mod compiler_queries;
mod core;
mod def;
mod expr;
mod lowering;
mod namespace;
mod patterns;
mod regex_util;
mod relation;
mod sequence;
mod source;
mod strings;
mod type_check;
mod typed_expr;
mod types;

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: Packages,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs<'m>,
    pub(crate) expressions: FnvHashMap<ExprId, Expr>,

    pub(crate) strings: Strings<'m>,
    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,
    pub(crate) patterns: Patterns,

    pub(crate) codegen_tasks: CodegenTasks<'m>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem, sources: Sources) -> Self {
        Self {
            packages: Default::default(),
            sources,
            strings: Strings::new(mem),
            types: Types::new(mem),
            namespaces: Default::default(),
            defs: Defs::new(mem),
            expressions: Default::default(),
            def_types: Default::default(),
            relations: Relations::default(),
            patterns: Patterns::default(),
            codegen_tasks: Default::default(),
            errors: Default::default(),
        }
    }

    /// Entry point of all compilation: Compiles the full package topology
    pub fn compile_package_topology(
        &mut self,
        topology: PackageTopology,
    ) -> Result<(), UnifiedCompileError> {
        let mut root_defs = vec![];

        for parsed_package in topology.packages {
            debug!(
                "lower package {:?}: {:?}",
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

            for error in parsed_package.parser_errors {
                self.push_error(match error {
                    ontol_parser::Error::Lex(lex_error) => {
                        let span = lex_error.span();
                        CompileError::Lex(LexError::new(lex_error)).spanned(&src.span(&span))
                    }
                    ontol_parser::Error::Parse(parse_error) => {
                        let span = parse_error.span();
                        CompileError::Parse(ParseError::new(parse_error)).spanned(&src.span(&span))
                    }
                });
            }

            let package_def_id = self.define_package(parsed_package.package_id);
            self.packages
                .loaded_packages
                .insert(parsed_package.reference, package_def_id);

            let mut lowering = Lowering::new(self, &src);

            for stmt in parsed_package.statements {
                let _ignored = lowering.lower_statement(stmt);
            }

            root_defs.append(&mut lowering.finish());
        }

        self.compile_all_packages(root_defs)
    }

    fn compile_all_packages(&mut self, root_defs: Vec<DefId>) -> Result<(), UnifiedCompileError> {
        let mut type_check = self.type_check();
        for root_def in root_defs {
            type_check.check_def(root_def);
        }

        // Call this after all source files have been compiled
        compile_all_patterns(self);

        self.type_check().check_domain_types();
        self.type_check().check_unions();
        self.check_error()?;

        execute_codegen_tasks(self);
        self.check_error()?;

        Ok(())
    }

    /// Finish compilation, turn into environment.
    pub fn into_env(mut self) -> Env {
        let package_ids = self.package_ids();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut serde_generator = self.serde_generator();

        let mut builder = Env::builder();

        // For now, create serde operators for every domain
        for package_id in package_ids {
            let mut domain = Domain::default();

            let type_namespace = namespaces.remove(&package_id).unwrap().types;

            for (type_name, type_def_id) in type_namespace {
                let entity_info =
                    if let Some(properties) = self.relations.properties_by_type(type_def_id) {
                        if let Some(id_relation_id) = &properties.id {
                            let (relationship, _) = self
                                .get_subject_property_meta(type_def_id, *id_relation_id)
                                .expect("BUG: problem getting property meta");

                            Some(EntityInfo {
                                id_relation_id: *id_relation_id,
                                id_value_def_id: relationship.object.0.def_id,
                                id_operator_id: serde_generator
                                    .get_serde_operator_id(SerdeKey::Def(DefVariant::new(
                                        relationship.object.0.def_id,
                                        DataModifier::IDENTITY,
                                    )))
                                    .unwrap(),
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                let public = match self.defs.get_def_kind(type_def_id) {
                    Some(DefKind::Type(TypeDef { public, .. })) => *public,
                    _ => true,
                };

                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public,
                    name: type_name,
                    entity_info,
                    operator_id: serde_generator.get_serde_operator_id(SerdeKey::Def(
                        DefVariant::new(type_def_id, DataModifier::default()),
                    )),
                });
            }

            builder.add_domain(package_id, domain);
        }

        let (serde_operators, serde_operators_per_def) = serde_generator.finish();

        builder
            .lib(self.codegen_tasks.result_lib)
            .translations(self.codegen_tasks.result_translations)
            .serde_operators(serde_operators, serde_operators_per_def)
            .string_like_types(self.defs.string_like_types)
            .string_patterns(self.patterns.string_patterns)
            .build()
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().copied().collect()
    }

    /// Check for errors and bail out of the compilation process now, if in error state.
    pub(crate) fn check_error(&mut self) -> Result<(), UnifiedCompileError> {
        if self.errors.errors.is_empty() {
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: std::mem::take(&mut self.errors.errors),
            })
        }
    }

    pub(crate) fn push_error(&mut self, error: SpannedCompileError) {
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

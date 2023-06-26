use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::{DefKind, Defs, RelationshipMeta, TypeDef};
use error::{CompileError, ParseError, UnifiedCompileError};

pub use error::*;
use expr::Expressions;
use fnv::FnvHashMap;
use lowering::Lowering;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    config::PackageConfig,
    env::{Domain, EntityInfo, Env, TypeInfo},
    serde::SerdeKey,
    value::PropertyId,
    DataModifier, DefId, DefVariant, PackageId,
};
use package::{PackageTopology, Packages};
use patterns::{compile_all_patterns, Patterns};
use primitive::Primitives;
use relation::{Properties, Relations};
pub use source::*;
use strings::Strings;
use tracing::debug;
use types::{DefTypes, Types};

pub mod error;
pub mod hir_unify;
pub mod mem;
pub mod package;
pub mod serde_codegen;
pub mod source;
pub mod typed_hir;

mod codegen;
mod compiler_queries;
mod core;
mod def;
mod expr;
mod lowering;
mod namespace;
mod patterns;
mod primitive;
mod regex_util;
mod relation;
mod sequence;
mod strings;
mod type_check;
mod types;

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: Packages,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs<'m>,
    pub(crate) package_config_table: FnvHashMap<PackageId, PackageConfig>,
    pub(crate) primitives: Primitives,
    pub(crate) expressions: Expressions,

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
        let mut defs = Defs::new(mem);
        let primitives = Primitives::new(&mut defs);

        Self {
            sources,
            packages: Default::default(),
            namespaces: Default::default(),
            defs,
            package_config_table: Default::default(),
            primitives,
            expressions: Default::default(),
            strings: Strings::new(mem),
            types: Types::new(mem),
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

            self.package_config_table
                .insert(parsed_package.package_id, parsed_package.config);

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
        let mut package_config_map = std::mem::take(&mut self.package_config_table);
        let docs = std::mem::take(&mut self.namespaces.docs);
        let mut serde_generator = self.serde_generator();

        let mut builder = Env::builder();

        let dynamic_sequence_operator_id = serde_generator.make_dynamic_sequence_operator();

        // For now, create serde operators for every domain
        for package_id in package_ids {
            let mut domain = Domain::default();

            let namespace = namespaces.remove(&package_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_map.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            for (type_name, type_def_id) in type_namespace {
                let entity_info =
                    if let Some(properties) = self.relations.properties_by_type(type_def_id) {
                        if let Some(id_relationship_id) = &properties.identified_by {
                            let identifies_meta = self
                                .defs
                                .lookup_relationship_meta(*id_relationship_id)
                                .expect("BUG: problem getting property meta");

                            Some(EntityInfo {
                                id_relationship_id: *id_relationship_id,
                                id_value_def_id: identifies_meta.relationship.subject.0.def_id,
                                id_operator_id: serde_generator
                                    .gen_operator_id(SerdeKey::Def(DefVariant::new(
                                        identifies_meta.relationship.subject.0.def_id,
                                        DataModifier::NONE,
                                    )))
                                    .unwrap(),
                                id_inherent_property_name: match self
                                    .find_inherent_primary_id(type_def_id, properties)
                                {
                                    Some(meta) => meta
                                        .relation
                                        .subject_prop(&self.defs)
                                        .map(|name| name.into()),
                                    None => None,
                                },
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
                    name: Some(type_name),
                    entity_info,
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(DefVariant::new(
                        type_def_id,
                        DataModifier::default(),
                    ))),
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: false,
                    name: None,
                    entity_info: None,
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(DefVariant::new(
                        type_def_id,
                        DataModifier::default(),
                    ))),
                });
            }

            builder.add_domain(package_id, domain);
        }

        let (serde_operators, serde_operators_per_def) = serde_generator.finish();

        builder
            .lib(self.codegen_tasks.result_lib)
            .docs(docs)
            .const_procs(self.codegen_tasks.result_const_procs)
            .mapper_procs(self.codegen_tasks.result_map_procs)
            .serde_operators(serde_operators, serde_operators_per_def)
            .dynamic_sequence_operator_id(dynamic_sequence_operator_id)
            .string_like_types(self.defs.string_like_types)
            .string_patterns(self.patterns.string_patterns)
            .build()
    }

    fn find_inherent_primary_id(
        &self,
        _entity_id: DefId,
        properties: &Properties,
    ) -> Option<RelationshipMeta<'m>> {
        let id_relationship_id = properties.identified_by?;
        let inherent_id = self
            .relations
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.map.as_ref()?;
        let _property = map.get(&PropertyId::subject(inherent_id))?;

        self.defs.lookup_relationship_meta(inherent_id).ok()
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

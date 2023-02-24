//! Central compiler data structure

use std::collections::HashMap;

use crate::{
    codegen::CodegenTasks,
    def::Defs,
    error::{CompileErrors, UnifiedCompileError},
    expr::{Expr, ExprId},
    mem::Mem,
    namespace::Namespaces,
    package::Packages,
    patterns::Patterns,
    relation::Relations,
    source::Sources,
    strings::Strings,
    types::{DefTypes, Types},
    SpannedCompileError,
};
use fnv::FnvHashMap;
use ontol_runtime::{
    env::{Domain, Env, TypeInfo},
    serde::SerdeOperatorKey,
    string_types::StringLikeType,
    PackageId,
};
use smartstring::alias::String;

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
}

impl<'m> Compiler<'m> {
    /// Finish compilation, turn into environment.
    pub fn into_env(mut self) -> Env {
        let package_ids = self.package_ids();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut serde_generator = self.serde_generator();

        let mut out_domains: FnvHashMap<PackageId, Domain> = Default::default();

        // For now, create serde operators for every domain
        for package_id in package_ids {
            let mut types: HashMap<String, TypeInfo> = Default::default();

            let type_namespace = namespaces.remove(&package_id).unwrap().types;

            for (type_name, type_def_id) in type_namespace {
                types.insert(
                    type_name,
                    TypeInfo {
                        def_id: type_def_id,
                        serde_operator_id: serde_generator
                            .get_serde_operator_id(SerdeOperatorKey::Identity(type_def_id)),
                    },
                );
            }

            out_domains.insert(package_id, Domain { types });
        }

        let (serde_operators, serde_operators_per_def) = serde_generator.finish();

        Env {
            domains: out_domains,
            lib: self.codegen_tasks.result_lib,
            translations: self.codegen_tasks.result_translations,
            serde_operators_per_def,
            serde_operators,
            string_like_types: HashMap::from_iter([(self.defs.uuid(), StringLikeType::Uuid)]),
            string_patterns: self.patterns.string_patterns,
        }
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

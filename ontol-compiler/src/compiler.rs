//! Central compiler data structure

use std::collections::HashMap;

use crate::{
    codegen::CodegenTasks,
    def::Defs,
    error::{CompileErrors, UnifiedCompileError},
    expr::{Expr, ExprId},
    mem::Mem,
    namespace::Namespaces,
    relation::Relations,
    source::{Package, Sources},
    types::{DefTypes, Types},
    SpannedCompileError,
};
use ontol_runtime::{
    env::{Domain, Env, TypeInfo},
    PackageId,
};
use smartstring::alias::String;

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: HashMap<PackageId, Package>,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,

    pub(crate) codegen_tasks: CodegenTasks<'m>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            sources: Default::default(),
            packages: Default::default(),
            types: Types::new(mem),
            namespaces: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
            relations: Default::default(),
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

        let mut out_domains: HashMap<PackageId, Domain> = Default::default();

        // For now, create serde operators for every domain
        for package_id in package_ids {
            let mut types: HashMap<String, TypeInfo> = Default::default();

            let type_namespace = namespaces.remove(&package_id).unwrap().types;

            for (type_name, type_def_id) in type_namespace {
                types.insert(
                    type_name,
                    TypeInfo {
                        def_id: type_def_id,
                        serde_operator_id: serde_generator.get_serde_operator_id(type_def_id),
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
        }
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().map(|id| *id).collect()
    }

    ///
    pub(crate) fn check_error(&mut self) -> Result<(), UnifiedCompileError> {
        if self.errors.errors.is_empty() {
            Ok(())
        } else {
            let mut errors = vec![];
            errors.append(&mut self.errors.errors);
            Err(UnifiedCompileError { errors })
        }
    }

    pub(crate) fn push_error(&mut self, error: SpannedCompileError) {
        self.errors.errors.push(error);
    }
}

impl<'m> AsRef<Defs> for Compiler<'m> {
    fn as_ref(&self) -> &Defs {
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

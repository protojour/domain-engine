//! Central compiler data structure

use std::{collections::HashMap, sync::Arc};

use smartstring::alias::String;

use crate::{
    binding::Bindings,
    compile::error::CompileErrors,
    def::{DefId, Defs},
    env::{Domain, Env, TypeInfo},
    expr::{Expr, ExprId},
    mem::Mem,
    namespace::Namespaces,
    relation::Relations,
    source::{Package, PackageId, Sources},
    types::{DefTypes, Types},
};

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: HashMap<PackageId, Package>,
    pub bindings: Bindings<'m>,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            sources: Default::default(),
            packages: Default::default(),
            bindings: Bindings::new(mem),
            types: Types::new(mem),
            namespaces: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
            relations: Default::default(),
            errors: Default::default(),
        }
    }
}

impl<'m> Compiler<'m> {
    /// Finish compilation, turn into environment.
    pub fn into_env(mut self) -> Arc<Env> {
        let mut domains: HashMap<PackageId, Domain> = Default::default();

        // For now, create serde operators for every domain
        for package_id in self.package_ids() {
            let mut types: HashMap<String, TypeInfo> = Default::default();

            let domain_binding = self.bindings_builder().new_binding(package_id);

            for (type_name, operator_id) in domain_binding.serde_operators {
                types.insert(
                    type_name,
                    TypeInfo {
                        serde_operator_id: Some(operator_id),
                    },
                );
            }

            domains.insert(package_id, Domain { types });
        }

        Arc::new(Env {
            serde_operators: self.bindings.serde_operators,
            domains,
        })
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().map(|id| *id).collect()
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

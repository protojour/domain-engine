//! Central compiler data structure

use std::{collections::HashMap, sync::Arc};

use crate::{
    compile::error::CompileErrors,
    def::Defs,
    expr::{Expr, ExprId},
    mem::Mem,
    namespace::{Namespaces, Space},
    relation::Relations,
    source::{Package, Sources},
    types::{DefTypes, Types},
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
            errors: Default::default(),
        }
    }
}

impl<'m> Compiler<'m> {
    /// Finish compilation, turn into environment.
    pub fn into_env(self) -> Arc<Env> {
        let mut domains: HashMap<PackageId, Domain> = Default::default();

        let mut serde_generator = self.serde_generator();

        // For now, create serde operators for every domain
        for package_id in self.package_ids() {
            let mut types: HashMap<String, TypeInfo> = Default::default();

            for (type_name, def_id) in self.namespaces.namespaces[&package_id].space(Space::Type) {
                types.insert(
                    type_name.clone(),
                    TypeInfo {
                        serde_operator_id: serde_generator.get_serde_operator_id(*def_id),
                    },
                );
            }

            domains.insert(package_id, Domain { types });
        }

        Arc::new(Env {
            serde_operators: serde_generator.finish(),
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

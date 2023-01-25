//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    binding::Bindings,
    compile::error::CompileErrors,
    def::{Defs, Namespaces},
    expr::{Expr, ExprId},
    mem::Mem,
    relation::Relations,
    source::{Package, PackageId, Sources},
    types::{DefTypes, Types},
};

/// Runtime environment
#[derive(Debug)]
pub struct Env<'m> {
    pub(crate) mem: &'m Mem,
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

impl<'m> Env<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
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

impl<'m> AsRef<Defs> for Env<'m> {
    fn as_ref(&self) -> &Defs {
        &self.defs
    }
}

impl<'m> AsRef<DefTypes<'m>> for Env<'m> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'m> AsRef<Relations> for Env<'m> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}

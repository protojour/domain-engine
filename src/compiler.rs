//! Central compiler data structure

use std::collections::HashMap;

use crate::{
    binding::Bindings,
    compile::error::CompileErrors,
    def::Defs,
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

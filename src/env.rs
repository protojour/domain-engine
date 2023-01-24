//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    compile::error::CompileErrors,
    def::{DefId, Defs, Namespaces},
    expr::{Expr, ExprId},
    mem::Mem,
    relation::Relations,
    source::{Package, PackageId, Sources},
    types::{TypeRef, Types},
};

/// Runtime environment
#[derive(Debug)]
pub struct Env<'m> {
    pub(crate) sources: Sources,

    pub(crate) packages: HashMap<PackageId, Package>,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: HashMap<DefId, TypeRef<'m>>,
    pub(crate) relations: Relations,

    pub(crate) errors: CompileErrors,
}

impl<'m> Env<'m> {
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

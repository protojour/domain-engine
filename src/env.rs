//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    compile_error::CompileErrors,
    def::{Def, DefId},
    expr::{Expr, ExprId},
    misc::{Package, PackageId, Source, SourceId},
    types::{TypeRef, Types},
};

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

pub struct Env<'m> {
    pub(crate) def_counter: u32,
    pub(crate) expr_counter: u32,

    pub(crate) packages: HashMap<PackageId, Package>,
    pub(crate) sources: HashMap<SourceId, Source>,

    pub(crate) namespace: HashMap<PackageId, HashMap<String, DefId>>,
    pub(crate) defs: HashMap<DefId, Def>,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: HashMap<DefId, TypeRef<'m>>,

    pub(crate) errors: CompileErrors,
}

/// Intern something in an arena
pub trait Intern<T> {
    type Facade;

    fn intern(&mut self, value: T) -> Self::Facade;
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            def_counter: Default::default(),
            expr_counter: Default::default(),
            packages: Default::default(),
            sources: Default::default(),
            types: Types::new(mem),
            namespace: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
            errors: Default::default(),
        }
    }
}

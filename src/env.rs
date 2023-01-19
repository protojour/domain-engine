//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    compile::error::CompileErrors,
    def::{Def, DefId, Namespaces},
    expr::{Expr, ExprId},
    mem::Mem,
    source::{Package, PackageId, Sources},
    types::{TypeRef, Types},
};

/// Runtime environment
#[derive(Debug)]
pub struct Env<'m> {
    next_def_id: DefId,
    next_expr_id: ExprId,

    pub(crate) sources: Sources,

    pub(crate) packages: HashMap<PackageId, Package>,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: HashMap<DefId, Def>,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: HashMap<DefId, TypeRef<'m>>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            next_def_id: DefId(0),
            next_expr_id: ExprId(0),
            sources: Default::default(),
            packages: Default::default(),
            types: Types::new(mem),
            namespaces: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
            errors: Default::default(),
        }
    }

    pub fn alloc_def_id(&mut self) -> DefId {
        let id = self.next_def_id;
        self.next_def_id.0 += 1;
        id
    }

    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }
}

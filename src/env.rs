//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    compile_error::CompileErrors,
    def::{Def, DefId},
    expr::{Expr, ExprId},
    mem::Mem,
    misc::{Package, PackageId, Source, SourceId},
    types::{TypeRef, Types},
    SString,
};

pub struct Env<'m> {
    next_def_id: DefId,
    next_expr_id: ExprId,

    pub(crate) packages: HashMap<PackageId, Package>,
    pub(crate) sources: HashMap<SourceId, Source>,

    pub(crate) namespace: HashMap<PackageId, HashMap<SString, DefId>>,
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

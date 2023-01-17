//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::atomic::AtomicU32,
};

use crate::{
    def::{Def, DefId},
    expr::{Expr, ExprId},
    misc::{Package, PackageId, Source, SourceId},
    types::{Type, TypeKind},
};

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

pub struct Env<'m> {
    mem: &'m Mem,

    pub(crate) def_counter: AtomicU32,
    pub(crate) expr_counter: AtomicU32,

    pub(crate) packages: HashMap<PackageId, Package>,
    pub(crate) sources: HashMap<SourceId, Source>,

    pub(crate) strings: RefCell<HashSet<&'m str>>,
    pub(crate) types: RefCell<HashSet<&'m TypeKind<'m>>>,
    pub(crate) type_slices: RefCell<HashSet<&'m [Type<'m>]>>,
    pub(crate) namespace: RefCell<HashMap<PackageId, HashMap<String, DefId>>>,
    pub(crate) defs: RefCell<HashMap<DefId, Def>>,
    pub(crate) expressions: RefCell<HashMap<ExprId, Expr>>,

    pub(crate) def_types: RefCell<HashMap<DefId, Type<'m>>>,
}

pub trait Intern<T> {
    type Facade;

    fn intern(&self, value: T) -> Self::Facade;
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            def_counter: Default::default(),
            expr_counter: Default::default(),
            packages: Default::default(),
            sources: Default::default(),
            strings: Default::default(),
            types: Default::default(),
            type_slices: Default::default(),
            namespace: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
        }
    }

    pub fn bump(&self) -> &'m bumpalo::Bump {
        &self.mem.bump
    }
}

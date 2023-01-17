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
    types::{Type, TypeKind, Types},
};

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

pub struct Env<'m> {
    pub(crate) def_counter: AtomicU32,
    pub(crate) expr_counter: AtomicU32,

    pub(crate) packages: HashMap<PackageId, Package>,
    pub(crate) sources: HashMap<SourceId, Source>,

    pub(crate) types: Types<'m>,
    pub(crate) namespace: RefCell<HashMap<PackageId, HashMap<String, DefId>>>,
    pub(crate) defs: RefCell<HashMap<DefId, Def>>,
    pub(crate) defs2: HashMap<DefId, Def>,
    pub(crate) expressions: RefCell<HashMap<ExprId, Expr>>,

    pub(crate) def_types: HashMap<DefId, Type<'m>>,
}

pub trait InternMut<T> {
    type Facade;

    fn intern_mut(&mut self, value: T) -> Self::Facade;
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
            defs2: Default::default(),
            expressions: Default::default(),
            // def_types: Default::default(),
            def_types: Default::default(),
        }
    }
}

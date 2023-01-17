//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::{cell::RefCell, collections::HashSet};

use crate::types::TypeKind;

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

pub struct Env<'m> {
    mem: &'m Mem,
    pub(crate) strings: RefCell<HashSet<&'m str>>,
    pub(crate) types: RefCell<HashSet<&'m TypeKind<'m>>>,
}

pub trait Intern<T> {
    type Facade;

    fn intern(&self, value: T) -> Self::Facade;
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m mut Mem) -> Self {
        Self {
            mem,
            strings: Default::default(),
            types: Default::default(),
        }
    }

    pub fn bump(&self) -> &'m bumpalo::Bump {
        &self.mem.bump
    }
}

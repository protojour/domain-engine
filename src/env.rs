//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::{collections::HashMap, ops::Deref};

use crate::types::{Type, TypeKind};

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

pub struct Env<'m> {
    mem: &'m Mem,
    types: HashMap<&'m str, Type<'m>>,
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m mut Mem) -> Self {
        Self {
            mem,
            types: Default::default(),
        }
    }

    pub fn bump(&self) -> &'m bumpalo::Bump {
        &self.mem.bump
    }
}

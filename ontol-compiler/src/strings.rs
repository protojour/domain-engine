use std::{collections::HashSet, fmt::Debug};

use crate::mem::Mem;

pub struct Strings<'m> {
    mem: &'m Mem,
    strings: HashSet<&'m str>,
}

impl<'m> Strings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            strings: Default::default(),
        }
    }

    pub fn intern(&mut self, str: &str) -> &'m str {
        if let Some(str) = self.strings.get(str) {
            return str;
        }
        let str = self.mem.bump.alloc_str(str);
        self.strings.insert(str);
        str
    }
}

impl<'m> Debug for Strings<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Strings").finish()
    }
}

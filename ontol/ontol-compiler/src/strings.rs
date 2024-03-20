use std::{collections::HashMap, fmt::Debug, ops::Index};

use arcstr::ArcStr;
use ontol_runtime::{ontology::ontol::TextConstant, phf::PhfKey};

use crate::mem::Mem;

/// TODO: Might not need to use the bump allocator,
/// could use owned ArcStr values instead.
///
/// Many compiler types should switch to storing TextConstant (index) anyway
pub struct Strings<'m> {
    mem: &'m Mem,
    table: HashMap<&'m str, Option<TextConstant>>,
    constants: Vec<&'m str>,
    detached: bool,
}

impl<'m> Strings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            constants: vec![],
            table: Default::default(),
            detached: false,
        }
    }

    /// Strings are detached to a standalone value at the end of the compilation session.
    /// self should not be used for interning anymore after that.
    pub fn detach(&mut self) -> Self {
        self.detached = true;
        Self {
            mem: self.mem,
            table: std::mem::take(&mut self.table),
            constants: std::mem::take(&mut self.constants),
            detached: false,
        }
    }

    pub fn attach(&mut self, other: Strings<'m>) {
        self.table = other.table;
        self.constants = other.constants;
        self.detached = false;
    }

    pub fn make_text_constants(self) -> Vec<ArcStr> {
        self.constants
            .iter()
            .map(|str| ArcStr::from(*str))
            .collect()
    }

    pub fn intern(&mut self, str: &str) -> &'m str {
        assert!(!self.detached);

        if let Some((interned_str, _)) = self.table.get_key_value(str) {
            return interned_str;
        }

        let str = self.mem.bump.alloc_str(str);
        self.table.insert(str, None);

        str
    }

    /// Get a TextConstant, a representation of a deduplicated text literal for use with the compiled Ontology.
    pub fn intern_constant(&mut self, str: &str) -> TextConstant {
        assert!(!self.detached);

        if let Some((interned_str, constant)) = self.table.get_key_value(str).map(|(k, v)| (*k, *v))
        {
            return match constant {
                Some(constant) => constant,
                None => {
                    let constant = push_constant(&mut self.constants, interned_str);
                    *self.table.get_mut(interned_str).unwrap() = Some(constant);

                    constant
                }
            };
        }

        let interned_str = self.mem.bump.alloc_str(str);

        let constant = push_constant(&mut self.constants, interned_str);
        self.table.insert(interned_str, Some(constant));

        constant
    }

    pub fn make_phf_key(&mut self, ident: &str) -> PhfKey {
        let string = ArcStr::from(ident);
        let constant = self.intern_constant(ident);

        PhfKey { string, constant }
    }

    pub fn get_constant(&self, str: &str) -> Option<TextConstant> {
        self.table.get(str).cloned().flatten()
    }
}

fn push_constant<'m>(vec: &mut Vec<&'m str>, str: &'m str) -> TextConstant {
    let next = vec.len();
    vec.push(str);
    TextConstant(next as u32)
}

impl<'m> Debug for Strings<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Strings").finish()
    }
}

impl<'m> Index<TextConstant> for Strings<'m> {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        self.constants[index.0 as usize]
    }
}

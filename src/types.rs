use std::{collections::HashSet, ops::Deref};

use crate::{
    def::DefId,
    env::{Env, InternMut, Mem},
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Type<'m>(&'m TypeKind<'m>);

impl<'m> Type<'m> {
    pub fn kind(&self) -> &TypeKind<'m> {
        self.0
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum TypeKind<'m> {
    Constant(i32),
    Number,
    New(DefId, Type<'m>),
    Function {
        args: &'m [Type<'m>],
        output: Type<'m>,
    },
}

impl<'m> Deref for Type<'m> {
    type Target = TypeKind<'m>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Types<'m> {
    mem: &'m Mem,
    pub(crate) types: HashSet<&'m TypeKind<'m>>,
    pub(crate) slices: HashSet<&'m [Type<'m>]>,
}

impl<'m> Types<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            types: Default::default(),
            slices: Default::default(),
        }
    }
}

impl<'m> InternMut<TypeKind<'m>> for Types<'m> {
    type Facade = Type<'m>;

    fn intern_mut(&mut self, kind: TypeKind<'m>) -> Self::Facade {
        match self.types.get(&kind) {
            Some(kind) => Type(kind),
            None => {
                let kind = self.mem.bump.alloc(kind);
                self.types.insert(kind);
                Type(kind)
            }
        }
    }
}

impl<'m, const N: usize> InternMut<[Type<'m>; N]> for Types<'m> {
    type Facade = &'m [Type<'m>];

    fn intern_mut(&mut self, types: [Type<'m>; N]) -> Self::Facade {
        match self.slices.get(types.as_slice()) {
            Some(slice) => slice,
            None => {
                let slice = self.mem.bump.alloc_slice_fill_iter(types.into_iter());
                self.slices.insert(slice);
                slice
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::Mem;

    fn type_ptr(ty: Type) -> usize {
        ty.kind() as *const _ as usize
    }

    #[test]
    fn dedup_types() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);

        let c0 = env.types.intern_mut(TypeKind::Constant(42));
        let c1 = env.types.intern_mut(TypeKind::Constant(42));
        let c2 = env.types.intern_mut(TypeKind::Constant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }

    #[test]
    fn test_alloc_type() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);

        let constant = env.types.intern_mut(TypeKind::Constant(42));
        let ty = env.types.intern_mut(TypeKind::New(DefId(42), constant));
    }
}

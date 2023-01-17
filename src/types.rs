use std::collections::HashSet;

use crate::{
    def::DefId,
    env::{Intern, Mem},
};

// #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
// pub struct Type<'m>(pub &'m TypeKind<'m>);

pub type TypeRef<'m> = &'m Type<'m>;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Type<'m> {
    Constant(i32),
    Number,
    New(DefId, TypeRef<'m>),
    Function {
        params: &'m [TypeRef<'m>],
        output: TypeRef<'m>,
    },
    Error,
}

pub struct Types<'m> {
    mem: &'m Mem,
    pub(crate) types: HashSet<&'m Type<'m>>,
    pub(crate) slices: HashSet<&'m [TypeRef<'m>]>,
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

impl<'m> Intern<Type<'m>> for Types<'m> {
    type Facade = TypeRef<'m>;

    fn intern(&mut self, ty: Type<'m>) -> Self::Facade {
        match self.types.get(&ty) {
            Some(ty) => ty,
            None => {
                let type_ref = self.mem.bump.alloc(ty);
                self.types.insert(type_ref);
                type_ref
            }
        }
    }
}

impl<'m, const N: usize> Intern<[TypeRef<'m>; N]> for Types<'m> {
    type Facade = &'m [TypeRef<'m>];

    fn intern(&mut self, types: [TypeRef<'m>; N]) -> Self::Facade {
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
    use crate::env::{Env, Mem};

    fn type_ptr(ty: TypeRef) -> usize {
        ty as *const _ as usize
    }

    #[test]
    fn dedup_types() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);

        let c0 = env.types.intern(Type::Constant(42));
        let c1 = env.types.intern(Type::Constant(42));
        let c2 = env.types.intern(Type::Constant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }

    #[test]
    fn test_alloc_type() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);

        let constant = env.types.intern(Type::Constant(42));
        let ty = env.types.intern(Type::New(DefId(42), constant));
    }
}

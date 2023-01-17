use std::ops::Deref;

use crate::{
    def::DefId,
    env::{Env, Intern},
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

impl<'m> Intern<&str> for Env<'m> {
    type Facade = &'m str;

    fn intern(&self, str: &str) -> Self::Facade {
        let mut strings = self.strings.borrow_mut();
        match strings.get(str) {
            Some(str) => *str,
            None => {
                let str = self.bump().alloc_str(str);
                strings.insert(str);
                str
            }
        }
    }
}

impl<'m> Intern<TypeKind<'m>> for Env<'m> {
    type Facade = Type<'m>;

    fn intern(&self, kind: TypeKind<'m>) -> Self::Facade {
        let mut types = self.types.borrow_mut();
        match types.get(&kind) {
            Some(kind) => Type(kind),
            None => {
                let kind = self.bump().alloc(kind);
                types.insert(kind);
                Type(kind)
            }
        }
    }
}

impl<'m, const N: usize> Intern<[Type<'m>; N]> for Env<'m> {
    type Facade = &'m [Type<'m>];

    fn intern(&self, types: [Type<'m>; N]) -> Self::Facade {
        let mut slices = self.type_slices.borrow_mut();
        match slices.get(types.as_slice()) {
            Some(slice) => slice,
            None => {
                let slice = self.bump().alloc_slice_fill_iter(types.into_iter());
                slices.insert(slice);
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
        let env = Env::new(&mem);

        let c0 = env.intern(TypeKind::Constant(42));
        let c1 = env.intern(TypeKind::Constant(42));
        let c2 = env.intern(TypeKind::Constant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }

    #[test]
    fn test_alloc_type() {
        let mem = Mem::default();
        let env = Env::new(&mem);

        let ty = env.intern(TypeKind::New(DefId(42), env.intern(TypeKind::Constant(42))));
    }
}

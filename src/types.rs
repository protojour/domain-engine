use std::ops::Deref;

use crate::env::{Env, Intern};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Type<'m>(pub(crate) &'m TypeKind<'m>);

impl<'m> Type<'m> {
    fn kind(&self) -> &TypeKind<'m> {
        self.0
    }
}

#[derive(PartialEq, Eq, Hash)]
pub enum TypeKind<'m> {
    Constant(i32),
    New(&'m str, Type<'m>),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::Mem;

    fn type_ptr(ty: Type) -> usize {
        ty.kind() as *const _ as usize
    }

    #[test]
    fn dedup_types() {
        let mut mem = Mem::default();
        let env = Env::new(&mut mem);

        let c0 = env.intern(TypeKind::Constant(42));
        let c1 = env.intern(TypeKind::Constant(42));
        let c2 = env.intern(TypeKind::Constant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }

    #[test]
    fn test_alloc_type() {
        let mut mem = Mem::default();
        let env = Env::new(&mut mem);

        let ty = env.intern(TypeKind::New(
            env.intern("name"),
            env.intern(TypeKind::Constant(42)),
        ));
    }
}

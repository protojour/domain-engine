use std::ops::Deref;

use crate::env::Env;

#[derive(Clone, Copy)]
pub struct Type<'m>(pub(crate) &'m TypeKind<'m>);

impl<'m> Type<'m> {
    fn kind(&self) -> &TypeKind<'m> {
        self.0
    }
}

impl<'m> Deref for Type<'m> {
    type Target = TypeKind<'m>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'m> Env<'m> {
    fn name(&self, name: &str) -> &'m str {
        self.bump().alloc_str(name)
    }

    fn constant(&self, constant: i32) -> Type<'m> {
        Type(self.bump().alloc(TypeKind::Constant(constant)))
    }

    fn newtype(&self, name: &'m str, inner: Type<'m>) -> Type<'m> {
        Type(self.bump().alloc(TypeKind::New(name, inner)))
    }
}

pub enum TypeKind<'m> {
    Constant(i32),
    New(&'m str, Type<'m>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::Mem;

    #[test]
    fn test_alloc_type() {
        let mut mem = Mem::default();
        let env = Env::new(&mut mem);

        let ty = env.newtype(env.name("name"), env.constant(42));
    }
}

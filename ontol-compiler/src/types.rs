use std::collections::{HashMap, HashSet};

use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::{
    def::{DefKind, Defs},
    mem::{Intern, Mem},
    type_check::inference::TypeVar,
};

pub type TypeRef<'m> = &'m Type<'m>;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Type<'m> {
    // Don't know what to name this..
    // This is the "type" of an equivalence assertion.
    // It has no specific meaning.
    Tautology,
    Constant(i32),
    /// Any number
    Number,
    /// Any string
    String,
    /// A specific string
    StringConstant(DefId),
    // Maybe this is a macro instead of a function, because
    // it represents abstraction of syntax:
    Function {
        params: &'m [TypeRef<'m>],
        output: TypeRef<'m>,
    },
    // User-defined data type from a domain:
    Domain(DefId),
    Infer(TypeVar<'m>),
    Error,
}

#[derive(Debug)]
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
                let ty = self.mem.bump.alloc(ty);
                self.types.insert(ty);
                ty
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

#[derive(Default, Debug)]
pub struct DefTypes<'m> {
    pub map: HashMap<DefId, TypeRef<'m>>,
}

pub(crate) fn format_type(ty: TypeRef, defs: &Defs) -> String {
    match ty {
        Type::Tautology => format!("tautology"),
        Type::Constant(val) => format!("number({})", val),
        Type::Number => format!("number"),
        Type::String => format!("string"),
        Type::StringConstant(def_id) => {
            let Some(DefKind::StringLiteral(lit)) = defs.get_def_kind(*def_id) else {
                panic!();
            };

            format!("\"{lit}\"")
        }
        Type::Function { .. } => format!("function"),
        Type::Domain(_) => format!("domain(FIXME)"),
        Type::Infer(_) => format!("?infer"),
        Type::Error => format!("error!"),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::Compiler;

    fn type_ptr(ty: TypeRef) -> usize {
        ty as *const _ as usize
    }

    #[test]
    fn dedup_types() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem);

        let c0 = compiler.types.intern(Type::Constant(42));
        let c1 = compiler.types.intern(Type::Constant(42));
        let c2 = compiler.types.intern(Type::Constant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }
}

use std::{collections::HashSet, fmt::Display};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::DefId;

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
    IntConstant(i32),
    Unit(DefId),
    EmptySequence(DefId),
    /// Any integer
    Int(DefId),
    /// Any number
    Number(DefId),
    /// Any string
    String(DefId),
    /// A specific string
    StringConstant(DefId),
    Regex(DefId),
    Uuid(DefId),
    Array(TypeRef<'m>),
    Option(TypeRef<'m>),
    // Maybe this is a macro instead of a function, because
    // it represents abstraction of syntax:
    Function {
        params: &'m [TypeRef<'m>],
        output: TypeRef<'m>,
    },
    // User-defined data type from a domain:
    Domain(DefId),
    Anonymous(DefId),
    Package,
    BuiltinRelation,
    Infer(TypeVar<'m>),
    Error,
}

impl<'m> Type<'m> {
    pub fn get_single_def_id(&self) -> Option<DefId> {
        match self {
            Self::Tautology => None,
            Self::Unit(def_id) => Some(*def_id),
            Self::EmptySequence(def_id) => Some(*def_id),
            Self::IntConstant(_) => todo!(),
            Self::Int(def_id) => Some(*def_id),
            Self::Number(def_id) => Some(*def_id),
            Self::String(def_id) => Some(*def_id),
            Self::StringConstant(def_id) => Some(*def_id),
            Self::Regex(def_id) => Some(*def_id),
            Self::Uuid(def_id) => Some(*def_id),
            Self::Array(_) => None,
            Self::Option(_) => None,
            Self::Function { .. } => None,
            Self::Domain(def_id) => Some(*def_id),
            Self::Anonymous(def_id) => Some(*def_id),
            Self::Package => None,
            Self::BuiltinRelation => None,
            Self::Infer(_) => None,
            Self::Error => None,
        }
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous(_))
    }
}

#[derive(Debug)]
pub struct Types<'m> {
    mem: &'m Mem,
    pub(crate) types: FnvHashSet<&'m Type<'m>>,
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

impl<'m> Intern<Vec<TypeRef<'m>>> for Types<'m> {
    type Facade = &'m [TypeRef<'m>];

    fn intern(&mut self, types: Vec<TypeRef<'m>>) -> Self::Facade {
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
    pub map: FnvHashMap<DefId, TypeRef<'m>>,
}

pub struct FormatType<'m, 'c>(pub TypeRef<'m>, pub &'c Defs<'m>);

impl<'m, 'c> Display for FormatType<'m, 'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ty = self.0;
        let defs = self.1;

        match ty {
            Type::Tautology => write!(f, "tautology"),
            Type::IntConstant(val) => write!(f, "int({val})"),
            Type::Unit(_) => write!(f, "unit"),
            Type::EmptySequence(_) => write!(f, "[]"),
            Type::Int(_) => write!(f, "int"),
            Type::Number(_) => write!(f, "number"),
            Type::String(_) => write!(f, "string"),
            Type::StringConstant(def_id) => {
                let Some(DefKind::StringLiteral(lit)) = defs.get_def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "\"{lit}\"")
            }
            Type::Regex(def_id) => {
                let Some(DefKind::Regex(lit)) = defs.get_def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "/{lit}/")
            }
            Type::Uuid(_) => write!(f, "uuid"),
            Type::Array(ty) => {
                write!(f, "{}[]", FormatType(ty, defs))
            }
            Type::Option(ty) => {
                write!(f, "{}?", FormatType(ty, defs))
            }
            Type::Function { .. } => write!(f, "function"),
            Type::Domain(def_id) => {
                let ident = defs
                    .get_def_kind(*def_id)
                    .unwrap()
                    .opt_identifier()
                    .unwrap();
                write!(f, "{ident}")
            }
            Type::Anonymous(_) => {
                write!(f, "anonymous")
            }
            Type::Package => write!(f, "package"),
            Type::BuiltinRelation => write!(f, "relation"),
            Type::Infer(_) => write!(f, "?infer"),
            Type::Error => write!(f, "error!"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compiler::Compiler, Sources};

    fn type_ptr(ty: TypeRef) -> usize {
        ty as *const _ as usize
    }

    #[test]
    fn dedup_types() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Sources::default());

        let c0 = compiler.types.intern(Type::IntConstant(42));
        let c1 = compiler.types.intern(Type::IntConstant(42));
        let c2 = compiler.types.intern(Type::IntConstant(66));

        assert_eq!(type_ptr(c0), type_ptr(c1));
        assert_ne!(type_ptr(c1), type_ptr(c2));
    }
}

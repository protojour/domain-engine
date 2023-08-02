use std::{collections::HashSet, fmt::Display};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{string_types::StringLikeType, DefId};

use crate::{
    def::{DefKind, Defs},
    mem::{Intern, Mem},
    primitive::Primitives,
    type_check::inference::TypeVar,
};

pub type TypeRef<'m> = &'m Type<'m>;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Type<'m> {
    // Don't know what to name this..
    // This is the "type" of an equivalence assertion.
    // It has no specific meaning.
    Tautology,
    Unit(DefId),
    // The DefId encodes which variant this is, false, true or bool
    Bool(DefId),
    EmptySequence(DefId),
    /// Any number
    Number(DefId),
    /// Any integer
    Int(DefId),
    IntConstant(i64),
    /// Any string
    String(DefId),
    /// A specific string
    StringConstant(DefId),
    Regex(DefId),
    StringLike(DefId, StringLikeType),
    Seq(TypeRef<'m>, TypeRef<'m>),
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
    // A builtin function for generating values
    ValueGenerator(DefId),
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
            Self::Bool(def_id) => Some(*def_id),
            Self::EmptySequence(def_id) => Some(*def_id),
            Self::Number(def_id) => Some(*def_id),
            Self::Int(def_id) => Some(*def_id),
            Self::IntConstant(_) => todo!(),
            Self::String(def_id) => Some(*def_id),
            Self::StringConstant(def_id) => Some(*def_id),
            Self::Regex(def_id) => Some(*def_id),
            Self::StringLike(def_id, _) => Some(*def_id),
            Self::Seq(_, _) => None,
            Self::Option(ty) => ty.get_single_def_id(),
            Self::Function { .. } => None,
            Self::Domain(def_id) => Some(*def_id),
            Self::Anonymous(def_id) => Some(*def_id),
            Self::ValueGenerator(def_id) => Some(*def_id),
            Self::Package => None,
            Self::BuiltinRelation => None,
            Self::Infer(_) => None,
            Self::Error => None,
        }
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous(_))
    }

    pub fn is_domain_specific(&self) -> bool {
        matches!(self, Self::Domain(_) | Self::Anonymous(_))
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
    pub table: FnvHashMap<DefId, TypeRef<'m>>,
}

pub struct FormatType<'m, 'c>(pub TypeRef<'m>, pub &'c Defs<'m>, pub &'c Primitives);

impl<'m, 'c> Display for FormatType<'m, 'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(ty, defs, primitives) = self;

        match ty {
            Type::Tautology => write!(f, "tautology"),
            Type::Unit(_) => write!(f, "unit"),
            Type::Bool(def_id) => {
                if *def_id == primitives.true_value {
                    write!(f, "true")
                } else if *def_id == primitives.false_value {
                    write!(f, "false")
                } else {
                    write!(f, "bool")
                }
            }
            Type::EmptySequence(_) => write!(f, "[]"),
            Type::Number(_) => write!(f, "number"),
            Type::Int(_) => write!(f, "int"),
            Type::IntConstant(val) => write!(f, "int({val})"),
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
            Type::StringLike(_, StringLikeType::Uuid) => write!(f, "uuid"),
            Type::StringLike(_, StringLikeType::DateTime) => write!(f, "datetime"),
            Type::Seq(rel, val) => {
                write!(
                    f,
                    "[{}: {}]",
                    FormatType(rel, defs, primitives),
                    FormatType(val, defs, primitives)
                )
            }
            Type::Option(ty) => {
                write!(f, "{}?", FormatType(ty, defs, primitives))
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
            Type::ValueGenerator(_) => write!(f, "value_generator"),
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
    use crate::{Compiler, Sources};

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

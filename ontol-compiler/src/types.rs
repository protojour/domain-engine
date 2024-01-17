use std::{collections::HashSet, fmt::Display};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{text_like_types::TextLikeType, DefId};
use ordered_float::NotNan;

use crate::{
    def::{DefKind, Defs},
    mem::{Intern, Mem},
    primitive::{PrimitiveKind, Primitives},
    type_check::ena_inference::TypeVar,
};

pub type TypeRef<'m> = &'m Type<'m>;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Type<'m> {
    // Don't know what to name this..
    // This is the "type" of an equivalence assertion.
    // It has no specific meaning.
    Tautology,
    Primitive(PrimitiveKind, DefId),
    EmptySequence(DefId),
    IntConstant(i64),
    FloatConstant(NotNan<f64>),
    TextConstant(DefId),
    Regex(DefId),
    TextLike(DefId, TextLikeType),
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

pub static UNIT_TYPE: Type = Type::Primitive(PrimitiveKind::Unit, DefId::unit());
pub static ERROR_TYPE: Type = Type::Error;

impl<'m> Type<'m> {
    pub fn get_single_def_id(&self) -> Option<DefId> {
        match self {
            Self::Tautology => None,
            Self::Primitive(_, def_id) => Some(*def_id),
            Self::EmptySequence(def_id) => Some(*def_id),
            Self::IntConstant(_) | Self::FloatConstant(_) => todo!(),
            Self::TextConstant(def_id) => Some(*def_id),
            Self::Regex(def_id) => Some(*def_id),
            Self::TextLike(def_id, _) => Some(*def_id),
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
                let slice = self.mem.bump.alloc_slice_fill_iter(types);
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
                let slice = self.mem.bump.alloc_slice_fill_iter(types);
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
            Type::Primitive(kind, def_id) => {
                // write!(f, "{}", kind.ident())
                match (kind, self.1.def_kind(*def_id)) {
                    (_, DefKind::Primitive(_, Some(ident))) => write!(f, "{ident}"),
                    (_, DefKind::Primitive(primitive_kind, None)) => {
                        write!(f, "{primitive_kind:?}")
                    }
                    _ => unreachable!(),
                }
            }
            Type::EmptySequence(_) => write!(f, "[]"),
            Type::IntConstant(val) => write!(f, "int({val})"),
            Type::FloatConstant(val) => write!(f, "float({val})"),
            Type::TextConstant(def_id) => {
                let DefKind::TextLiteral(lit) = defs.def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "\"{lit}\"")
            }
            Type::Regex(def_id) => {
                let DefKind::Regex(lit) = defs.def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "/{lit}/")
            }
            Type::TextLike(_, TextLikeType::Uuid) => write!(f, "uuid"),
            Type::TextLike(_, TextLikeType::DateTime) => write!(f, "datetime"),
            Type::Seq(rel, val) => {
                write!(
                    f,
                    "{{{}: {}}}",
                    FormatType(rel, defs, primitives),
                    FormatType(val, defs, primitives)
                )
            }
            Type::Option(ty) => {
                write!(f, "{}?", FormatType(ty, defs, primitives))
            }
            Type::Function { .. } => write!(f, "function"),
            Type::Domain(def_id) => {
                let ident = defs.def_kind(*def_id).opt_identifier().unwrap();
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

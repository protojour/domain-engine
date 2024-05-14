use std::{collections::HashSet, fmt::Display};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    ontology::{map::Extern, ontol::TextLikeType},
    DefId,
};
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
    Extern(DefId),
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
            Self::Extern(def_id) => Some(*def_id),
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

#[derive(Default)]
pub struct DefTypes<'m> {
    pub table: FnvHashMap<DefId, TypeRef<'m>>,
    pub ontology_externs: FnvHashMap<DefId, Extern>,
}

pub struct FormatType<'m, 'c> {
    ty: TypeRef<'m>,
    defs: &'c Defs<'m>,
    primitives: &'c Primitives,
    root: bool,
}

impl<'m, 'c> FormatType<'m, 'c> {
    pub fn new(ty: TypeRef<'m>, defs: &'c Defs<'m>, primitives: &'c Primitives) -> Self {
        Self {
            ty,
            defs,
            primitives,
            root: true,
        }
    }

    fn child(&self, ty: TypeRef<'m>) -> Self {
        Self {
            ty,
            defs: self.defs,
            primitives: self.primitives,
            root: false,
        }
    }
}

impl<'m, 'c> Display for FormatType<'m, 'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tick = if self.root { "`" } else { "" };

        match self.ty {
            Type::Tautology => write!(f, "tautology"),
            Type::Primitive(kind, def_id) => {
                // write!(f, "{}", kind.ident())
                match (kind, self.defs.def_kind(*def_id)) {
                    (_, DefKind::Primitive(_, Some(ident))) => {
                        write!(f, "{tick}ontol.{ident}{tick}")
                    }
                    (_, DefKind::Primitive(primitive_kind, None)) => {
                        write!(f, "{primitive_kind:?}")
                    }
                    _ => unreachable!(),
                }
            }
            Type::EmptySequence(_) => write!(f, "{tick}[]{tick}"),
            Type::IntConstant(val) => write!(f, "{tick}ontol.int({val}){tick}"),
            Type::FloatConstant(val) => write!(f, "{tick}ontol.float({val}){tick}"),
            Type::TextConstant(def_id) => {
                let DefKind::TextLiteral(lit) = self.defs.def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "\"{lit}\"")
            }
            Type::Regex(def_id) => {
                let DefKind::Regex(lit) = self.defs.def_kind(*def_id) else {
                    panic!();
                };

                write!(f, "/{lit}/")
            }
            Type::TextLike(_, TextLikeType::Uuid) => write!(f, "{tick}ontol.uuid{tick}"),
            Type::TextLike(_, TextLikeType::DateTime) => write!(f, "{tick}ontol.datetime{tick}"),
            Type::Seq(rel, val) => {
                write!(f, "{{{}: {}}}", self.child(rel), self.child(val),)
            }
            Type::Option(ty) => {
                write!(f, "{tick}{}?{tick}", self.child(ty))
            }
            Type::Function { .. } => write!(f, "function"),
            Type::Domain(def_id) => {
                let ident = self.defs.def_kind(*def_id).opt_identifier().unwrap();
                write!(f, "{tick}{ident}{tick}")
            }
            Type::Anonymous(_) => {
                write!(f, "anonymous type")
            }
            Type::ValueGenerator(_) => write!(f, "value generator"),
            Type::Package => write!(f, "package"),
            Type::BuiltinRelation => write!(f, "relation"),
            Type::Extern(def_id) => {
                let ident = self.defs.def_kind(*def_id).opt_identifier().unwrap();
                write!(f, "extern({ident})")
            }
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

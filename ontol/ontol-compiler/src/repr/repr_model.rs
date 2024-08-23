use std::ops::RangeInclusive;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{DefId, PropId};
use ordered_float::NotNan;
use smallvec::SmallVec;

use crate::{misc::TypeParam, primitive::Primitives, SourceSpan};

#[derive(Debug)]
pub struct Repr {
    pub kind: ReprKind,

    /// Params at type level (i.e. no string/member relations).
    /// These are collected from all supertypes.
    pub type_params: FnvHashMap<DefId, TypeParam>,
}

#[derive(Eq, PartialEq, Debug)]
pub enum ReprKind {
    Unit,
    /// Scalar type without further attributes.
    /// Currently placeholder for boolean, unit, text
    Scalar(DefId, ReprScalarKind, SourceSpan),
    /// struct variant, from fmt constructors with 1 allowed scalar property (for now).
    /// Possibly generalize this later
    FmtStruct(Option<(PropId, DefId)>),
    /// Sequence
    Seq,
    /// Just a plain old struct, no `is` business
    Struct,
    /// Struct merged with other Structs
    StructIntersection(SmallVec<(DefId, SourceSpan), 1>),
    // Intersection of non-structs
    Intersection(Vec<(DefId, SourceSpan)>),
    // StructUnion(Vec<(DefId, SourceSpan)>),
    Union(Vec<(DefId, SourceSpan)>, UnionBound),
    Extern,
    Macro,
}

#[derive(Eq, PartialEq, Debug)]
pub enum UnionBound {
    /// Any kind of union, no recognized uniformity
    Any,
    /// Scalar union
    Scalar(ReprScalarKind),
    /// Every member of the union is a struct
    Struct,
    /// Every member of the union is an FmtStruct
    Fmt,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ReprScalarKind {
    I64(RangeInclusive<i64>),
    F64(RangeInclusive<NotNan<f64>>),
    Serial,
    Boolean,
    Text,
    TextConstant(DefId),
    Octets,
    DateTime,
    Other,
}

impl ReprScalarKind {
    pub fn combine(&self, other: &ReprScalarKind) -> Self {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) => {
                Self::I64(*(a.start().min(b.start()))..=*(a.end().max(b.end())))
            }
            (Self::F64(a), Self::F64(b)) => {
                Self::F64(*(a.start().min(b.start()))..=*(a.end().max(b.end())))
            }
            (Self::Serial, Self::Serial) => Self::Serial,
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            (Self::Text, Self::Text) => Self::Text,
            (Self::Text, Self::TextConstant(_)) => Self::Text,
            (Self::TextConstant(_), Self::Text) => Self::Text,
            (Self::TextConstant(_), Self::TextConstant(_)) => Self::Text,
            (Self::Octets, Self::Octets) => Self::Octets,
            (Self::DateTime, Self::DateTime) => Self::DateTime,
            _ => Self::Other,
        }
    }

    /// saturate this scalar so it doesn't turn out to be a Unit
    pub fn saturate(&self) -> Self {
        match self {
            Self::TextConstant(_) => Self::Text,
            _ => self.clone(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub enum NumberResolution {
    Integer,
    /// Note: Float is an abstract number resolution, f32 and f64 are its sub resolutions:
    Float,
    F32,
    F64,
}

impl NumberResolution {
    pub fn def_id(&self, primitives: &Primitives) -> DefId {
        match self {
            Self::Integer => primitives.integer,
            Self::Float => primitives.float,
            Self::F32 => todo!(),
            Self::F64 => primitives.f64,
        }
    }

    pub fn is_sub_resolution_of(&self, other: Self) -> bool {
        matches!((self, other), (Self::F32 | Self::F64, Self::Float))
    }
}

pub(super) struct ReprBuilder {
    pub kind: Option<ReprKind>,

    /// Typed relation parameters
    pub type_params: FnvHashMap<DefId, TypeParam>,

    pub number_resolutions: IndexMap<NumberResolution, SourceSpan>,
}

use std::ops::Range;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::DefId;
use ordered_float::NotNan;
use smallvec::SmallVec;

use crate::{primitive::Primitives, relation::TypeParam, SourceSpan};

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
    /// Currently placeholder for bool, unit, string
    Scalar(DefId, SourceSpan),
    I64(DefId, Range<i64>, SourceSpan),
    F64(DefId, Range<NotNan<f64>>, SourceSpan),
    /// Sequence
    Seq,
    /// Just a plain old struct, no `is` business
    Struct,
    /// Struct merged with other Structs
    StructIntersection(SmallVec<[(DefId, SourceSpan); 1]>),
    // Intersection of non-structs
    Intersection(Vec<(DefId, SourceSpan)>),
    StructUnion(Vec<(DefId, SourceSpan)>),
    Union(Vec<(DefId, SourceSpan)>),
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
            Self::Integer => primitives.int,
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

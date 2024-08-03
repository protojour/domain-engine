use std::ops::RangeInclusive;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{ontology::domain::DefRepr, DefId};
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
    /// Sequence
    Seq,
    /// Just a plain old struct, no `is` business
    Struct,
    /// Struct merged with other Structs
    StructIntersection(SmallVec<(DefId, SourceSpan), 1>),
    // Intersection of non-structs
    Intersection(Vec<(DefId, SourceSpan)>),
    StructUnion(Vec<(DefId, SourceSpan)>),
    Union(Vec<(DefId, SourceSpan)>),
    Extern,
}

impl ReprKind {
    pub fn to_def_repr(&self) -> DefRepr {
        match self {
            ReprKind::Unit => DefRepr::Unit,
            ReprKind::Scalar(_, ReprScalarKind::I64(_), _) => DefRepr::I64,
            ReprKind::Scalar(_, ReprScalarKind::F64(_), _) => DefRepr::F64,
            ReprKind::Scalar(_, ReprScalarKind::Serial, _) => DefRepr::Serial,
            ReprKind::Scalar(_, ReprScalarKind::Boolean, _) => DefRepr::Boolean,
            ReprKind::Scalar(_, ReprScalarKind::Text, _) => DefRepr::Text,
            ReprKind::Scalar(_, ReprScalarKind::TextConstant(_), _) => DefRepr::Unit,
            ReprKind::Scalar(_, ReprScalarKind::Octets, _) => DefRepr::Octets,
            ReprKind::Scalar(_, ReprScalarKind::DateTime, _) => DefRepr::DateTime,
            ReprKind::Scalar(_, ReprScalarKind::Other, _) => DefRepr::Unknown,
            ReprKind::Seq => DefRepr::Seq,
            ReprKind::Struct => DefRepr::Struct,
            ReprKind::StructIntersection(_) => DefRepr::Struct,
            ReprKind::Intersection(defs) => {
                DefRepr::Intersection(defs.iter().map(|(def_id, _)| *def_id).collect())
            }
            ReprKind::StructUnion(defs) => {
                DefRepr::StructUnion(defs.iter().map(|(def_id, _)| *def_id).collect())
            }
            ReprKind::Union(defs) => {
                DefRepr::Union(defs.iter().map(|(def_id, _)| *def_id).collect())
            }
            ReprKind::Extern => DefRepr::Unknown,
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
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

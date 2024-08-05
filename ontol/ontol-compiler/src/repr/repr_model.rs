use std::ops::RangeInclusive;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    ontology::domain::{DefRepr, DefReprUnionBound},
    DefId, RelId,
};
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
    FmtStruct(Option<(RelId, DefId)>),
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
}

#[derive(Eq, PartialEq, Debug)]
pub enum UnionBound {
    /// Any kind of union, no recognized uniformity
    Any,
    /// Every member of the union is a struct
    Struct,
    /// Every member of the union is an FmtStruct
    Fmt,
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
            ReprKind::FmtStruct(opt_attr) => DefRepr::FmtStruct(*opt_attr),
            ReprKind::Seq => DefRepr::Seq,
            ReprKind::Struct => DefRepr::Struct,
            ReprKind::StructIntersection(_) => DefRepr::Struct,
            ReprKind::Intersection(defs) => {
                DefRepr::Intersection(defs.iter().map(|(def_id, _)| *def_id).collect())
            }
            ReprKind::Union(defs, bound) => DefRepr::Union(
                defs.iter().map(|(def_id, _)| *def_id).collect(),
                match bound {
                    UnionBound::Any => DefReprUnionBound::Any,
                    UnionBound::Struct => DefReprUnionBound::Struct,
                    UnionBound::Fmt => DefReprUnionBound::Fmt,
                },
            ),
            ReprKind::Extern => DefRepr::Unknown,
        }
    }
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

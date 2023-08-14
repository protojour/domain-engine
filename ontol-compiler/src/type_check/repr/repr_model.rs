use fnv::FnvHashMap;
use ontol_runtime::DefId;
use smallvec::SmallVec;

use crate::SourceSpan;

#[derive(Eq, PartialEq, Debug)]
pub struct Repr {
    pub kind: ReprKind,

    /// Params at type level (i.e. no string/member relations).
    /// These are collected from all supertypes.
    pub type_params: FnvHashMap<DefId, DefId>,
}

#[derive(Eq, PartialEq, Debug)]
pub enum ReprKind {
    Unit,
    /// Scalar type without further attributes
    Scalar(DefId, SourceSpan),
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

use ontol_runtime::{string_types::StringLikeType, DefId};
use smallvec::SmallVec;

use crate::SourceSpan;

#[derive(Eq, PartialEq, Debug)]
pub enum ReprKind {
    I64,
    Bool,
    String,
    StringLike(DefId, StringLikeType),
    EmptyDict,
    Seq,
    StructIntersection(SmallVec<[(DefId, SourceSpan); 1]>),
    Intersection(Vec<(DefId, SourceSpan)>),
    Union(Vec<(DefId, SourceSpan)>),
}

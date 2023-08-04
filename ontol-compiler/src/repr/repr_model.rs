use fnv::FnvHashMap;
use ontol_runtime::DefId;

use crate::SourceSpan;

#[derive(Default, Debug)]
pub struct ReprCtx {
    pub table: FnvHashMap<DefId, ReprKind>,
}

#[derive(Eq, PartialEq, Debug)]
pub enum ReprKind {
    I64,
    Bool,
    String,
    Struct,
    EmptyDict,
    Seq,
    Intersection(Vec<(DefId, SourceSpan)>),
    Union(Vec<(DefId, SourceSpan)>),
}

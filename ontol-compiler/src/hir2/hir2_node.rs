#![allow(unused)]

use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use smallvec::SmallVec;

use crate::{
    hir_node::{HirIdx, HirVariable},
    types::TypeRef,
    SourceSpan,
};

#[derive(Clone, Debug)]
pub struct Hir2Node<'m> {
    pub kind: Hir2Kind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug)]
pub enum Hir2Kind<'m> {
    VariableRef(HirIdx),
    Constant(i64),
    Unit,
    Call(BuiltinProc, SmallVec<[HirIdx; 2]>),
    MapCall(HirIdx, TypeRef<'m>),
    Construct(IndexMap<PropertyId, HirIdx>),
    Destruct(HirIdx, IndexMap<PropertyId, HirIdx>),
    MatchProp(Vec<HirPropMatchArm>),
    ConstructProp(HirIdx, HirIdx),
}

#[derive(Clone, Debug)]
pub struct HirPropMatchArm {
    pub pattern: HirPropPattern,
    pub node: HirIdx,
}

#[derive(Clone, Debug)]
pub enum HirPropPattern {
    Present(HirPattern, HirPattern),
    NotPresent,
}

#[derive(Clone, Debug)]
pub enum HirPattern {
    Wildcard,
    Binder(HirVariable),
}

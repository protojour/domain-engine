#![allow(unused)]

use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use smallvec::SmallVec;

use crate::{
    hir_node::{HirIdx, HirVariable},
    types::{Type, TypeRef},
    SourceSpan,
};

#[derive(Clone, Debug)]
pub struct Hir2Node<'m> {
    pub kind: Hir2Kind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

impl From<Hir2Kind<'static>> for Hir2Node<'static> {
    fn from(value: Hir2Kind<'static>) -> Self {
        Self {
            kind: value,
            ty: &Type::Tautology,
            span: SourceSpan::none(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Hir2Kind<'m> {
    Variable(u32),
    VariableRef(HirIdx),
    Constant(i64),
    Unit,
    Call(BuiltinProc, SmallVec<[HirIdx; 2]>),
    MapCall(HirIdx, TypeRef<'m>),
    Construct(Vec<HirIdx>),
    ConstructProp(PropertyId, HirIdx, HirIdx),
    Destruct(HirIdx, Vec<HirIdx>),
    DestructProp(Vec<HirPropMatchArm>),
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

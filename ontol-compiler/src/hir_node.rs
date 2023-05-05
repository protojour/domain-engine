//! # Typed expressions
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{
    fmt::Debug,
    ops::{Index, IndexMut},
};

use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use smallvec::SmallVec;

use crate::{types::TypeRef, SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct BindDepth(pub u16);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HirVariable(pub u16);

impl Debug for HirVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let var = &self.0;
        write!(f, "HirVariable({var})")
    }
}

/// A "code" node with complete type information attached.
/// Hir means High-level Intermediate Representation.
#[derive(Clone, Debug)]
pub struct HirNode<'m> {
    pub kind: HirKind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

/// The different kinds of nodes.
#[derive(Clone, Debug)]
pub enum HirKind<'m> {
    /// An expression with no information
    Unit,
    /// Call to a built-in procedure
    Call(BuiltinProc, SmallVec<[HirIdx; 2]>),
    /// A value pattern ("object" with one anonymous property/attribute)
    ValuePattern(HirIdx),
    /// A struct pattern, containing destructuring of properties
    StructPattern(IndexMap<PropertyId, HirIdx>),
    /// A variable definition
    Variable(HirVariable),
    /// A variable reference (usage site)
    VariableRef(HirIdx),
    /// A constant/literal expression
    Constant(i64),
    /// A mapping from one type to another.
    /// Normally translates into a procedure call.
    MapCall(HirIdx, TypeRef<'m>),
    Aggr(HirIdx, HirBodyIdx),
}

/// An index/reference to a typed HirNode.
///
/// This reference is tied to a `HirNodeTable` and is not globally valid.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub struct HirIdx(pub u32);

pub const ERROR_NODE: HirIdx = HirIdx(u32::MAX);

/// A reference to an aggregation body
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct HirBodyIdx(pub u32);

impl Debug for HirBodyIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HirBodyIdx({})", self.0)
    }
}

#[derive(Debug)]
pub struct HirBody {
    pub first: HirIdx,
    pub second: HirIdx,
}

impl HirBody {
    pub fn order(&self, direction: CodeDirection) -> (HirIdx, HirIdx) {
        match direction {
            CodeDirection::Forward => (self.first, self.second),
            CodeDirection::Backward => (self.second, self.first),
        }
    }

    pub fn bindings_node(&self, direction: CodeDirection) -> HirIdx {
        match direction {
            CodeDirection::Forward => self.first,
            CodeDirection::Backward => self.second,
        }
    }
}

impl Default for HirBody {
    fn default() -> Self {
        Self {
            first: ERROR_NODE,
            second: ERROR_NODE,
        }
    }
}

#[derive(Clone, Copy)]
pub enum CodeDirection {
    Forward,
    Backward,
}

#[derive(Default, Debug)]
pub struct HirNodeTable<'m>(pub(super) Vec<HirNode<'m>>);

impl<'m> HirNodeTable<'m> {
    pub fn add(&mut self, expr: HirNode<'m>) -> HirIdx {
        let id = HirIdx(self.0.len() as u32);
        self.0.push(expr);
        id
    }
}

impl<'m> Index<HirIdx> for HirNodeTable<'m> {
    type Output = HirNode<'m>;

    fn index(&self, index: HirIdx) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

impl<'m> IndexMut<HirIdx> for HirNodeTable<'m> {
    fn index_mut(&mut self, index: HirIdx) -> &mut Self::Output {
        &mut self.0[index.0 as usize]
    }
}

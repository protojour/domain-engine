//! # Typed expressions
//!
//! TODO: This module is a remnant of the old code generator and should be removed
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{
    fmt::Debug,
    ops::{Index, IndexMut},
};

use smallvec::SmallVec;

use crate::{types::TypeRef, SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct BindDepth(pub u16);

/// A "code" node with complete type information attached.
/// Hir means High-level Intermediate Representation.
#[derive(Clone, Debug)]
pub struct HirNode<'m> {
    pub kind: HirKind,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

/// The different kinds of nodes.
#[derive(Clone, Debug)]
pub enum HirKind {
    /// A variable definition
    Variable(ontos::Variable),
    /// Match a value
    #[allow(unused)]
    Match(HirIdx, HirMatchTable),
}

#[derive(Clone, Debug)]
pub struct HirMatchTable {
    pub table: SmallVec<[(HirPredicate, HirBodyIdx); 2]>,
}

#[derive(Clone, Debug)]
#[allow(unused)]
pub enum HirPredicate {
    True,
    IsNotUnit,
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

impl Default for HirBody {
    fn default() -> Self {
        Self {
            first: ERROR_NODE,
            second: ERROR_NODE,
        }
    }
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

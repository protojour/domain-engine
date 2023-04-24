//! # Typed expressions
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{fmt::Debug, ops::Index};

use indexmap::IndexMap;
use ontol_runtime::{proc::BuiltinProc, value::PropertyId};
use smallvec::SmallVec;

use crate::{types::TypeRef, SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct BindDepth(pub u16);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SyntaxVar(pub u16, pub BindDepth);

impl Debug for SyntaxVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let var = &self.0;
        let depth = &self.1 .0;
        write!(f, "SyntaxVar({var} d={depth})")
    }
}

/// An "code" node with complete type information attached.
/// Ir means Intermediate Representation.
#[derive(Clone, Debug)]
pub struct IrNode<'m> {
    pub kind: IrKind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

/// The 'kind' of a typed expression
#[derive(Clone, Debug)]
pub enum IrKind<'m> {
    /// An expression with no information
    Unit,
    /// Call to a built-in procedure
    Call(BuiltinProc, SmallVec<[IrNodeId; 2]>),
    /// A value pattern ("object" with one anonymous property/attribute)
    ValuePattern(IrNodeId),
    /// A struct pattern, containing destructuring of properties
    StructPattern(IndexMap<PropertyId, IrNodeId>),
    /// A variable definition
    Variable(SyntaxVar),
    /// A variable reference (usage site)
    VariableRef(IrNodeId),
    /// A constant/literal expression
    Constant(i64),
    /// A mapping from one type to another.
    /// Normally translates into a procedure call.
    MapCall(IrNodeId, TypeRef<'m>),
    /// A mapping operation on a sequence.
    /// The first expression is the array.
    /// The syntax var is the iterated value.
    /// The second expression is the item/body.
    MapSequenceBalanced(IrNodeId, SyntaxVar, IrNodeId, TypeRef<'m>),
    MapSequence(IrNodeId, SyntaxVar, IrNodeId, TypeRef<'m>),
}

/// A reference to a typed expression.
///
/// This reference is tied to a `TypedExprTable` and is not globally valid.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub struct IrNodeId(pub u32);

pub const ERROR_NODE: IrNodeId = IrNodeId(u32::MAX);

#[derive(Default, Debug)]
pub struct IrNodeTable<'m>(pub(super) Vec<IrNode<'m>>);

impl<'m> IrNodeTable<'m> {
    pub fn add(&mut self, expr: IrNode<'m>) -> IrNodeId {
        let id = IrNodeId(self.0.len() as u32);
        self.0.push(expr);
        id
    }
}

impl<'m> Index<IrNodeId> for IrNodeTable<'m> {
    type Output = IrNode<'m>;

    fn index(&self, index: IrNodeId) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

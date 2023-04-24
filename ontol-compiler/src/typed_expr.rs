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

/// An expression with type information attached
#[derive(Clone, Debug)]
pub struct TypedExpr<'m> {
    pub kind: TypedExprKind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

/// The 'kind' of a typed expression
#[derive(Clone, Debug)]
pub enum TypedExprKind<'m> {
    /// An expression with no information
    Unit,
    /// Call to a built-in procedure
    Call(BuiltinProc, SmallVec<[ExprRef; 2]>),
    /// A value pattern ("object" with one anonymous property/attribute)
    ValuePattern(ExprRef),
    /// A struct pattern, containing destructuring of properties
    StructPattern(IndexMap<PropertyId, ExprRef>),
    /// A variable definition
    Variable(SyntaxVar),
    /// A variable reference (usage site)
    VariableRef(ExprRef),
    /// A constant/literal expression
    Constant(i64),
    /// A mapping from one type to another.
    /// Normally translates into a procedure call.
    MapCall(ExprRef, TypeRef<'m>),
    /// A mapping operation on a sequence.
    /// The first expression is the array.
    /// The syntax var is the iterated value.
    /// The second expression is the item/body.
    MapSequenceBalanced(ExprRef, SyntaxVar, ExprRef, TypeRef<'m>),
    MapSequence(ExprRef, SyntaxVar, ExprRef, TypeRef<'m>),
}

/// A reference to a typed expression.
///
/// This reference is tied to a `TypedExprTable` and is not globally valid.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub struct ExprRef(pub u32);

pub const ERROR_NODE: ExprRef = ExprRef(u32::MAX);

#[derive(Default, Debug)]
pub struct TypedExprTable<'m>(pub(super) Vec<TypedExpr<'m>>);

impl<'m> TypedExprTable<'m> {
    pub fn add(&mut self, expr: TypedExpr<'m>) -> ExprRef {
        let id = ExprRef(self.0.len() as u32);
        self.0.push(expr);
        id
    }
}

impl<'m> Index<ExprRef> for TypedExprTable<'m> {
    type Output = TypedExpr<'m>;

    fn index(&self, index: ExprRef) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

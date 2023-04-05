//! # Typed expressions
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{fmt::Debug, ops::Index};

use indexmap::IndexMap;
use ontol_runtime::{proc::BuiltinProc, value::PropertyId};
use smallvec::SmallVec;

use crate::{
    codegen::equation_solver::{EquationSolver, SubstitutionTable},
    types::TypeRef,
    SourceSpan,
};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SyntaxVar(pub u32);

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
    /// A value object (object with one anonymous property/attribute)
    ValueObjPattern(ExprRef),
    /// A map object pattern
    MapObjPattern(IndexMap<PropertyId, ExprRef>),
    /// A variable definition
    Variable(SyntaxVar),
    /// A variable reference (usage site)
    VariableRef(ExprRef),
    /// A constant/literal expression
    Constant(i64),
    /// A translation from one type to another
    Translate(ExprRef, TypeRef<'m>),
    /// A mapping operation on an array
    SequenceMap(ExprRef, TypeRef<'m>),
}

impl<'m> Debug for TypedExprEquation<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedExprTable").finish()
    }
}

/// A reference to a typed expression.
///
/// This reference is tied to a `TypedExprTable` and is not globally valid.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct ExprRef(pub u32);

pub const ERROR_NODE: ExprRef = ExprRef(u32::MAX);

#[derive(Default)]
pub struct TypedExprVec<'m>(pub(super) Vec<TypedExpr<'m>>);

impl<'m> Index<ExprRef> for TypedExprVec<'m> {
    type Output = TypedExpr<'m>;

    fn index(&self, index: ExprRef) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

/// Table used to store equation-like expression trees
/// suitable for rewriting.
#[derive(Default)]
pub struct TypedExprEquation<'m> {
    /// The set of expressions part of the equation
    pub expr_vec: TypedExprVec<'m>,
    /// Rewrites for the reduced side
    pub reductions: SubstitutionTable,
    /// Rewrites for the expanded side
    pub expansions: SubstitutionTable,
}

impl<'m> TypedExprEquation<'m> {
    pub fn add_expr(&mut self, expr: TypedExpr<'m>) -> ExprRef {
        let id = ExprRef(self.expr_vec.0.len() as u32);
        self.expr_vec.0.push(expr);
        self.reductions.push();
        self.expansions.push();

        id
    }

    /// Mark the table as "sealed".
    /// A sealed table can be reset to its original state.
    pub fn seal(self) -> SealedTypedExprEquation<'m> {
        let size = self.expr_vec.0.len();
        let mut sealed = SealedTypedExprEquation { size, inner: self };
        sealed.reset();
        sealed
    }

    /// Create a new solver
    pub fn solver<'e>(&'e mut self) -> EquationSolver<'e, 'm> {
        EquationSolver::new(self)
    }

    #[inline]
    pub fn resolve_expr<'e>(
        &'e self,
        substitutions: &SubstitutionTable,
        source_node: ExprRef,
    ) -> (ExprRef, &'e TypedExpr<'m>, SourceSpan) {
        let span = self.expr_vec[source_node].span;
        let root_node = substitutions.resolve(source_node);
        (root_node, &self.expr_vec[root_node], span)
    }

    pub fn debug_tree<'e>(
        &'e self,
        expr_ref: ExprRef,
        substitutions: &'e SubstitutionTable,
    ) -> DebugTree<'e, 'm> {
        DebugTree {
            equation: self,
            substitutions,
            expr_ref,
            property_id: None,
            depth: 0,
        }
    }
}

#[derive(Debug)]
pub struct SealedTypedExprEquation<'m> {
    size: usize,
    pub inner: TypedExprEquation<'m>,
}

impl<'m> SealedTypedExprEquation<'m> {
    /// Reset all rewrites, which backtracks to original state
    pub fn reset(&mut self) {
        self.inner.expr_vec.0.truncate(self.size);
        self.inner.reductions.reset(self.size);
        self.inner.expansions.reset(self.size);

        // auto rewrites of variable refs
        for (index, expr) in self.inner.expr_vec.0.iter().enumerate() {
            if let TypedExprKind::VariableRef(var_ref) = &expr.kind {
                self.inner.reductions[ExprRef(index as u32)] = *var_ref;
                self.inner.expansions[ExprRef(index as u32)] = *var_ref;
            }
        }
    }
}

pub struct DebugTree<'a, 'm> {
    equation: &'a TypedExprEquation<'m>,
    substitutions: &'a SubstitutionTable,
    expr_ref: ExprRef,
    property_id: Option<PropertyId>,
    depth: usize,
}

impl<'a, 'm> DebugTree<'a, 'm> {
    fn child(&self, expr_ref: ExprRef, property_id: Option<PropertyId>) -> Self {
        Self {
            equation: self.equation,
            substitutions: self.substitutions,
            expr_ref,
            property_id,
            depth: self.depth + 1,
        }
    }

    fn header(&self, name: &str, resolved: ExprRef) -> String {
        use std::fmt::Write;
        let mut s = String::new();

        if self.expr_ref != resolved {
            write!(&mut s, "{{{}->{}}}", self.expr_ref.0, resolved.0).unwrap();
        } else {
            write!(&mut s, "{{{}}}", self.expr_ref.0).unwrap();
        }

        if let Some(property_id) = &self.property_id {
            let def_id = &property_id.relation_id.0;
            write!(&mut s, " (rel {}, {}) ", def_id.0 .0, def_id.1).unwrap();
        }

        write!(&mut s, "{}", name).unwrap();

        s
    }
}

impl<'a, 'm> Debug for DebugTree<'a, 'm> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.depth > 20 {
            return write!(f, "[ERROR depth exceeded]");
        }

        let (resolved, expr, _) = self
            .equation
            .resolve_expr(self.substitutions, self.expr_ref);

        match &expr.kind {
            TypedExprKind::Unit => f.debug_tuple(&self.header("Unit", resolved)).finish()?,
            TypedExprKind::Call(proc, params) => {
                let mut tup = f.debug_tuple(&self.header(&format!("{proc:?}"), resolved));
                for param in params {
                    tup.field(&self.child(*param, None));
                }
                tup.finish()?
            }
            TypedExprKind::ValueObjPattern(expr_ref) => f
                .debug_tuple(&self.header("ValueObj", resolved))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::MapObjPattern(attributes) => {
                let mut tup = f.debug_tuple(&self.header("MapObj", resolved));
                for (property_id, expr_ref) in attributes {
                    tup.field(&self.child(*expr_ref, Some(*property_id)));
                }
                tup.finish()?;
            }
            TypedExprKind::Constant(c) => f
                .debug_tuple(&self.header(&format!("Constant({c})"), resolved))
                .finish()?,
            TypedExprKind::Variable(SyntaxVar(v)) => f
                .debug_tuple(&self.header(&format!("Variable({v})"), resolved))
                .finish()?,
            TypedExprKind::VariableRef(var_ref) => f
                .debug_tuple(&self.header("VarRef", resolved))
                .field(&self.child(*var_ref, None))
                .finish()?,
            TypedExprKind::Translate(expr_ref, _) => f
                .debug_tuple(&self.header("Translate", resolved))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::SequenceMap(expr_ref, _) => f
                .debug_tuple(&self.header("SequenceMap", resolved))
                .field(&self.child(*expr_ref, None))
                .finish()?,
        };

        Ok(())
    }
}

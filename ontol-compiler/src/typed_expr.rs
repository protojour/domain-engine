//! # Typed expressions
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{fmt::Debug, ops::Index};

use indexmap::IndexMap;
use ontol_runtime::{proc::BuiltinProc, value::PropertyId};
use smallvec::SmallVec;

use crate::{
    codegen::rewrite::{RewriteTable, Rewriter},
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

impl<'m> Debug for TypedExprTable<'m> {
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
pub struct TypedExpressions<'m>(pub(super) Vec<TypedExpr<'m>>);

impl<'m> Index<ExprRef> for TypedExpressions<'m> {
    type Output = TypedExpr<'m>;

    fn index(&self, index: ExprRef) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

/// Table used to store expression trees
/// suitable for equation-style rewriting.
#[derive(Default)]
pub struct TypedExprTable<'m> {
    pub expressions: TypedExpressions<'m>,
    pub source_rewrites: RewriteTable,
    pub target_rewrites: RewriteTable,
}

impl<'m> TypedExprTable<'m> {
    pub fn add_expr(&mut self, expr: TypedExpr<'m>) -> ExprRef {
        let auto_rewrite = match &expr.kind {
            TypedExprKind::VariableRef(var_ref) => Some(*var_ref),
            _ => None,
        };

        let id = ExprRef(self.expressions.0.len() as u32);
        self.expressions.0.push(expr);
        self.source_rewrites.push();
        self.target_rewrites.push();

        if let Some(auto_rewrite) = auto_rewrite {
            self.source_rewrites.rewrite(id, auto_rewrite);
            self.target_rewrites.rewrite(id, auto_rewrite);
        }

        id
    }

    /// Mark the table as "sealed".
    /// A sealed table can be reset to its original state.
    pub fn seal(self) -> SealedTypedExprTable<'m> {
        let size = self.expressions.0.len();
        SealedTypedExprTable { size, inner: self }
    }

    /// Create a new rewriter
    pub fn rewriter<'c>(&'c mut self) -> Rewriter<'c, 'm> {
        Rewriter::new(self)
    }

    #[inline]
    pub fn resolve_expr<'t>(
        &'t self,
        rewrite_table: &RewriteTable,
        source_node: ExprRef,
    ) -> (ExprRef, &'t TypedExpr<'m>, SourceSpan) {
        let span = self.expressions[source_node].span;
        let root_node = rewrite_table.resolve(source_node);
        (root_node, &self.expressions[root_node], span)
    }

    pub fn debug<'a>(
        &'a self,
        rewrites: &'a RewriteTable,
        root_expr: ExprRef,
    ) -> DebugTree<'a, 'm> {
        DebugTree {
            table: self,
            rewrites,
            expr_ref: root_expr,
            property_id: None,
            depth: 0,
        }
    }
}

#[derive(Debug)]
pub struct SealedTypedExprTable<'m> {
    size: usize,
    pub inner: TypedExprTable<'m>,
}

impl<'m> SealedTypedExprTable<'m> {
    /// Reset all rewrites, which backtracks to original state
    pub fn reset(&mut self) {
        self.inner.expressions.0.truncate(self.size);
        self.inner.source_rewrites.reset(self.size);
        self.inner.target_rewrites.reset(self.size);

        // auto rewrites of variable refs
        for (index, expr) in self.inner.expressions.0.iter().enumerate() {
            if let TypedExprKind::VariableRef(var_ref) = &expr.kind {
                self.inner
                    .source_rewrites
                    .rewrite(ExprRef(index as u32), *var_ref);
                self.inner
                    .target_rewrites
                    .rewrite(ExprRef(index as u32), *var_ref);
            }
        }
    }
}

pub struct DebugTree<'a, 'm> {
    table: &'a TypedExprTable<'m>,
    rewrites: &'a RewriteTable,
    expr_ref: ExprRef,
    property_id: Option<PropertyId>,
    depth: usize,
}

impl<'a, 'm> DebugTree<'a, 'm> {
    fn child(&self, expr_ref: ExprRef, property_id: Option<PropertyId>) -> Self {
        Self {
            table: self.table,
            rewrites: self.rewrites,
            expr_ref,
            property_id,
            depth: self.depth + 1,
        }
    }

    fn header(&self, name: &str, target_expr_ref: ExprRef) -> String {
        use std::fmt::Write;
        let mut s = String::new();

        if self.expr_ref != target_expr_ref {
            write!(&mut s, "{{{}->{}}}", self.expr_ref.0, target_expr_ref.0).unwrap();
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

        let (target_expr_ref, expr, _) = self.table.resolve_expr(self.rewrites, self.expr_ref);

        match &expr.kind {
            TypedExprKind::Unit => f
                .debug_tuple(&self.header("Unit", target_expr_ref))
                .finish()?,
            TypedExprKind::Call(proc, params) => {
                let mut tup = f.debug_tuple(&self.header(&format!("{proc:?}"), target_expr_ref));
                for param in params {
                    tup.field(&self.child(*param, None));
                }
                tup.finish()?
            }
            TypedExprKind::ValueObjPattern(expr_ref) => f
                .debug_tuple(&self.header("ValueObj", target_expr_ref))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::MapObjPattern(attributes) => {
                let mut tup = f.debug_tuple(&self.header("MapObj", target_expr_ref));
                for (property_id, expr_ref) in attributes {
                    tup.field(&self.child(*expr_ref, Some(*property_id)));
                }
                tup.finish()?;
            }
            TypedExprKind::Constant(c) => f
                .debug_tuple(&self.header(&format!("Constant({c})"), target_expr_ref))
                .finish()?,
            TypedExprKind::Variable(SyntaxVar(v)) => f
                .debug_tuple(&self.header(&format!("Variable({v})"), target_expr_ref))
                .finish()?,
            TypedExprKind::VariableRef(var_ref) => f
                .debug_tuple(&self.header("VarRef", target_expr_ref))
                .field(&self.child(*var_ref, None))
                .finish()?,
            TypedExprKind::Translate(expr_ref, _) => f
                .debug_tuple(&self.header("Translate", target_expr_ref))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::SequenceMap(expr_ref, _) => f
                .debug_tuple(&self.header("SequenceMap", target_expr_ref))
                .field(&self.child(*expr_ref, None))
                .finish()?,
        };

        Ok(())
    }
}

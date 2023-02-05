//! # Typed expressiona
//!
//! These data structures are emitted from the type check stage, and used in the codegen stage.

use std::{collections::HashMap, fmt::Debug, ops::Index};

use ontol_runtime::{proc::BuiltinProc, RelationId};

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
    Call(BuiltinProc, Box<[ExprRef]>),
    /// A value object (object with one anonymous property/attribute)
    ValueObjPattern(ExprRef),
    /// A map object pattern
    MapObjPattern(HashMap<RelationId, ExprRef>),
    /// A variable definition
    Variable(SyntaxVar),
    /// A variable reference (usage site)
    VariableRef(ExprRef),
    /// A constant/literal expression
    Constant(i64),
    /// A translation from one type to another
    Translate(ExprRef, TypeRef<'m>),
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

    pub fn debug_tree(&self, rewrites: &RewriteTable, expr_ref: ExprRef) -> String {
        self.debug_tree_guard(rewrites, expr_ref, 0)
    }

    fn debug_tree_guard(&self, rewrites: &RewriteTable, expr_ref: ExprRef, depth: usize) -> String {
        if depth > 20 {
            return format!("[ERROR depth exceeded]");
        }
        let (target_expr_ref, expr, _) = self.resolve_expr(rewrites, expr_ref);
        let s = match &expr.kind {
            TypedExprKind::Unit => format!("{{}}"),
            TypedExprKind::Call(proc, params) => {
                let param_strings = params
                    .iter()
                    .map(|param_ref| self.debug_tree_guard(rewrites, *param_ref, depth + 1))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({proc:?} {param_strings})")
            }
            TypedExprKind::ValueObjPattern(expr_ref) => {
                format!(
                    "(obj! {})",
                    self.debug_tree_guard(rewrites, *expr_ref, depth + 1)
                )
            }
            TypedExprKind::MapObjPattern(attributes) => {
                let attr_strings = attributes
                    .iter()
                    .map(|(relation_id, expr_ref)| {
                        let val = self.debug_tree_guard(rewrites, *expr_ref, depth + 1);
                        format!("({} {})", (relation_id.0 .0), val)
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("(obj! {attr_strings})")
            }
            TypedExprKind::Constant(c) => format!("{c}"),
            TypedExprKind::Variable(SyntaxVar(v)) => format!("var:{v}"),
            TypedExprKind::VariableRef(var_ref) => {
                let val = self.debug_tree_guard(rewrites, *var_ref, depth + 1);
                format!("=>{val}")
            }
            TypedExprKind::Translate(expr_ref, _) => {
                format!(
                    "(translate! {})",
                    self.debug_tree_guard(rewrites, *expr_ref, depth + 1)
                )
            }
        };
        if expr_ref != target_expr_ref {
            format!("{{{}->{}}}#{}", expr_ref.0, target_expr_ref.0, s)
        } else {
            format!("{{{}}}#{}", expr_ref.0, s)
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
            match &expr.kind {
                TypedExprKind::VariableRef(var_ref) => {
                    self.inner
                        .source_rewrites
                        .rewrite(ExprRef(index as u32), *var_ref);
                    self.inner
                        .target_rewrites
                        .rewrite(ExprRef(index as u32), *var_ref);
                }
                _ => {}
            }
        }
    }
}

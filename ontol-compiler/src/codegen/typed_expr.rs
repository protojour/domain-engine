use std::{collections::HashMap, fmt::Debug, ops::Index};

use ontol_runtime::{proc::BuiltinProc, RelationId};

use crate::{types::TypeRef, SourceSpan};

use super::rewrite::{RewriteTable, Rewriter};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SyntaxVar(pub u32);

#[derive(Clone, Debug)]
pub struct TypedExpr<'m> {
    pub kind: TypedExprKind<'m>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug)]
pub enum TypedExprKind<'m> {
    Unit,
    Call(BuiltinProc, Box<[NodeId]>),
    ValueObj(NodeId),
    MapObj(HashMap<RelationId, NodeId>),
    Variable(SyntaxVar),
    VariableRef(NodeId),
    Constant(i64),
    Translate(NodeId, TypeRef<'m>),
}

impl<'m> Debug for TypedExprTable<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedExprTable").finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct NodeId(pub u32);

pub const ERROR_NODE: NodeId = NodeId(u32::MAX);

#[derive(Default)]
pub struct TypedExpressions<'m>(pub(super) Vec<TypedExpr<'m>>);

impl<'m> Index<NodeId> for TypedExpressions<'m> {
    type Output = TypedExpr<'m>;

    fn index(&self, index: NodeId) -> &Self::Output {
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
    pub fn add_expr(&mut self, expr: TypedExpr<'m>) -> NodeId {
        let auto_rewrite = match &expr.kind {
            TypedExprKind::VariableRef(var_node_id) => Some(*var_node_id),
            _ => None,
        };

        let id = NodeId(self.expressions.0.len() as u32);
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
        source_node: NodeId,
    ) -> (NodeId, &'t TypedExpr<'m>, SourceSpan) {
        let span = self.expressions[source_node].span;
        let root_node = rewrite_table.resolve(source_node);
        (root_node, &self.expressions[root_node], span)
    }

    pub fn debug_tree(&self, rewrites: &RewriteTable, node_id: NodeId) -> String {
        self.debug_tree_guard(rewrites, node_id, 0)
    }

    fn debug_tree_guard(&self, rewrites: &RewriteTable, node_id: NodeId, depth: usize) -> String {
        if depth > 20 {
            return format!("[ERROR depth exceeded]");
        }
        let (target_node_id, expr, _) = self.resolve_expr(rewrites, node_id);
        let s = match &expr.kind {
            TypedExprKind::Unit => format!("{{}}"),
            TypedExprKind::Call(proc, params) => {
                let param_strings = params
                    .iter()
                    .map(|node_id| self.debug_tree_guard(rewrites, *node_id, depth + 1))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({proc:?} {param_strings})")
            }
            TypedExprKind::ValueObj(node_id) => {
                format!(
                    "(obj! {})",
                    self.debug_tree_guard(rewrites, *node_id, depth + 1)
                )
            }
            TypedExprKind::MapObj(attributes) => {
                let attr_strings = attributes
                    .iter()
                    .map(|(relation_id, node_id)| {
                        let val = self.debug_tree_guard(rewrites, *node_id, depth + 1);
                        format!("({} {})", (relation_id.0 .0), val)
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("(obj! {attr_strings})")
            }
            TypedExprKind::Constant(c) => format!("{c}"),
            TypedExprKind::Variable(SyntaxVar(v)) => format!("var:{v}"),
            TypedExprKind::VariableRef(var_node_id) => {
                let val = self.debug_tree_guard(rewrites, *var_node_id, depth + 1);
                format!("=>{val}")
            }
            TypedExprKind::Translate(node_id, _) => {
                format!(
                    "(translate! {})",
                    self.debug_tree_guard(rewrites, *node_id, depth + 1)
                )
            }
        };
        if node_id != target_node_id {
            format!("{{{}->{}}}#{}", node_id.0, target_node_id.0, s)
        } else {
            format!("{{{}}}#{}", node_id.0, s)
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
                TypedExprKind::VariableRef(node_id) => {
                    self.inner
                        .source_rewrites
                        .rewrite(NodeId(index as u32), *node_id);
                    self.inner
                        .target_rewrites
                        .rewrite(NodeId(index as u32), *node_id);
                }
                _ => {}
            }
        }
    }
}

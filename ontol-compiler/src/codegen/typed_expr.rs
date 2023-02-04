use std::{collections::HashMap, fmt::Debug, ops::Index};

use ontol_runtime::{proc::BuiltinProc, RelationId};

use crate::types::TypeRef;

use super::rewrite::{RewriteTable, Rewriter};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SyntaxVar(pub u32);

#[derive(Clone, Debug)]
pub struct TypedExpr<'m> {
    pub ty: TypeRef<'m>,
    pub kind: TypedExprKind<'m>,
}

#[derive(Clone, Debug)]
pub enum TypedExprKind<'m> {
    Unit,
    Call(BuiltinProc, Box<[NodeId]>),
    ValueObj(NodeId),
    MapObj(HashMap<RelationId, NodeId>),
    Variable(SyntaxVar),
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
        let id = NodeId(self.expressions.0.len() as u32);
        self.expressions.0.push(expr);
        self.source_rewrites.push_node(id);
        self.target_rewrites.push_node(id);
        id
    }

    /// Mark the table as "sealed".
    /// A sealed table can be reset to its original state.
    pub fn seal(self) -> SealedTypedExprTable<'m> {
        let size = self.expressions.0.len();
        SealedTypedExprTable { size, inner: self }
    }

    pub fn rewriter<'c>(&'c mut self) -> Rewriter<'c, 'm> {
        Rewriter::new(self)
    }

    pub fn get_expr<'t>(
        &'t self,
        rewrite_table: &RewriteTable,
        source_node: NodeId,
    ) -> (NodeId, &'t TypedExpr<'m>) {
        let root_node = rewrite_table.find_root(source_node);
        (root_node, &self.expressions[root_node])
    }

    pub fn debug_tree(&self, rewrites: &RewriteTable, node_id: NodeId) -> String {
        self.debug_tree_guard(rewrites, node_id, 0)
    }

    fn debug_tree_guard(&self, rewrites: &RewriteTable, node_id: NodeId, depth: usize) -> String {
        if depth > 20 {
            return format!("[ERROR depth exceeded]");
        }
        let (target_node_id, expr) = self.get_expr(rewrites, node_id);
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
            TypedExprKind::Variable(SyntaxVar(v)) => format!(":{v}"),
            TypedExprKind::Translate(node_id, _) => {
                format!(
                    "(translate! {})",
                    self.debug_tree_guard(rewrites, *node_id, depth + 1)
                )
            }
        };
        if node_id != target_node_id {
            format!("[{}=>{}]@{}", node_id.0, target_node_id.0, s)
        } else {
            format!("{}@{}", node_id.0, s)
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
    }
}

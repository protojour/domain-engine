//! (a (/ :x :y)) <- (a :x)(b :y) unpack=> ERROR
//!
//! (a (* :x 1000)) <- (a :x) unpack=> (a :x) <- (a (/ :x 1000))
//!
//!
//!
//! (a 1@(* 2@(+ 3@:x 42) 1000)) 2=>3 | (a 3@:x)          | 3=>4
//! (a 1@(* 3@:x 1000))          1=>3 | (a 4@(- 3@:x 42)) | 4=>5
//! (a 3@:x)                          | (a 5@(/ 4@(- 3@:x 42) 1000))
//!

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use ontol_runtime::{vm::BuiltinProc, PropertyId};

use crate::{rewrite::RewriteTable, types::TypeRef};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SyntaxVar(pub u32);

#[derive(Debug)]
pub struct TypedExpr<'m> {
    pub ty: TypeRef<'m>,
    pub kind: TypedExprKind,
}

#[derive(Debug)]
pub enum TypedExprKind {
    Unit,
    Call(BuiltinProc, Box<[NodeId]>),
    Obj(HashMap<PropertyId, NodeId>),
    Variable(SyntaxVar),
    Constant(i64),
}

#[derive(Default)]
pub struct TypedExprTable<'m> {
    exprs: Vec<TypedExpr<'m>>,
    pub source_rewrites: RewriteTable,
    pub target_rewrites: RewriteTable,
}

impl<'m> Debug for TypedExprTable<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedExprTable").finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct NodeId(pub u32);

pub const ERROR_NODE: NodeId = NodeId(u32::MAX);

impl<'m> TypedExprTable<'m> {
    pub fn add_expr(&mut self, expr: TypedExpr<'m>) -> NodeId {
        let id = NodeId(self.exprs.len() as u32);
        self.exprs.push(expr);
        self.source_rewrites.push_node(id);
        self.target_rewrites.push_node(id);
        id
    }

    pub fn expr_norewrite(&self, id: NodeId) -> &TypedExpr<'m> {
        &self.exprs[id.0 as usize]
    }

    pub fn fetch_expr<'t>(
        &'t self,
        rewrite_table: &RewriteTable,
        source_node: NodeId,
    ) -> (NodeId, &'t TypedExpr<'m>) {
        let root_node = rewrite_table.find_root(source_node);
        (root_node, &self.exprs[root_node.0 as usize])
    }

    pub fn fetch_exprs<'t>(
        &'t self,
        rewrite_table: &RewriteTable,
        node_ids: &[NodeId],
        target: &mut Vec<(NodeId, &'t TypedExpr<'m>)>,
    ) {
        target.clear();
        for source_node in node_ids {
            let root_node = rewrite_table.find_root(*source_node);
            target.push((root_node, &self.exprs[root_node.0 as usize]));
        }
    }

    pub fn find_variables(
        &self,
        rewrites: &RewriteTable,
        node_id: NodeId,
        variables: &mut HashSet<SyntaxVar>,
    ) {
        let (_, expr) = self.fetch_expr(rewrites, node_id);
        match &expr.kind {
            TypedExprKind::Unit => {}
            TypedExprKind::Call(_, params) => {
                for param in params.iter() {
                    self.find_variables(rewrites, *param, variables);
                }
            }
            TypedExprKind::Obj(attributes) => {
                for (_, val) in attributes.iter() {
                    self.find_variables(rewrites, *val, variables);
                }
            }
            TypedExprKind::Constant(_) => {}
            TypedExprKind::Variable(var) => {
                variables.insert(*var);
            }
        }
    }

    pub fn debug_tree(&self, rewrites: &RewriteTable, node_id: NodeId) -> String {
        let (_, expr) = self.fetch_expr(rewrites, node_id);
        match &expr.kind {
            TypedExprKind::Unit => format!("{{}}"),
            TypedExprKind::Call(proc, params) => {
                let param_strings = params
                    .iter()
                    .map(|node_id| self.debug_tree(rewrites, *node_id))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({proc:?} {param_strings})")
            }
            TypedExprKind::Obj(attributes) => {
                let attr_strings = attributes
                    .iter()
                    .map(|(prop, node_id)| {
                        let val = self.debug_tree(rewrites, *node_id);
                        format!("({} {})", prop.0, val)
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("(obj! {attr_strings})")
            }
            TypedExprKind::Constant(c) => format!("{c}"),
            TypedExprKind::Variable(SyntaxVar(v)) => format!(":{v}"),
        }
    }
}

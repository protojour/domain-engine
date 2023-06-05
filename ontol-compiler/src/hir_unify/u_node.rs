use std::collections::BTreeMap;

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::kind::Optional;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use tracing::debug;

use crate::typed_hir::{Meta, TypedHirNode};

use super::var_path::VarPath;

pub struct UNode<'m> {
    pub kind: UNodeKind<'m>,
    pub free_variables: BitSet,
    pub meta: Meta<'m>,
}

impl<'m> UNode<'m> {
    pub fn union_var(mut self, var: ontol_hir::Variable) -> Self {
        self.free_variables.insert(var.0 as usize);
        self
    }
}

pub enum UNodeKind<'m> {
    Leaf(LeafUNodeKind),
    Block(BlockUNode<'m>),
    Attr(AttrUNode<'m>),
    Expr(ExprUNode<'m>),
    SubScope(SubScopeUNode<'m>),
}

pub enum LeafUNodeKind {
    VariableRef(ontol_hir::Variable),
    Int(i64),
    Unit,
}

pub struct BlockUNode<'m> {
    pub kind: BlockUNodeKind<'m>,
    pub body: UBlockBody<'m>,
}

#[derive(Default)]
pub struct UBlockBody<'m> {
    pub sub_scoping: BTreeMap<usize, UBlockBody<'m>>,
    pub nodes: Vec<UNode<'m>>,
    pub dependent_scopes: Vec<UNode<'m>>,
}

pub enum BlockUNodeKind<'m> {
    Struct(ontol_hir::Binder),
    Let(ontol_hir::Binder, Box<UNode<'m>>),
}

pub struct AttrUNode<'m> {
    pub kind: AttrUNodeKind,
    pub attr: UAttr<'m>,
}

pub enum AttrUNodeKind {
    Seq(ontol_hir::Label),
    PropVariant(Optional, ontol_hir::Variable, PropertyId),
}

pub struct ExprUNode<'m> {
    pub kind: ExprUNodeKind,
    pub args: Vec<UNode<'m>>,
}

pub enum ExprUNodeKind {
    Call(BuiltinProc),
    Map,
}

pub struct SubScopeUNode<'m> {
    pub scope_idx: usize,
    pub sub_node: Box<UNode<'m>>,
}

pub struct UAttr<'m> {
    pub rel: Box<UNode<'m>>,
    pub val: Box<UNode<'m>>,
}

impl<'m> UNode<'m> {
    pub fn expand_scoping<'s>(
        &mut self,
        variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
    ) {
        todo!()
        // let unscoped = std::mem::take(&mut self.nodes);
        //
        // debug!("expand scoping, variable_paths = {variable_paths:?}");
        //
        // for target_node in unscoped {
        //     let mut paths_iterator = full_var_path(&target_node.free_variables, variable_paths);
        //     if let Some(PathSegment::Root(root_scope, scope_index)) = paths_iterator.next() {
        //         debug!("root_index: {scope_index}");
        //         ScopingCtx::new().merge_with_scope(
        //             self,
        //             target_node,
        //             Some(NextScope {
        //                 parent_scope: root_scope,
        //                 subscope_idx: scope_index,
        //             }),
        //             &mut paths_iterator,
        //         );
        //     } else {
        //         self.nodes.push(target_node);
        //     }
        // }
    }
}

use std::{collections::BTreeMap, fmt::Debug};

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::kind::{NodeKind, Optional};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use tracing::debug;

use crate::{
    hir_unify::var_path::{full_var_path, PathSegment},
    typed_hir::{Meta, TypedHirNode},
};

use super::var_path::{PathsIterator, VarPath};

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

    fn steal(&mut self) -> Self {
        let mut kind = UNodeKind::Stolen;
        std::mem::swap(&mut self.kind, &mut kind);
        Self {
            kind,
            free_variables: self.free_variables.clone(),
            meta: self.meta,
        }
    }
}

impl<'m> Debug for UNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UNode")
            .field(&self.free_variables)
            .field(&self.kind)
            .finish()
    }
}

#[derive(Debug)]
pub enum UNodeKind<'m> {
    Stolen,
    Leaf(LeafUNodeKind),
    Block(BlockUNodeKind<'m>, UBlockBody<'m>),
    Expr(ExprUNodeKind, Vec<UNode<'m>>),
    Attr(AttrUNodeKind, UAttr<'m>),
    SubScope(usize, Box<UNode<'m>>),
}

#[derive(Debug)]
pub enum LeafUNodeKind {
    VariableRef(ontol_hir::Variable),
    Int(i64),
    Unit,
}

#[derive(Debug)]
pub enum BlockUNodeKind<'m> {
    Raw,
    Struct(ontol_hir::Binder),
    Let(ontol_hir::Binder, Box<UNode<'m>>),
}

#[derive(Default, Debug)]
pub struct UBlockBody<'m> {
    pub sub_scoping: BTreeMap<usize, UBlockBody<'m>>,
    pub nodes: Vec<UNode<'m>>,
    pub dependent_scopes: Vec<UNode<'m>>,
}

#[derive(Debug)]
pub enum AttrUNodeKind {
    Seq(ontol_hir::Label),
    PropVariant(Optional, ontol_hir::Variable, PropertyId),
}

#[derive(Debug)]
pub enum ExprUNodeKind {
    Call(BuiltinProc),
    Map,
}

#[derive(Debug)]
pub struct UAttr<'m> {
    pub rel: Box<UNode<'m>>,
    pub val: Box<UNode<'m>>,
}

impl<'m> UNode<'m> {}

pub fn expand_scoping<'s, 'm>(
    u_node: &mut UNode<'m>,
    path_table: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
) {
    debug!("path table: {path_table:?}");

    ScopingCtx::new().expand_root(u_node, path_table)
}

#[derive(Debug)]
struct ScopingCtx {
    target_option_depth: u16,
    scope_option_depth: u16,
}

impl ScopingCtx {
    fn new() -> Self {
        Self {
            target_option_depth: 0,
            scope_option_depth: 0,
        }
    }

    fn expand_root<'s, 'm>(
        &mut self,
        u_node: &mut UNode<'m>,
        path_table: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
    ) {
        if u_node.free_variables.is_empty() {
            return;
        }

        match &mut u_node.kind {
            UNodeKind::Block(_kind, body) => {
                for child_node in std::mem::take(&mut body.nodes) {
                    if child_node
                        .free_variables
                        .iter()
                        .any(|var| path_table.contains_key(&ontol_hir::Variable(var as u32)))
                    {
                        let mut paths = full_var_path(&child_node.free_variables, path_table);
                        match paths.next() {
                            Some(PathSegment::Root(hir, subscope_idx)) => self.expand(
                                child_node,
                                ScopeExpansion {
                                    parent_scope: ParentScope::Block(body),
                                    next_scope: Some(NextScope {
                                        parent_hir: hir,
                                        subscope_idx,
                                    }),
                                },
                                &mut paths,
                            ),
                            _ => {}
                        }
                    } else {
                        body.nodes.push(child_node);
                    }
                }
            }
            UNodeKind::SubScope(..) => panic!("Subscope"),
            _ => {
                let mut paths = full_var_path(&u_node.free_variables, path_table);
                let new_node = u_node.steal();
                match paths.next() {
                    Some(PathSegment::Root(hir, subscope_idx)) => self.expand(
                        new_node,
                        ScopeExpansion {
                            parent_scope: ParentScope::ScopeNode(u_node),
                            next_scope: Some(NextScope {
                                parent_hir: hir,
                                subscope_idx,
                            }),
                        },
                        &mut paths,
                    ),
                    Some(_) => todo!(),
                    None => {}
                }
            }
        }
    }

    fn expand_node<'s, 'm>(
        &mut self,
        u_node: &mut UNode<'m>,
        next_scope: Option<NextScope<'s, 'm>>,
        paths: &mut PathsIterator<'s, 'm>,
    ) {
        match &mut u_node.kind {
            UNodeKind::Stolen => panic!("Stolen"),
            UNodeKind::Leaf(LeafUNodeKind::VariableRef(var)) => {
                if !u_node.free_variables.is_empty() {}
            }
            UNodeKind::Leaf(_) => {}
            UNodeKind::Block(_kind, body) => {
                for child_node in std::mem::take(&mut body.nodes) {
                    self.expand(
                        child_node,
                        ScopeExpansion {
                            parent_scope: ParentScope::Block(body),
                            next_scope,
                        },
                        paths,
                    );
                }
            }
            UNodeKind::Expr(kind, _) => todo!("Expr::{kind:?}"),
            UNodeKind::Attr(kind, _) => todo!("Attr::{kind:?}"),
            UNodeKind::SubScope(..) => panic!("Subscope"),
        }
    }

    fn expand<'s, 'u, 'm>(
        &mut self,
        u_node: UNode<'m>,
        mut expansion: ScopeExpansion<'s, 'u, 'm>,
        paths: &mut PathsIterator<'s, 'm>,
    ) {
        if u_node.free_variables.is_empty() {
            expansion.parent_scope.append_unscoped(u_node);
            return;
        }

        let UNode {
            kind,
            free_variables,
            meta,
        } = u_node;

        match kind {
            UNodeKind::Block(kind, block) => {
                todo!("inner block");
            }
            UNodeKind::Attr(
                AttrUNodeKind::PropVariant(optional, struct_var, property_id),
                mut attr,
            ) => {
                let mut expansion = if optional.0 {
                    self.target_option_depth += 1;
                    self.expand_scope_to_match_optional_depth(expansion, paths)
                } else {
                    self.expand_scope_as_far_as_possible(expansion, paths)
                };

                self.expand_node(&mut attr.rel, expansion.next_scope, paths);
                self.expand_node(&mut attr.val, expansion.next_scope, paths);

                expansion.parent_scope.append_unscoped(UNode {
                    free_variables,
                    kind: UNodeKind::Attr(
                        AttrUNodeKind::PropVariant(optional, struct_var, property_id),
                        attr,
                    ),
                    meta,
                });

                if optional.0 {
                    self.target_option_depth -= 1;
                }
            }
            kind => {
                let u_node = UNode {
                    free_variables,
                    kind,
                    meta,
                };

                let mut next_expansion = self.expand_scope_as_far_as_possible(expansion, paths);
                match &next_expansion.next_scope {
                    Some(_) => self.expand(u_node, next_expansion, paths),
                    None => next_expansion.parent_scope.append_unscoped(u_node),
                }
            }
        }
    }

    fn expand_scope_as_far_as_possible<'s, 'u, 'm>(
        &mut self,
        mut current_expansion: ScopeExpansion<'s, 'u, 'm>,
        paths: &mut PathsIterator<'s, 'm>,
    ) -> ScopeExpansion<'s, 'u, 'm> {
        loop {
            match self.expand_scope_level(current_expansion, ExpandTermination::NoOption, paths) {
                Ok(expansion) => {
                    if expansion.next_scope.is_none() {
                        return expansion;
                    }
                    current_expansion = expansion;
                }
                Err(prev_expansion) => return prev_expansion,
            }
        }
    }

    fn expand_scope_to_match_optional_depth<'s, 'u, 'm>(
        &mut self,
        mut current_expansion: ScopeExpansion<'s, 'u, 'm>,
        paths: &mut PathsIterator<'s, 'm>,
    ) -> ScopeExpansion<'s, 'u, 'm> {
        while self.scope_option_depth < self.target_option_depth {
            match self.expand_scope_level(current_expansion, ExpandTermination::OneOption, paths) {
                Ok(expansion) => {
                    if expansion.next_scope.is_none() {
                        return expansion;
                    }
                    current_expansion = expansion;
                }
                Err(_) => panic!(),
            }
        }

        current_expansion
    }

    fn expand_scope_level<'s, 'u, 'm>(
        &mut self,
        current_expansion: ScopeExpansion<'s, 'u, 'm>,
        termination: ExpandTermination,
        paths: &mut PathsIterator<'s, 'm>,
    ) -> Result<ScopeExpansion<'s, 'u, 'm>, ScopeExpansion<'s, 'u, 'm>> {
        let ScopeExpansion {
            parent_scope,
            next_scope,
        } = current_expansion;

        let NextScope {
            parent_hir,
            subscope_idx,
        } = match current_expansion.next_scope {
            Some(next_scope) => next_scope,
            None => {
                return Ok(ScopeExpansion {
                    parent_scope,
                    next_scope: None,
                })
            }
        };

        debug!("expand scope level {subscope_idx}");

        match &parent_hir.kind {
            NodeKind::VariableRef(..) | NodeKind::Unit | NodeKind::Int(_) => panic!(),
            NodeKind::Let(_, definition, body) => {
                let sub_scope = if subscope_idx == 0 {
                    definition
                } else {
                    &body[subscope_idx - 1]
                };
                self.expand_path(parent_scope.sub_scoping(subscope_idx), sub_scope, paths)
            }
            NodeKind::Call(_, sub_scopes) => self.expand_path(
                parent_scope.sub_scoping(subscope_idx),
                &sub_scopes[subscope_idx],
                paths,
            ),
            NodeKind::Map(sub_scope) => {
                self.expand_path(parent_scope.sub_scoping(subscope_idx), sub_scope, paths)
            }
            NodeKind::Seq(_, attr) => self.expand_path(
                parent_scope.sub_scoping(subscope_idx),
                &attr[subscope_idx],
                paths,
            ),
            NodeKind::Struct(_, sub_scopes) => {
                debug!("struct scope");
                self.expand_path(
                    parent_scope.sub_scoping(subscope_idx),
                    &sub_scopes[subscope_idx],
                    paths,
                )
            }
            NodeKind::Prop(optional, struct_var, id, variant_scopes) => {
                if optional.0 && matches!(termination, ExpandTermination::NoOption) {
                    return Err(ScopeExpansion {
                        parent_scope,
                        next_scope,
                    });
                }

                let sub_scopable = parent_scope.sub_scoping(subscope_idx);
                if optional.0 {
                    self.scope_option_depth += 1;
                }
                let prop_variant = &variant_scopes[subscope_idx];
                let attr_idx = match paths.next().expect("Needs an attribute scope") {
                    PathSegment::Sub(idx) => idx,
                    PathSegment::Root(..) => panic!("Root segment at attribute scope"),
                };

                let attr_scope = &prop_variant.attr[attr_idx].as_ref();
                let attr_sub_scopable = sub_scopable.sub_scoping(attr_idx);

                debug!(
                    "prop scope {struct_var} {id} variant_idx: {subscope_idx} attr_idx: {attr_idx}"
                );

                self.expand_path(attr_sub_scopable, attr_scope, paths)
            }
            NodeKind::MatchProp(..) => {
                panic!("match-prop is not a scope")
            }
            NodeKind::Gen(_, _, sub_scopes) => self.expand_path(
                parent_scope.sub_scoping(subscope_idx),
                &sub_scopes[subscope_idx],
                paths,
            ),
            NodeKind::Iter(_, _, sub_scopes) => self.expand_path(
                parent_scope.sub_scoping(subscope_idx),
                &sub_scopes[subscope_idx],
                paths,
            ),
            NodeKind::Push(_, attr) => self.expand_path(
                parent_scope.sub_scoping(subscope_idx),
                &attr[subscope_idx],
                paths,
            ),
        }
    }

    fn expand_path<'s, 'u, 'm>(
        &mut self,
        scope: ParentScope<'u, 'm>,
        parent_hir: &'s TypedHirNode<'m>,
        paths_iterator: &mut PathsIterator<'s, 'm>,
    ) -> Result<ScopeExpansion<'s, 'u, 'm>, ScopeExpansion<'s, 'u, 'm>> {
        match paths_iterator.next() {
            Some(PathSegment::Sub(next_subscope_idx)) => Ok(ScopeExpansion {
                parent_scope: scope,
                next_scope: Some(NextScope {
                    parent_hir,
                    subscope_idx: next_subscope_idx,
                }),
            }),
            Some(PathSegment::Root(_root_scope, _next_subscope_idx)) => {
                todo!();
                // parent_nodes.dependent_scopes.push(NodeSet::default());
                // let dependent_nodes = parent_nodes.dependent_scopes.last_mut().unwrap();
                // CurrentScope {
                //     parent_nodes: dependent_nodes,
                //     next_scope: Some(NextScope {
                //         parent_scope: root_scope,
                //         subscope_idx: next_subscope_idx,
                //     }),
                // }
            }
            None => Ok(ScopeExpansion {
                parent_scope: scope,
                next_scope: None,
            }),
        }
    }
}

enum ExpandTermination {
    NoOption,
    OneOption,
}

struct ScopeExpansion<'s, 'u, 'm> {
    parent_scope: ParentScope<'u, 'm>,
    next_scope: Option<NextScope<'s, 'm>>,
}

pub enum ParentScope<'u, 'm> {
    Block(&'u mut UBlockBody<'m>),
    ScopeNode(&'u mut UNode<'m>),
}

impl<'u, 'm> ParentScope<'u, 'm> {
    fn sub_scoping(self, subscope_idx: usize) -> ParentScope<'u, 'm> {
        match self {
            Self::Block(block) => {
                let block_body = block.sub_scoping.entry(subscope_idx).or_default();
                ParentScope::Block(block_body)
            }
            Self::ScopeNode(node) => {
                if matches!(&node.kind, UNodeKind::Stolen) {
                    let inner = UNode {
                        kind: UNodeKind::Stolen,
                        free_variables: node.free_variables.clone(),
                        meta: node.meta,
                    };

                    node.kind = UNodeKind::SubScope(subscope_idx, Box::new(inner));

                    match &mut node.kind {
                        UNodeKind::SubScope(_, inner) => ParentScope::ScopeNode(inner.as_mut()),
                        _ => panic!(),
                    }
                } else {
                    panic!("ScopeNode is not Void")
                }
            }
        }
    }

    fn append_unscoped(&mut self, u_node: UNode<'m>) {
        match self {
            Self::Block(block) => {
                block.nodes.push(u_node);
            }
            Self::ScopeNode(node) => {
                if matches!(&node.kind, UNodeKind::Stolen) {
                    **node = u_node;
                } else {
                    panic!("ScopeNode is not Void")
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct NextScope<'s, 'm> {
    parent_hir: &'s TypedHirNode<'m>,
    subscope_idx: usize,
}

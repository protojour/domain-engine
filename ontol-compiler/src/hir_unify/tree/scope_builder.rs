use bit_set::BitSet;
use ontol_hir::kind::{Dimension, NodeKind};

use crate::{
    hir_unify::VarSet,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::TypeRef,
};

use super::scope;

pub struct ScopeBuilder<'m> {
    _unit_type: TypeRef<'m>,
    in_scope: VarSet,
}

pub struct ScopeBinder<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub scope: scope::Scope<'m>,
}

impl<'m> ScopeBinder<'m> {
    fn unbound(scope: scope::Scope<'m>) -> Self {
        Self {
            binder: None,
            scope,
        }
    }

    fn to_scope_pattern_binding(self) -> scope::PatternBinding<'m> {
        match &self.scope.kind {
            scope::Kind::Unit => scope::PatternBinding::Wildcard,
            _ => scope::PatternBinding::Scope(
                match self.binder {
                    Some(binder) => binder.variable,
                    None => panic!("missing scope binder: {:?}", self.scope),
                },
                self.scope,
            ),
        }
    }
}

impl<'m> ScopeBuilder<'m> {
    pub fn new(unit_type: TypeRef<'m>) -> Self {
        Self {
            _unit_type: unit_type,
            in_scope: VarSet::default(),
        }
    }

    pub fn build_scope_binder(&mut self, node: &TypedHirNode<'m>) -> ScopeBinder<'m> {
        match &node.kind {
            NodeKind::Var(var) => {
                let scope = self.mk_scope(scope::Kind::Var(*var), node.meta);
                let scope = if self.in_scope.0.contains(var.0 as usize) {
                    scope
                } else {
                    scope.union_var(*var)
                };

                ScopeBinder {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: node.meta.ty,
                    }),
                    scope,
                }
            }
            NodeKind::Unit => ScopeBinder::unbound(self.mk_scope(scope::Kind::Unit, node.meta)),
            NodeKind::Int(_) => ScopeBinder::unbound(self.mk_scope(scope::Kind::Unit, node.meta)),
            NodeKind::Let(..) => todo!(),
            NodeKind::Call(proc, args) => todo!(),
            NodeKind::Map(arg) => self.build_scope_binder(arg),
            NodeKind::Seq(label, attr) => {
                todo!("seq scope")
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(*binder, |zelf| {
                let props: Vec<_> = nodes
                    .iter()
                    .enumerate()
                    .flat_map(|(disjoint_group, node)| zelf.build_props(node, disjoint_group))
                    .collect();

                let mut union = UnionBuilder::default();
                union.plus_iter(props.iter().map(|prop| &prop.vars));

                ScopeBinder {
                    binder: Some(TypedBinder {
                        variable: binder.0,
                        ty: node.meta.ty,
                    }),
                    scope: scope::Scope {
                        vars: union.vars,
                        kind: scope::Kind::Struct(scope::Struct(*binder, props)),
                        meta: node.meta,
                    },
                }
            }),
            NodeKind::Prop(optional, struct_var, id, variants) => panic!("standalone prop"),
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                todo!()
            }
            NodeKind::Iter(..) => {
                todo!()
            }
            NodeKind::Push(..) => {
                todo!()
            }
        }
    }

    fn build_props(
        &mut self,
        node: &TypedHirNode<'m>,
        disjoint_group: usize,
    ) -> Vec<scope::Prop<'m>> {
        match &node.kind {
            NodeKind::Prop(optional, struct_var, prop_id, variants) => variants
                .iter()
                .map(|variant| {
                    let (kind, vars) = match variant.dimension {
                        Dimension::Singular => {
                            let mut union = UnionBuilder::default();
                            let rel = union
                                .plus(self.build_scope_binder(&variant.attr.rel))
                                .to_scope_pattern_binding();
                            let val = union
                                .plus(self.build_scope_binder(&variant.attr.val))
                                .to_scope_pattern_binding();

                            (scope::PropKind::Attr(rel, val), union.vars)
                        }
                        Dimension::Seq(label) => {
                            todo!("seq prop")
                        }
                    };

                    scope::Prop {
                        struct_var: *struct_var,
                        optional: *optional,
                        prop_id: *prop_id,
                        disjoint_group,
                        kind,
                        vars,
                    }
                })
                .collect(),
            _ => panic!("not a prop: {node}"),
        }
    }

    fn mk_scope(&self, kind: scope::Kind<'m>, meta: Meta<'m>) -> scope::Scope<'m> {
        scope::Scope {
            kind,
            vars: VarSet(BitSet::new()),
            meta,
        }
    }

    fn enter_binder<T>(
        &mut self,
        binder: ontol_hir::Binder,
        func: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.in_scope.0.insert(binder.0 .0 as usize) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.0.remove(binder.0 .0 as usize);
        value
    }
}

#[derive(Default)]
pub struct UnionBuilder {
    vars: VarSet,
}

impl UnionBuilder {
    fn plus<'m>(&mut self, binder: ScopeBinder<'m>) -> ScopeBinder<'m> {
        self.vars.0.union_with(&binder.scope.vars.0);
        binder
    }

    fn plus_vec<'m>(&mut self, scopes: Vec<scope::Scope<'m>>) -> Vec<scope::Scope<'m>> {
        for scope in &scopes {
            self.vars.0.union_with(&scope.vars.0);
        }
        scopes
    }

    fn plus_iter<'a>(&mut self, iter: impl Iterator<Item = &'a VarSet>) {
        for var_set in iter {
            self.vars.0.union_with(&var_set.0);
        }
    }

    // pub fn plus_block_body<'m>(&mut self, u_nodes: Vec<UNode<'m>>) -> UBlockBody<'m> {
    //     let u_nodes = self.plus_vec(u_nodes);
    //     UBlockBody {
    //         sub_scoping: Default::default(),
    //         nodes: u_nodes,
    //         dependent_scopes: Default::default(),
    //     }
    // }
}

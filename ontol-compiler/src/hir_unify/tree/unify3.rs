use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::kind::{
    Attribute, Dimension, MatchArm, NodeKind, Optional, PropPattern, PropVariant,
};
use ontol_runtime::DefId;
use tracing::debug;

use crate::{
    hir_unify::{unifier::UnifierResult, UnifierError, VarSet},
    mem::Intern,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
    types::{Type, TypeRef, Types},
    SourceSpan,
};

use super::{expr, scope};

pub struct Unifier3<'a, 'm> {
    pub(super) types: &'a mut Types<'m>,
}

pub struct UnifiedNode3<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub node: TypedHirNode<'m>,
}

struct UnifiedBlock3<'m> {
    pub typed_binder: Option<TypedBinder<'m>>,
    pub block: Vec<TypedHirNode<'m>>,
}

struct ScopedProps<'m> {
    scope: scope::Prop<'m>,
    props: Vec<expr::Prop<'m>>,
}

impl<'a, 'm> Unifier3<'a, 'm> {
    pub(super) fn unify3(
        &mut self,
        scope: scope::Scope<'m>,
        expr: expr::Expr<'m>,
    ) -> Result<UnifiedNode3<'m>, UnifierError> {
        match (expr.kind, scope.kind) {
            // ### Expr constants - no scope needed:
            (expr::Kind::Unit, _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Unit,
                    meta: expr.meta,
                },
            }),
            (expr::Kind::Int(int), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(int),
                    meta: expr.meta,
                },
            }),
            // ### Simple scopes, no swizzling needed:
            (expr::Kind::Var(var), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Var(var),
                    meta: expr.meta,
                },
            }),
            // ### Unit scopes:
            (expr::Kind::Prop(prop), scope::Kind::Unit) => {
                let unit_scope = self.unit_scope();
                let rel = self.unify3(unit_scope.clone(), prop.attr.rel)?;
                let val = self.unify3(unit_scope, prop.attr.val)?;

                Ok(UnifiedNode3 {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Prop(
                            Optional(false),
                            prop.struct_var,
                            prop.prop_id,
                            vec![PropVariant {
                                dimension: Dimension::Singular,
                                attr: Attribute {
                                    rel: Box::new(rel.node),
                                    val: Box::new(val.node),
                                },
                            }],
                        ),
                        meta: expr.meta,
                    },
                })
            }
            (expr::Kind::Call(call), scope::Kind::Unit) => {
                let unit_scope = self.unit_scope();
                let args = call
                    .1
                    .into_iter()
                    .map(|arg| {
                        self.unify3(unit_scope.clone(), arg)
                            .map(|unified| unified.node)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(UnifiedNode3 {
                    typed_binder: None,
                    node: TypedHirNode {
                        kind: NodeKind::Call(call.0, args),
                        meta: expr.meta,
                    },
                })
            }
            (expr::Kind::Struct(struct_expr), scope::Kind::Unit) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Struct(struct_expr.0, vec![]),
                    meta: expr.meta,
                },
            }),
            // ### "zwizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let prop_hiearchy = PropHierarchyBuilder::new(struct_scope.1)?.build(struct_expr.1);
                let nodes = prop_hiearchy
                    .into_iter()
                    .map(|level| self.unify_merged_prop_scope(level))
                    .collect::<Result<_, _>>()?;

                Ok(UnifiedNode3 {
                    typed_binder: Some(TypedBinder {
                        variable: struct_scope.0 .0,
                        ty: scope.meta.ty,
                    }),
                    node: TypedHirNode {
                        kind: NodeKind::Struct(struct_expr.0, nodes),
                        meta: expr.meta,
                    },
                })
            }
            (expr, scope) => panic!("unhandled expr/scope combo: {expr:#?} / {scope:#?}"),
        }
    }

    fn fully_wrap_in_scope(
        &mut self,
        scope: scope::Scope<'m>,
        nodes_func: impl FnOnce(&mut Self) -> UnifierResult<Vec<TypedHirNode<'m>>>,
    ) -> UnifierResult<UnifiedBlock3<'m>> {
        match scope.kind {
            scope::Kind::Unit => {
                let nodes = nodes_func(self)?;

                Ok(UnifiedBlock3 {
                    typed_binder: None,
                    block: nodes,
                })
            }
            scope::Kind::Var(_) => todo!(),
            scope::Kind::Struct(_) => todo!(),
            scope::Kind::Let(let_scope) => {
                let inner_block = self.fully_wrap_in_scope(*let_scope.sub_scope, nodes_func)?;
                let ty = inner_block
                    .block
                    .iter()
                    .map(|node| node.meta.ty)
                    .last()
                    .unwrap_or_else(|| self.unit_type());

                let node = TypedHirNode {
                    kind: NodeKind::Let(
                        let_scope.inner_binder,
                        Box::new(let_scope.def),
                        inner_block.block,
                    ),
                    meta: Meta {
                        ty,
                        span: SourceSpan::none(),
                    },
                };

                Ok(UnifiedBlock3 {
                    typed_binder: let_scope.outer_binder,
                    block: vec![node],
                })
            }
        }
    }

    fn unify_merged_prop_scope(
        &mut self,
        level: PropHierarchy<'m>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let prop_exprs: Vec<_> = level
            .props
            .into_iter()
            .map(|prop| self.mk_prop_expr(prop))
            .collect();

        let sub_nodes = level
            .children
            .into_iter()
            .map(|sub_level| self.unify_merged_prop_scope(sub_level))
            .collect::<Result<Vec<_>, _>>()?;

        // FIXME: re-grouped match arms
        let match_arm: MatchArm<_> = match level.scope.kind {
            scope::PropKind::Attr(rel_scope, val_scope) => {
                let hir_rel_binding = rel_scope.hir_pattern_binding();
                let hir_val_binding = val_scope.hir_pattern_binding();

                // FIXME: if both scopes are defined, one should be "inside" the other
                let scope = match val_scope {
                    scope::PatternBinding::Wildcard => self.unit_scope(),
                    scope::PatternBinding::Scope(_, scope) => scope,
                };

                let unified_block = self.fully_wrap_in_scope(scope, move |zelf| {
                    let mut nodes = vec![];
                    let unit_scope = zelf.unit_scope();
                    for expr in prop_exprs {
                        nodes.push(zelf.unify3(unit_scope.clone(), expr)?.node);
                    }

                    nodes.extend(sub_nodes);

                    Ok(nodes)
                })?;

                MatchArm {
                    pattern: PropPattern::Attr(hir_rel_binding, hir_val_binding),
                    nodes: unified_block.block,
                }
            }
            scope::PropKind::Seq(binding) => {
                todo!()
            }
        };

        let mut match_arms = vec![match_arm];

        if level.scope.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(TypedHirNode {
            kind: NodeKind::MatchProp(level.scope.struct_var, level.scope.prop_id, match_arms),
            meta: Meta {
                ty: self.types.intern(Type::Unit(DefId::unit())),
                span: SourceSpan::none(),
            },
        })
    }

    fn mk_prop_expr(&mut self, prop: expr::Prop<'m>) -> expr::Expr<'m> {
        let free_vars = prop.free_vars.clone();
        expr::Expr {
            kind: expr::Kind::Prop(Box::new(prop)),
            meta: Meta {
                ty: self.unit_type(),
                span: SourceSpan::none(),
            },
            free_vars,
        }
    }

    fn unit(&mut self) -> TypedHirNode<'m> {
        TypedHirNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.unit_type(),
                span: SourceSpan::none(),
            },
        }
    }

    fn unit_scope(&mut self) -> scope::Scope<'m> {
        scope::Scope {
            kind: scope::Kind::Unit,
            meta: self.unit_meta(),
            vars: VarSet::default(),
        }
    }

    fn unit_meta(&mut self) -> Meta<'m> {
        Meta {
            ty: self.unit_type(),
            span: SourceSpan::none(),
        }
    }

    fn unit_type(&mut self) -> TypeRef<'m> {
        self.types.intern(Type::Unit(DefId::unit()))
    }
}

struct PropHierarchy<'m> {
    scope: scope::Prop<'m>,
    props: Vec<expr::Prop<'m>>,
    children: Vec<PropHierarchy<'m>>,
}

struct PropHierarchyBuilder<'m> {
    scope_props_by_index: FnvHashMap<usize, scope::Prop<'m>>,
    scope_index_by_var: FnvHashMap<ontol_hir::Var, usize>,
}

impl<'m> PropHierarchyBuilder<'m> {
    fn new(scope_props: Vec<scope::Prop<'m>>) -> UnifierResult<Self> {
        let mut scope_routing_table = FnvHashMap::default();

        for (idx, scope_prop) in scope_props.iter().enumerate() {
            for var in &scope_prop.vars {
                if let Some(_) = scope_routing_table.insert(var, idx) {
                    return Err(UnifierError::NonUniqueVariableDatapoints([var].into()));
                }
            }
        }

        Ok(Self {
            scope_props_by_index: scope_props.into_iter().enumerate().collect(),
            scope_index_by_var: scope_routing_table,
        })
    }

    fn build(mut self, expr_props: Vec<expr::Prop<'m>>) -> Vec<PropHierarchy<'m>> {
        let mut scope_assignments: FnvHashMap<usize, Vec<expr::Prop>> = Default::default();
        let mut scope_routing_table: FnvHashMap<ontol_hir::Var, usize> =
            self.scope_index_by_var.clone();

        // TODO: Return constant props!
        let mut constant_props: Vec<expr::Prop> = vec![];

        for expr_prop in expr_props {
            let mut free_var_iter = expr_prop.free_vars.iter();
            if let Some(first_free_var) = free_var_iter.next() {
                let scope_idx = scope_routing_table.get(&first_free_var).cloned().unwrap();

                if !expr_prop.optional.0 {
                    // re-route all following to the current match arm (scope arm)
                    for next_free_var in free_var_iter {
                        scope_routing_table.insert(next_free_var, scope_idx);
                    }
                }

                scope_assignments
                    .entry(scope_idx)
                    .or_default()
                    .push(expr_prop);
            } else {
                constant_props.push(expr_prop);
            }
        }

        if !constant_props.is_empty() {
            todo!("Constant props!");
        }

        scope_assignments
            .into_iter()
            .map(|(scope_index, expr_props)| {
                let in_scope = self
                    .scope_props_by_index
                    .get(&scope_index)
                    .unwrap()
                    .vars
                    .clone();

                self.build_sub_hierarchy(expr_props, scope_index, in_scope)
            })
            .collect()
    }

    fn build_sub_hierarchy(
        &mut self,
        expr_props: Vec<expr::Prop<'m>>,
        scope_index: usize,
        in_scope: VarSet,
    ) -> PropHierarchy<'m> {
        let mut in_scope_props = vec![];
        let mut sub_groups: FnvHashMap<ontol_hir::Var, Vec<expr::Prop>> = Default::default();

        for expr_prop in expr_props {
            let mut difference = expr_prop.free_vars.0.difference(&in_scope.0);

            if let Some(unscoped_var) = difference.next() {
                sub_groups
                    .entry(ontol_hir::Var(unscoped_var as u32))
                    .or_default()
                    .push(expr_prop);
            } else {
                in_scope_props.push(expr_prop);
            }
        }

        let children = sub_groups
            .into_iter()
            .map(|(var, sub_props)| {
                // put one more variable in scope
                let mut sub_in_scope = in_scope.clone();
                sub_in_scope.0.insert(var.0 as usize);

                let sub_scope_index = *self.scope_index_by_var.get(&var).unwrap();

                self.build_sub_hierarchy(sub_props, sub_scope_index, sub_in_scope)
            })
            .collect();

        PropHierarchy {
            scope: self.scope_props_by_index.get(&scope_index).unwrap().clone(),
            props: in_scope_props,
            children,
        }
    }
}

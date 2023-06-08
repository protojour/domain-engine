use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::kind::{MatchArm, NodeKind, Optional, PropPattern};
use ontol_runtime::DefId;

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
            // ### "swizzling" cases:
            (expr::Kind::Struct(struct_expr), scope::Kind::Struct(struct_scope)) => {
                let mut scope_idx_table: FnvHashMap<ontol_hir::Var, usize> = Default::default();

                for (idx, scope_prop) in struct_scope.1.iter().enumerate() {
                    for var in &scope_prop.vars {
                        if let Some(_) = scope_idx_table.insert(var, idx) {
                            return Err(UnifierError::NonUniqueVariableDatapoints([var].into()));
                        }
                    }
                }

                let mut scoped_props: FnvHashMap<usize, Vec<expr::Prop>> = Default::default();
                let mut root_scope_indexes = BitSet::new();
                let mut merged_scope_indexes = BitSet::new();
                let mut child_scope_table: FnvHashMap<usize, BitSet> = Default::default();
                let mut constant_props: Vec<expr::Prop> = vec![];

                for expr_prop in struct_expr.1 {
                    let mut free_var_iter = expr_prop.free_vars.iter();
                    if let Some(first_free_var) = free_var_iter.next() {
                        let first_scope_idx = *scope_idx_table.get(&first_free_var).unwrap();
                        root_scope_indexes.insert(first_scope_idx);

                        for free_var in free_var_iter {
                            let scope_idx = *scope_idx_table.get(&free_var).unwrap();
                            if !expr_prop.optional.0 {
                                merged_scope_indexes.insert(scope_idx);
                            }
                            child_scope_table
                                .entry(first_scope_idx)
                                .or_default()
                                .insert(scope_idx);
                        }

                        scoped_props
                            .entry(first_scope_idx)
                            .or_default()
                            .push(expr_prop);
                    } else {
                        constant_props.push(expr_prop);
                    }
                }

                let mut input_scopes_by_idx: FnvHashMap<usize, scope::Prop> =
                    struct_scope.1.into_iter().enumerate().collect();

                let mut nodes = vec![];

                for scope_idx in root_scope_indexes.iter() {
                    if merged_scope_indexes.contains(scope_idx) {
                        continue;
                    }

                    let root_scope = input_scopes_by_idx.remove(&scope_idx).unwrap();
                    let mut sub_scoped_props = vec![];

                    let mut merged_props = vec![];
                    if let Some(props) = scoped_props.remove(&scope_idx) {
                        merged_props.extend(props);
                    }

                    if let Some(child_scope_indexes) = child_scope_table.remove(&scope_idx) {
                        for child_scope_idx in child_scope_indexes.into_iter() {
                            let child_scope = if merged_scope_indexes.contains(child_scope_idx) {
                                // "safe" to remove, since it is fully merged
                                input_scopes_by_idx.remove(&child_scope_idx)
                            } else {
                                input_scopes_by_idx.get(&child_scope_idx).cloned()
                            }
                            .unwrap();

                            // sub_scoped_props.push(child_scope.unwrap());

                            if let Some(props) = scoped_props.remove(&child_scope_idx) {
                                if !props.is_empty() {
                                    sub_scoped_props.push(ScopedProps {
                                        scope: child_scope,
                                        props,
                                    });
                                }
                            }
                        }
                    }

                    if !merged_props.is_empty() {
                        nodes.push(self.unify_merged_prop_scope(
                            ScopedProps {
                                scope: root_scope,
                                props: merged_props,
                            },
                            sub_scoped_props,
                        )?);
                    }
                }

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
            (expr::Kind::Prop(prop), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Prop(Optional(false), prop.struct_var, prop.prop_id, vec![]),
                    meta: expr.meta,
                },
            }),
            (expr::Kind::Struct(struct_expr), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Struct(struct_expr.0, vec![]),
                    meta: expr.meta,
                },
            }),
            (expr::Kind::Call(_call), _) => Ok(UnifiedNode3 {
                typed_binder: None,
                node: TypedHirNode {
                    kind: NodeKind::Int(42),
                    meta: expr.meta,
                },
            }),
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
        root: ScopedProps<'m>,
        sub_props: Vec<ScopedProps<'m>>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let prop_exprs: Vec<_> = root
            .props
            .into_iter()
            .map(|prop| self.mk_prop_expr(prop))
            .collect();

        let sub_nodes = sub_props
            .into_iter()
            .map(|sub_scope| self.unify_merged_prop_scope(sub_scope, vec![]))
            .collect::<Result<Vec<_>, _>>()?;

        // FIXME: re-grouped match arms
        let match_arm: MatchArm<_> = match root.scope.kind {
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

        if root.scope.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(TypedHirNode {
            kind: NodeKind::MatchProp(root.scope.struct_var, root.scope.prop_id, match_arms),
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

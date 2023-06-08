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
    pub types: &'a mut Types<'m>,
}

struct UnifiedBlock3<'m> {
    typed_binder: Option<TypedBinder<'m>>,
    block: Vec<TypedHirNode<'m>>,
}

impl<'a, 'm> Unifier3<'a, 'm> {
    fn unify3(
        &mut self,
        scope: scope::Scope<'m>,
        expr: expr::Expr<'m>,
    ) -> Result<TypedHirNode<'m>, UnifierError> {
        match (expr.kind, scope.kind) {
            (expr::Kind::Var(_var), _) => Ok(self.unit()),
            (expr::Kind::Unit, _) => Ok(self.unit()),
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
                    let mut sub_scopes = vec![];

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
                            };
                            sub_scopes.push(child_scope.unwrap());

                            if let Some(props) = scoped_props.remove(&child_scope_idx) {
                                merged_props.extend(props);
                            }
                        }
                    }

                    nodes.push(self.unify_merged_prop_scope(
                        root_scope,
                        sub_scopes,
                        merged_props,
                    )?)
                }

                Ok(TypedHirNode {
                    kind: NodeKind::Struct(struct_expr.0, nodes),
                    meta: expr.meta,
                })
            }
            (expr::Kind::Prop(prop), _) => Ok(TypedHirNode {
                kind: NodeKind::Prop(prop.optional, prop.struct_var, prop.prop_id, vec![]),
                meta: expr.meta,
            }),
            (expr::Kind::Struct(struct_expr), _) => Ok(TypedHirNode {
                // FIXME
                kind: NodeKind::Struct(struct_expr.0, vec![]),
                meta: expr.meta,
            }),
            (expr::Kind::Call(_call), _) => Ok(self.unit()),
            (expr::Kind::Int(_int), _) => Ok(self.unit()),
            (expr, scope) => panic!("unhandled expr/scope combo: {expr:#?} / {scope:#?}"),
        }
    }

    fn unify_block(
        &mut self,
        scope: scope::Scope<'m>,
        expressions: Vec<expr::Expr<'m>>,
    ) -> Result<UnifiedBlock3<'m>, UnifierError> {
        let mut nodes = vec![];
        for expr in expressions {
            nodes.push(self.unify3(scope.clone(), expr)?);
        }

        Ok(UnifiedBlock3 {
            typed_binder: None,
            block: nodes,
        })
    }

    fn unify_merged_prop_scope(
        &mut self,
        root_scope: scope::Prop<'m>,
        sub_scopes: Vec<scope::Prop<'m>>,
        props: Vec<expr::Prop<'m>>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        let prop_exprs = props
            .into_iter()
            .map(|prop| self.mk_prop_expr(prop))
            .collect();
        // let block_expr = self.mk_block_expr(prop_exprs);

        // FIXME: re-grouped match arms
        let match_arm: MatchArm<_> = match root_scope.kind {
            scope::PropKind::Attr(rel_scope, val_scope) => {
                let hir_rel_binding = rel_scope.hir_pattern_binding();
                let hir_val_binding = rel_scope.hir_pattern_binding();

                // FIXME: rel_scope
                let unified_block = match val_scope {
                    scope::PatternBinding::Wildcard => {
                        let unit_scope = self.unit_scope();
                        self.unify_block(unit_scope, prop_exprs)?
                    }
                    scope::PatternBinding::Scope(binding, _) => {
                        // FIXME: binding scope
                        let unit_scope = self.unit_scope();
                        self.unify_block(unit_scope, prop_exprs)?
                    }
                };

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

        if root_scope.optional.0 {
            match_arms.push(MatchArm {
                pattern: PropPattern::Absent,
                nodes: vec![],
            });
        }

        Ok(TypedHirNode {
            kind: NodeKind::MatchProp(root_scope.struct_var, root_scope.prop_id, match_arms),
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

struct MergedPropScope<'m> {
    root_scope: scope::Prop<'m>,
    sub_scopes: Vec<scope::Prop<'m>>,
    props: Vec<expr::Prop<'m>>,
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use indoc::indoc;
    use ontol_hir::{kind::Optional, Binder, Var};
    use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
    use pretty_assertions::assert_eq;

    use crate::{
        hir_unify::{
            tree::{expr, scope},
            VarSet,
        },
        mem::Mem,
        typed_hir::Meta,
        types::Type,
        Compiler, SourceSpan,
    };

    use super::Unifier3;

    trait Vars: Sized {
        type Output: Sized;

        fn vars(self, vars: impl Into<VarSet>) -> Self::Output;
    }

    fn meta() -> Meta<'static> {
        Meta {
            ty: &Type::Error,
            span: SourceSpan::none(),
        }
    }

    fn expr(kind: expr::Kind<'static>, free_vars: impl Into<VarSet>) -> expr::Expr<'static> {
        expr::Expr {
            kind,
            meta: meta(),
            free_vars: free_vars.into(),
        }
    }

    fn scope(kind: scope::Kind<'static>, vars: impl Into<VarSet>) -> scope::Scope<'static> {
        scope::Scope {
            kind,
            meta: meta(),
            vars: vars.into(),
        }
    }

    impl Vars for expr::Struct<'static> {
        type Output = expr::Expr<'static>;

        fn vars(self, free_vars: impl Into<VarSet>) -> Self::Output {
            expr(expr::Kind::Struct(self), free_vars)
        }
    }

    impl Vars for expr::Call<'static> {
        type Output = expr::Expr<'static>;

        fn vars(self, free_vars: impl Into<VarSet>) -> Self::Output {
            expr(expr::Kind::Call(self), free_vars)
        }
    }

    impl Vars for scope::Struct<'static> {
        type Output = scope::Scope<'static>;

        fn vars(self, vars: impl Into<VarSet>) -> Self::Output {
            scope(scope::Kind::Struct(self), vars)
        }
    }

    impl From<()> for expr::Expr<'static> {
        fn from(_: ()) -> Self {
            expr(expr::Kind::Unit, VarSet::default())
        }
    }

    impl From<Var> for expr::Expr<'static> {
        fn from(var: Var) -> Self {
            expr(expr::Kind::Var(var), VarSet::default())
        }
    }

    impl<'m> From<i64> for expr::Expr<'m> {
        fn from(value: i64) -> Self {
            expr(expr::Kind::Int(value), VarSet::default())
        }
    }

    fn prop_id(str: &str) -> PropertyId {
        str.parse().unwrap()
    }

    fn test_trees(
        scope_optional: Optional,
        expr_optional: Optional,
    ) -> (scope::Scope<'static>, expr::Expr<'static>) {
        (
            scope::Struct(
                Binder(Var(2)),
                vec![
                    scope::Prop {
                        optional: scope_optional,
                        struct_var: Var(2),
                        prop_id: prop_id("S:0:0"),
                        disjoint_group: 0,
                        kind: scope::PropKind::Attr(
                            scope::PatternBinding::Wildcard,
                            scope::PatternBinding::Wildcard,
                        ),
                        vars: [Var(0)].into(),
                    },
                    scope::Prop {
                        optional: scope_optional,
                        struct_var: Var(2),
                        prop_id: prop_id("S:1:1"),
                        disjoint_group: 1,
                        kind: scope::PropKind::Attr(
                            scope::PatternBinding::Wildcard,
                            scope::PatternBinding::Wildcard,
                        ),
                        vars: [Var(1)].into(),
                    },
                ],
            )
            .vars([Var(0), Var(1)]),
            expr::Struct(
                Binder(Var(3)),
                vec![
                    expr::Prop {
                        optional: expr_optional,
                        struct_var: Var(3),
                        prop_id: prop_id("O:2:2"),
                        attr: (
                            (),
                            expr::Call(BuiltinProc::Add, vec![Var(0).into(), Var(1).into()])
                                .vars([Var(0), Var(1)]),
                        )
                            .into(),
                        free_vars: [Var(0), Var(1)].into(),
                    },
                    expr::Prop {
                        optional: expr_optional,
                        struct_var: Var(3),
                        prop_id: prop_id("O:3:3"),
                        attr: (
                            (),
                            expr::Call(BuiltinProc::Add, vec![Var(0).into(), 20.into()])
                                .vars([Var(0)]),
                        )
                            .into(),
                        free_vars: [Var(0)].into(),
                    },
                    expr::Prop {
                        optional: expr_optional,
                        struct_var: Var(3),
                        prop_id: prop_id("O:4:4"),
                        attr: (
                            (),
                            expr::Call(BuiltinProc::Add, vec![Var(1).into(), 20.into()])
                                .vars([Var(1)]),
                        )
                            .into(),
                        free_vars: [Var(1)].into(),
                    },
                ],
            )
            .vars([Var(0), Var(1)]),
        )
    }

    fn test_unify3<'m>(scope: scope::Scope<'m>, expr: expr::Expr<'m>) -> String {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Default::default());
        let output = Unifier3 {
            types: &mut compiler.types,
        }
        .unify3(scope, expr)
        .unwrap();

        let mut out_str = String::new();
        use std::fmt::Write;
        write!(&mut out_str, "{output}").unwrap();
        out_str
    }

    #[test]
    fn unify3_1() {
        let (scope, expr) = test_trees(Optional(false), Optional(false));
        let output = test_unify3(scope, expr);
        let expected = indoc! {"
            (struct ($d)
                (match-prop $c S:0:0
                    (($_ $_)
                        (prop $d O:2:2)
                        (prop $d O:3:3)
                        (prop $d O:4:4)
                    )
                )
            )"
        };
        assert_eq!(expected, output);
    }
}

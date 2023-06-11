use indoc::indoc;
use ontol_hir::{
    kind::{NodeKind, Optional},
    Binder, Var,
};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use pretty_assertions::assert_eq;
use test_log::test;

use crate::{
    hir_unify::{
        tree::{expr, scope, unify3::Unifier3},
        VarSet,
    },
    mem::Mem,
    typed_hir::{HirFunc, Meta, TypedHirNode},
    types::Type,
    Compiler, SourceSpan,
};

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

fn expr<'m>(kind: expr::Kind<'m>, free_vars: impl Into<VarSet>) -> expr::Expr<'m> {
    expr::Expr {
        kind,
        meta: meta(),
        free_vars: free_vars.into(),
    }
}

fn scope<'m>(kind: scope::Kind<'m>, vars: impl Into<VarSet>) -> scope::Scope<'m> {
    scope::Scope {
        kind,
        meta: meta(),
        vars: vars.into(),
    }
}

impl<'m> Vars for expr::Struct<'m> {
    type Output = expr::Expr<'m>;

    fn vars(self, free_vars: impl Into<VarSet>) -> Self::Output {
        expr(expr::Kind::Struct(self), free_vars)
    }
}

impl<'m> Vars for expr::Call<'m> {
    type Output = expr::Expr<'m>;

    fn vars(self, free_vars: impl Into<VarSet>) -> Self::Output {
        expr(expr::Kind::Call(self), free_vars)
    }
}

impl<'m> Vars for scope::Struct<'m> {
    type Output = scope::Scope<'m>;

    fn vars(self, vars: impl Into<VarSet>) -> Self::Output {
        scope(scope::Kind::Struct(self), vars)
    }
}

impl<'m> From<()> for expr::Expr<'m> {
    fn from(_: ()) -> Self {
        expr(expr::Kind::Unit, VarSet::default())
    }
}

impl<'m> From<()> for scope::Scope<'m> {
    fn from(_: ()) -> Self {
        scope(scope::Kind::Const, VarSet::default())
    }
}

impl<'m> From<Var> for expr::Expr<'m> {
    fn from(var: Var) -> Self {
        expr(expr::Kind::Var(var), VarSet::default())
    }
}

impl<'m> From<scope::Let<'m>> for scope::Scope<'m> {
    fn from(let_: scope::Let<'m>) -> Self {
        let mut var_set = VarSet::default();
        var_set.0.insert(let_.inner_binder.0 .0 as usize);
        scope(scope::Kind::Let(let_), var_set)
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

fn test_trees<'m>(
    scope_optional: Optional,
    expr_optional: Optional,
) -> (scope::Scope<'m>, expr::Expr<'m>) {
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
                        scope::PatternBinding::Wildcard(meta()),
                        scope::PatternBinding::Scope(
                            Var(4),
                            scope::Let {
                                outer_binder: None,
                                inner_binder: Binder(Var(0)),
                                def: TypedHirNode {
                                    kind: NodeKind::Call(
                                        BuiltinProc::Sub,
                                        vec![
                                            TypedHirNode {
                                                kind: NodeKind::Var(Var(4)),
                                                meta: meta(),
                                            },
                                            TypedHirNode {
                                                kind: NodeKind::Int(10),
                                                meta: meta(),
                                            },
                                        ],
                                    ),
                                    meta: meta(),
                                },
                                sub_scope: Box::new(().into()),
                            }
                            .into(),
                        ),
                    ),
                    vars: [Var(0)].into(),
                },
                scope::Prop {
                    optional: scope_optional,
                    struct_var: Var(2),
                    prop_id: prop_id("S:1:1"),
                    disjoint_group: 1,
                    kind: scope::PropKind::Attr(
                        scope::PatternBinding::Wildcard(meta()),
                        scope::PatternBinding::Scope(
                            Var(5),
                            scope::Let {
                                outer_binder: None,
                                inner_binder: Binder(Var(1)),
                                def: TypedHirNode {
                                    kind: NodeKind::Call(
                                        BuiltinProc::Sub,
                                        vec![
                                            TypedHirNode {
                                                kind: NodeKind::Var(Var(5)),
                                                meta: meta(),
                                            },
                                            TypedHirNode {
                                                kind: NodeKind::Int(10),
                                                meta: meta(),
                                            },
                                        ],
                                    ),
                                    meta: meta(),
                                },
                                sub_scope: Box::new(().into()),
                            }
                            .into(),
                        ),
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
                        expr::Call(BuiltinProc::Add, vec![Var(0).into(), 20.into()]).vars([Var(0)]),
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
                        expr::Call(BuiltinProc::Add, vec![Var(1).into(), 20.into()]).vars([Var(1)]),
                    )
                        .into(),
                    free_vars: [Var(1)].into(),
                },
            ],
        )
        .vars([Var(0), Var(1)]),
    )
}

fn test_unify3(provider: impl Fn(&()) -> (scope::Scope<'_>, expr::Expr<'_>)) -> String {
    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, Default::default());
    let (scope, expr) = provider(&());
    let unified = Unifier3 {
        types: &mut compiler.types,
    }
    .unify3(scope, expr)
    .unwrap();
    let func = HirFunc {
        arg: unified.typed_binder.unwrap(),
        body: unified.node,
    };

    let mut out_str = String::new();
    use std::fmt::Write;
    write!(&mut out_str, "{func}").unwrap();
    out_str
}

#[test]
fn unify3_1_mandatory() {
    let output = test_unify3(|_| test_trees(Optional(false), Optional(false)));
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (let ($a (- $e 10))
                        (prop $d O:3:3
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $f)
                                (let ($b (- $f 10))
                                    (prop $d O:2:2
                                        (#u (+ $a $b))
                                    )
                                    (prop $d O:4:4
                                        (#u (+ $b 20))
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn unify3_1_option() {
    let output = test_unify3(|_| test_trees(Optional(true), Optional(true)));
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (let ($a (- $e 10))
                        (prop $d O:3:3
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $f)
                                (let ($b (- $f 10))
                                    (prop $d O:2:2
                                        (#u (+ $a $b))
                                    )
                                )
                            )
                            (())
                        )
                    )
                )
                (())
            )
            (match-prop $c S:1:1
                (($_ $f)
                    (let ($b (- $f 10))
                        (prop $d O:4:4
                            (#u (+ $b 20))
                        )
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

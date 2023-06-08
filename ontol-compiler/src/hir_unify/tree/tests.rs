use indoc::indoc;
use ontol_hir::{kind::Optional, Binder, Var};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use pretty_assertions::assert_eq;

use crate::{
    hir_unify::{
        tree::{expr, scope, unify3::Unifier3},
        VarSet,
    },
    mem::Mem,
    typed_hir::{HirFunc, Meta},
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

impl From<()> for scope::Scope<'static> {
    fn from(_: ()) -> Self {
        scope(scope::Kind::Unit, VarSet::default())
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
                        scope::PatternBinding::Scope(Var(4), ().into()),
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

fn test_unify3<'m>(scope: scope::Scope<'m>, expr: expr::Expr<'m>) -> String {
    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, Default::default());
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

// BUG: wrong
#[test]
fn unify3_1_mandatory() {
    let (scope, expr) = test_trees(Optional(false), Optional(false));
    let output = test_unify3(scope, expr);
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (prop $d O:2:2)
                    (prop $d O:3:3)
                    (match-prop $c S:1:1
                        (($_ $_)
                            (prop $d O:4:4)
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

// BUG: wrong
#[test]
fn unify3_1_option() {
    let (scope, expr) = test_trees(Optional(true), Optional(true));
    let output = test_unify3(scope, expr);
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (prop $d O:2:2)
                    (prop $d O:3:3)
                    (match-prop $c S:1:1
                        (($_ $_)
                            (prop $d O:4:4)
                        )
                        (())
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

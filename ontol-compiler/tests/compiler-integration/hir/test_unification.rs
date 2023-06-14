use indoc::indoc;
use ontol_compiler::{
    hir_unify::unify_to_function,
    mem::Mem,
    typed_hir::{TypedHir, TypedHirNode},
    Compiler,
};
use pretty_assertions::assert_eq;
use test_log::test;

fn parse_typed<'m>(src: &str) -> TypedHirNode<'m> {
    ontol_hir::parse::Parser::new(TypedHir)
        .parse(src)
        .unwrap()
        .0
}

fn test_unify(scope: &str, expr: &str) -> String {
    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, Default::default());
    let func = unify_to_function(&parse_typed(scope), &parse_typed(expr), &mut compiler).unwrap();
    let mut output = String::new();
    use std::fmt::Write;
    write!(&mut output, "{func}").unwrap();
    output
}

#[test]
fn test_unify_no_op() {
    let output = test_unify("$a", "$a");
    assert_eq!("|$a| $a", output);
}

#[test]
fn test_unify_expr1() {
    let output = test_unify("(- $a 10)", "$a");
    let expected = indoc! {"
        |$b| (let ($a (+ $b 10))
            $a
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_expr2() {
    let output = test_unify("$a", "(+ $a 20)");
    assert_eq!("|$a| (+ $a 20)", output);
}

#[test]
fn test_unify_symmetric_exprs() {
    let output = test_unify("(- $a 10)", "(+ $a 20)");
    let expected = indoc! {"
        |$b| (let ($a (+ $b 10))
            (+ $a 20)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_struct() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0 (#u $a))
        )
        ",
        "
        (struct ($c)
            (prop $c S:0:1 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $a)
                    (prop $c S:0:1
                        (#u $a)
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_two_prop_struct() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0 (#u $a))
            (prop $c S:1:1 (#u $b))
        )
        ",
        "
        (struct ($d)
            (prop $d O:0:0 (#u $a))
            (prop $d O:1:1 (#u $b))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $a)
                    (prop $d O:0:0
                        (#u $a)
                    )
                )
            )
            (match-prop $c S:1:1
                (($_ $b)
                    (prop $d O:1:1
                        (#u $b)
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_struct_map_prop() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0 (#u (map $a)))
        )
        ",
        "
        (struct ($c)
            (prop $c S:0:1 (#u (map $a)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $a)
                    (prop $c S:0:1
                        (#u (map $a))
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_struct_simple_arithmetic() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0 (#u (- $a 10)))
        )
        ",
        "
        (struct ($c)
            (prop $c S:0:1 (#u (+ $a 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $d)
                    (let ($a (+ $d 10))
                        (prop $c S:0:1
                            (#u (+ $a 20))
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_struct_arithmetic_property_dependency() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0 (#u (- $a 10)))
            (prop $c S:1:1 (#u (- $b 10)))
        )
        ",
        "
        (struct ($d)
            (prop $d O:2:2 (#u (+ $a $b)))
            (prop $d O:3:3 (#u (+ $a 20)))
            (prop $d O:4:4 (#u (+ $b 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (let ($a (+ $e 10))
                        (prop $d O:3:3
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $f)
                                (let ($b (+ $f 10))
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
fn test_unify_basic_seq_prop() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq (@d)
                    #u
                    $a
                )
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1
                (seq (@d)
                    #u
                    $a
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                ((seq $d)
                    (prop $c S:1:1
                        (#u
                            (gen $d ($e $_ $a)
                                (push $e #u $a)
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
fn test_unify_seq_prop_deep() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq (@f)
                    #u
                    (struct ($c)
                        (prop $c S:1:1
                            (seq (@g)
                                #u
                                (map $a)
                            )
                        )
                    )
                )
            )
        )",
        "
        (struct ($d)
            (prop $d O:0:0
                (seq (@f)
                    #u
                    (struct ($e)
                        (prop $e O:1:1
                            (seq (@g)
                                #u
                                (map $a)
                            )
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {"
        |$b| (struct ($d)
            (match-prop $b S:0:0
                ((seq $f)
                    (prop $d O:0:0
                        (#u
                            (gen $f ($h $_ $c)
                                (push $h #u
                                    (struct ($e)
                                        (match-prop $c S:1:1
                                            ((seq $g)
                                                (prop $e O:1:1
                                                    (#u
                                                        (gen $g ($i $_ $a)
                                                            (push $i #u (map $a))
                                                        )
                                                    )
                                                )
                                            )
                                        )
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
fn test_unify_flat_map1() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0
                (seq (@d)
                    #u
                    (struct ($e)
                        (prop $e S:2:2
                            (#u $a)
                        )
                    )
                )
            )
            (prop $c S:1:1 (#u $b))
        )
        ",
        "
        (seq (@d)
            #u
            (struct ($f)
                (prop $f O:0:0
                    (#u $a)
                )
                (prop $f O:1:1
                    (#u $b)
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (match-prop $c S:1:1
            (($_ $b)
                (match-prop $c S:0:0
                    ((seq $d)
                        (gen $d ($g $_ $e)
                            (push $g #u
                                (struct ($f)
                                    (match-prop $e S:2:2
                                        (($_ $a)
                                            (prop $f O:0:0
                                                (#u $a)
                                            )
                                        )
                                    )
                                    (prop $f O:1:1
                                        (#u $b)
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
fn test_unify_opt_props1() {
    let output = test_unify(
        "
        (struct ($b)
            (prop? $b S:0:0
                (#u (map $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop? $c O:1:1
                (#u (map $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $a)
                    (prop $c O:1:1
                        (#u (map $a))
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_props2() {
    let output = test_unify(
        "
        (struct ($b)
            (prop? $b S:0:0
                (#u
                    (struct ($c)
                        (prop? $c S:1:1
                            (#u $a)
                        )
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop? $d O:0:0
                (#u
                    (struct ($e)
                        (prop? $e O:1:1
                            (#u $a)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($d)
            (match-prop $b S:0:0
                (($_ $c)
                    (prop $d O:0:0
                        (#u
                            (struct ($e)
                                (match-prop $c S:1:1
                                    (($_ $a)
                                        (prop $e O:1:1
                                            (#u $a)
                                        )
                                    )
                                    (())
                                )
                            )
                        )
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_rel_and_val1() {
    let output = test_unify(
        "
        (struct ($c)
            (prop? $c S:1:7
                ($a $b)
            )
        )
        ",
        "
        (struct ($d)
            (prop? $d S:1:7
                (#u (+ $a $b))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:7
                (($a $b)
                    (prop $d S:1:7
                        (#u (+ $a $b))
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_rel_and_val_struct_merge1() {
    let output = test_unify(
        "
        (struct ($c)
            (prop? $c S:0:0
                (
                    (struct ($d) (prop $d S:1:0 (#u $a)))
                    (struct ($e) (prop $e S:1:1 (#u $b)))
                )
            )
        )
        ",
        "
        (struct ($f)
            (prop? $f O:0:0
                (#u (+ $a $b))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($f)
            (match-prop $c S:0:0
                (($d $e)
                    (prop $f O:0:0
                        (#u
                            (match-prop $e S:1:1
                                (($_ $b)
                                    (match-prop $d S:1:0
                                        (($_ $a) (+ $a $b))
                                    )
                                )
                            )
                        )
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_prop_variants() {
    let output = test_unify(
        "
        (struct ($e)
            (prop  $e S:0:0 (#u $a) (#u $b))
            (prop? $e S:1:1 (#u $c) (#u $d))
        )
        ",
        "
        (struct ($f)
            (prop  $f O:0:0 (#u $a) (#u $b))
            (prop? $f O:1:1 (#u $c) (#u $d))
        )
        ",
    );
    let expected = indoc! {"
        |$e| (struct ($f)
            (match-prop $e S:0:0
                (($_ $a)
                    (prop $f O:0:0
                        (#u $a)
                    )
                )
                (($_ $b)
                    (prop $f O:0:0
                        (#u $b)
                    )
                )
            )
            (match-prop $e S:1:1
                (($_ $c)
                    (prop $f O:1:1
                        (#u $c)
                    )
                )
                (($_ $d)
                    (prop $f O:1:1
                        (#u $d)
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(expected, output);
}

mod dep_scoping {
    pub const EXPR_1: &str = "
    (struct ($c)
        (prop $c S:0:0 (#u (+ $b $c)))
        (prop $c S:1:1 (#u (+ $a $b)))
        (prop $c S:2:2 (#u $a))
    )";

    pub const EXPR_2: &str = "
    (struct ($f)
        (prop $f O:0:0 (#u $a))
        (prop $f O:1:1 (#u $b))
        (prop $f O:2:2 (#u $c))
    )
    ";
}

#[test]
fn test_unify_dependent_scoping_forwards() {
    let output = test_unify(dep_scoping::EXPR_1, dep_scoping::EXPR_2);
    let expected = indoc! {"
        |$c| (struct ($f)
            (match-prop $c S:2:2
                (($_ $a)
                    (prop $f O:0:0
                        (#u $a)
                    )
                    (match-prop $c S:1:1
                        (($_ $h)
                            (let ($b (- $a $h))
                                (prop $f O:1:1
                                    (#u $b)
                                )
                                (match-prop $c S:0:0
                                    (($_ $g)
                                        (let ($c (- $b $g))
                                            (prop $f O:2:2
                                                (#u $c)
                                            )
                                        )
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
fn test_unify_dependent_scoping_backwards() {
    let output = test_unify(dep_scoping::EXPR_2, dep_scoping::EXPR_1);
    let expected = indoc! {"
        |$f| (struct ($c)
            (match-prop $f O:0:0
                (($_ $a)
                    (prop $c S:2:2
                        (#u $a)
                    )
                    (match-prop $f O:1:1
                        (($_ $b)
                            (prop $c S:1:1
                                (#u (+ $a $b))
                            )
                            (match-prop $f O:2:2
                                (($_ $c)
                                    (prop $c S:0:0
                                        (#u (+ $b $c))
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

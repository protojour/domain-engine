use indoc::indoc;
use ontol_compiler::{
    hir_unify::unifier::unify_to_function,
    mem::Mem,
    typed_hir::{TypedHir, TypedHirNode},
    Compiler,
};
use ontol_hir::parse::Parser;
use pretty_assertions::assert_eq;
use test_log::test;

fn parse_typed<'m>(src: &str) -> TypedHirNode<'m> {
    Parser::new(TypedHir).parse(src).unwrap().0
}

fn test_unify(source: &str, target: &str) -> String {
    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, Default::default());
    let func = unify_to_function(parse_typed(source), parse_typed(target), &mut compiler).unwrap();
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
                                )
                            )
                        )
                    )
                )
            )
            (match-prop $c S:1:1
                (($_ $g)
                    (let ($b (+ $g 10))
                        (prop $d O:4:4
                            (#u (+ $b 20))
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
                ((seq $h)
                    (prop $d O:0:0
                        (#u
                            (gen $h ($i $_ $c)
                                (push $i #u
                                    (struct ($e)
                                        (match-prop $c S:1:1
                                            ((seq $f)
                                                (prop $e O:1:1
                                                    (#u
                                                        (gen $f ($g $_ $a)
                                                            (push $g #u (map $a))
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
                    ((seq $g)
                        (gen $g ($h $_ $e)
                            (push $h #u
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

// BUG:
#[test]
fn test_unify_opt_props1() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:1:9
                (#u
                    (struct ($c)
                        (prop $c S:1:2
                            (#u $a)
                            ()
                        )
                    )
                )
                ()
            )
        )
        ",
        "
        (struct ($d)
            (prop $d S:1:12
                (#u
                    (struct ($e)
                        (prop $e S:1:5
                            (#u $a)
                            ()
                        )
                    )
                )
                ()
            )
        )
        ",
    );
    // BUG: the first (prop) should be inside only the first (match-prop), not both.
    let _expected = indoc! {"
        |$b| (struct ($d)
            (match-prop $b S:1:9
                (($_ $c)
                    (prop $d S:1:12
                        (#u
                            (struct ($e)
                                (match-prop $c S:1:2
                                    (($_ $a)
                                        (prop $e S:1:5
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
    let actual = indoc! {"
        |$b| (struct ($d)
            (match-prop $b S:1:9
                (($_ $c)
                    (match-prop $c S:1:2
                        (($_ $a)
                            (prop $d S:1:12
                                (#u
                                    (struct ($e)
                                        (prop $e S:1:5
                                            (#u $a)
                                            ()
                                        )
                                    )
                                )
                            )
                        )
                        (())
                    )
                )
                (())
            )
        )"
    };
    assert_eq!(actual, output);
}

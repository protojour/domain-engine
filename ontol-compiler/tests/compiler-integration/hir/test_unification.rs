use indoc::indoc;
use ontol_compiler::{
    mem::Mem,
    typed_ontos::{
        lang::{OntosNode, TypedOntos},
        unify::unifier::unify_to_function,
    },
    Compiler,
};
use ontol_hir::parse::Parser;
use pretty_assertions::assert_eq;
use test_log::test;

fn parse_typed<'m>(src: &str) -> OntosNode<'m> {
    Parser::new(TypedOntos).parse(src).unwrap().0
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
fn test_unify_complex_seq_prop2() {
    let output = test_unify(
        "
        (struct ($w)
            (prop $w S:1:9
                (seq (@aa)
                    #u
                    (struct ($x)
                        (prop $x S:1:9
                            (seq (@y)
                                #u
                                (map $c)
                            )
                        )
                        (prop $x S:1:11
                            (seq (@z)
                                #u
                                (map $e)
                            )
                        )
                    )
                )
            )
            (prop $w S:1:11
                (seq (@ae)
                    #u
                    (struct ($ab)
                        (prop $ab S:1:9
                            (seq (@ac)
                                #u
                                (map $h)
                            )
                        )
                        (prop $ab S:1:11
                            (seq (@ad)
                                #u
                                (map $j)
                            )
                        )
                    )
                )
            )
        )",
        "
        (struct ($af)
            (prop $af S:1:11
                (seq (@ae)
                    #u
                    (struct ($ag)
                        (prop $ag S:1:11
                            (seq (@ad)
                                #u
                                (map $j)
                            )
                        )
                        (prop $ag S:1:9
                            (seq (@ac)
                                #u
                                (map $h)
                            )
                        )
                    )
                )
            )
            (prop $af S:1:9
                (seq (@aa)
                    #u
                    (struct ($ah)
                        (prop $ah S:1:11
                            (seq (@z)
                                #u
                                (map $e)
                            )
                        )
                        (prop $ah S:1:9
                            (seq (@y)
                                #u
                                (map $c)
                            )
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {"
        |$w| (struct ($af)
            (match-prop $w S:1:9
                ((seq $am)
                    (prop $af S:1:9
                        (#u
                            (gen $am ($an $_ $x)
                                (push $an #u
                                    (struct ($ah)
                                        (match-prop $x S:1:9
                                            ((seq $ai)
                                                (prop $ah S:1:9
                                                    (#u
                                                        (gen $ai ($aj $_ $c)
                                                            (push $aj #u (map $c))
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        (match-prop $x S:1:11
                                            ((seq $ak)
                                                (prop $ah S:1:11
                                                    (#u
                                                        (gen $ak ($al $_ $e)
                                                            (push $al #u (map $e))
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
            (match-prop $w S:1:11
                ((seq $as)
                    (prop $af S:1:11
                        (#u
                            (gen $as ($at $_ $ab)
                                (push $at #u
                                    (struct ($ag)
                                        (match-prop $ab S:1:9
                                            ((seq $ao)
                                                (prop $ag S:1:9
                                                    (#u
                                                        (gen $ao ($ap $_ $h)
                                                            (push $ap #u (map $h))
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        (match-prop $ab S:1:11
                                            ((seq $aq)
                                                (prop $ag S:1:11
                                                    (#u
                                                        (gen $aq ($ar $_ $j)
                                                            (push $ar #u (map $j))
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

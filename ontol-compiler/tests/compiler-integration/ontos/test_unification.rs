use indoc::indoc;
use ontol_compiler::{
    mem::Mem,
    typed_ontos::{
        lang::{OntosNode, TypedOntos},
        unify::unifier::unify_to_function,
    },
    Compiler,
};
use ontos::parse::Parser;
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
                    ((seq $_ $e)
                        (match-prop $e S:2:2
                            (($_ $a)
                                (struct ($f)
                                    (prop $f O:0:0
                                        (#u $a)
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

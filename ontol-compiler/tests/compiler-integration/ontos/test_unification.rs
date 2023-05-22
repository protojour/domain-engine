use indoc::indoc;
use ontol_compiler::typed_ontos::{
    lang::{OntosNode, TypedOntos},
    unify::unifier::unify_to_function,
};
use ontos::parse::Parser;
use pretty_assertions::assert_eq;
use test_log::test;

fn parse_typed(src: &str) -> OntosNode<'static> {
    Parser::new(TypedOntos).parse(src).unwrap().0
}

fn test_unify(source: &str, target: &str) -> String {
    let func = unify_to_function(parse_typed(source), parse_typed(target)).unwrap();
    let mut output = String::new();
    use std::fmt::Write;
    write!(&mut output, "|${}| {}", func.arg.0 .0, func.body).unwrap();
    output
}

#[test]
fn test_unify_no_op() {
    let output = test_unify("$0", "$0");
    assert_eq!("|$0| $0", output);
}

#[test]
fn test_unify_expr1() {
    let output = test_unify("(- $0 10)", "$0");
    let expected = indoc! {"
        |$1| (let ($0 (+ $1 10))
            $0
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_expr2() {
    let output = test_unify("$0", "(+ $0 20)");
    assert_eq!("|$0| (+ $0 20)", output);
}

#[test]
fn test_unify_symmetric_exprs() {
    let output = test_unify("(- $0 10)", "(+ $0 20)");
    let expected = indoc! {"
        |$1| (let ($0 (+ $1 10))
            (+ $0 20)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_struct() {
    let output = test_unify(
        "
        (struct ($1)
            (prop $1 s:0:0 (#u $0))
        )
        ",
        "
        (struct ($2)
            (prop $2 s:0:1 (#u $0))
        )
        ",
    );
    let expected = indoc! {"
        |$1| (struct ($2)
            (match-prop $1 s:0:0
                (($_ $0)
                    (prop $2 s:0:1
                        (#u $0)
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
        (struct ($1)
            (prop $1 s:0:0 (#u (map $0)))
        )
        ",
        "
        (struct ($2)
            (prop $2 s:0:1 (#u (map $0)))
        )
        ",
    );
    let expected = indoc! {"
        |$1| (struct ($2)
            (match-prop $1 s:0:0
                (($_ $0)
                    (prop $2 s:0:1
                        (#u (map $0))
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
        (struct ($1)
            (prop $1 s:0:0 (#u (- $0 10)))
        )
        ",
        "
        (struct ($2)
            (prop $2 s:0:1 (#u (+ $0 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$1| (struct ($2)
            (match-prop $1 s:0:0
                (($_ $3)
                    (let ($0 (+ $3 10))
                        (prop $2 s:0:1
                            (#u (+ $0 20))
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
        (struct ($2)
            (prop $2 s:0:0 (#u (- $0 10)))
            (prop $2 s:1:1 (#u (- $1 10)))
        )
        ",
        "
        (struct ($3)
            (prop $3 o:2:2 (#u (+ $0 $1)))
            (prop $3 o:3:3 (#u (+ $0 20)))
            (prop $3 o:4:4 (#u (+ $1 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$2| (struct ($3)
            (match-prop $2 s:0:0
                (($_ $4)
                    (let ($0 (+ $4 10))
                        (prop $3 o:3:3
                            (#u (+ $0 20))
                        )
                        (match-prop $2 s:1:1
                            (($_ $5)
                                (let ($1 (+ $5 10))
                                    (prop $3 o:2:2
                                        (#u (+ $0 $1))
                                    )
                                )
                            )
                        )
                    )
                )
            )
            (match-prop $2 s:1:1
                (($_ $6)
                    (let ($1 (+ $6 10))
                        (prop $3 o:4:4
                            (#u (+ $1 20))
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
        (struct ($2)
            (prop $2 s:0:0 (#u $0))
            (prop $2 s:1:1
                (seq (#3)
                    #u
                    (struct ($4)
                        (prop $4 s:2:2
                            (#u $1)
                        )
                    )
                )
            )
        )
        ",
        "
        (seq (#3)
            #u
            (struct ($5)
                (prop $5 o:0:0
                    (#u $0)
                )
                (prop $5 o:1:1
                    (#u $1)
                )
            )
        )
        ",
    );
    let _expected = indoc! {"
        |$2| (match-prop $2 s:0:0
            (($_ $0)
                (match-prop $2 s:1:1
                    (seq ($_ $4)
                        (match-prop $4 s:2:2
                            (($_ $1)
                                (struct ($5)
                                    (prop $5 o:0:0
                                        (#u $0)
                                    )
                                    (prop $5 o:1:1
                                        (#u $1)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )"
    };
    // BUG: this is not the correct unification
    let actual = indoc! {"
        |$2| (match-prop $2 s:0:0
            (($_ $0)
                (match-prop $2 s:1:1
                    (($_ $4)
                        (match-prop $4 s:2:2
                            (($_ $1)
                                (seq (#3) #u
                                    (struct ($5)
                                        (prop $5 o:0:0
                                            (#u $0)
                                        )
                                        (prop $5 o:1:1
                                            (#u $1)
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
    assert_eq!(actual, output);
}

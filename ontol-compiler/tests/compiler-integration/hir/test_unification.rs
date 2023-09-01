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
fn test_unify_struct_in_struct_scope() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0
                (#u
                    (struct ($d)
                        (prop $d S:1:0 (#u $a))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:0:0 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $d)
                    (prop $d O:0:0
                        (#u
                            (match-prop $d S:1:0
                                (($_ $a) $a)
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
            (prop $c S:0:1 (#u (- $b 10)))
        )
        ",
        "
        (struct ($d)
            (prop $d O:0:0 (#u (+ $a $b)))
            (prop $d O:0:1 (#u (+ $a 20)))
            (prop $d O:0:2 (#u (+ $b 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (let ($a (+ $e 10))
                        (prop $d O:0:1
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:0:1
                            (($_ $f)
                                (let ($b (+ $f 10))
                                    (prop $d O:0:0
                                        (#u (+ $a $b))
                                    )
                                    (prop $d O:0:2
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
fn test_struct_arithmetic_property_dependency_within_struct() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0 (#u (- $a 10)))
            (prop $c S:0:1
                (#u
                    (struct ($e)
                        (prop $e S:1:0 (#u (- $b 10)))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:0:0 (#u (+ $a $b)))
            (prop $d O:0:1 (#u (+ $a 20)))
            (prop $d O:0:2 (#u (+ $b 20)))
        )
        ",
    );
    // FIXME: prop S:1:0 is extracted twice.
    // This will be fixed when scope properties get flattened and rebuilt.
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $f)
                    (let ($a (+ $f 10))
                        (prop $d O:0:1
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:0:1
                            (($_ $e)
                                (prop $d O:0:0
                                    (#u
                                        (match-prop $e S:1:0
                                            (($_ $g)
                                                (let ($b (+ $g 10))
                                                    (+ $a $b)
                                                )
                                            )
                                        )
                                    )
                                )
                                (prop $d O:0:2
                                    (#u
                                        (match-prop $e S:1:0
                                            (($_ $g)
                                                (let ($b (+ $g 10))
                                                    (+ $b 20)
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
fn test_unify_basic_seq_prop_no_default() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq (@d) (iter #u $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1
                (seq (@d) (iter #u $a))
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
                            (sequence ($e)
                                (for-each $d ($_ $a)
                                    (seq-push $e #u $a)
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
fn test_unify_basic_seq_prop_element_iter_mix() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0 (#u $a))
            (prop $b S:0:1 (seq (@d) (iter #u $b)))
            (prop $b S:0:2 (#u $c))
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1
                (seq (@d) (#u $a) (iter #u $b) (#u $c))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $a)
                    (match-prop $b S:0:2
                        (($_ $c)
                            (match-prop $b S:0:1
                                ((seq $d)
                                    (prop $c S:1:1
                                        (#u
                                            (sequence ($e)
                                                (seq-push $e #u $a)
                                                (for-each $d ($_ $b)
                                                    (seq-push $e #u $b)
                                                )
                                                (seq-push $e #u $c)
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
fn test_unify_seq_prop_deep() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq (@f)
                    (iter
                        #u
                        (struct ($c)
                            (prop $c S:1:1
                                (seq (@g)
                                    (iter #u (map $a))
                                )
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
                    (iter
                        #u
                        (struct ($e)
                            (prop $e O:1:1
                                (seq (@g)
                                    (iter #u (map $a))
                                )
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
                            (sequence ($h)
                                (for-each $f ($_ $c)
                                    (seq-push $h #u
                                        (struct ($e)
                                            (match-prop $c S:1:1
                                                ((seq $g)
                                                    (prop $e O:1:1
                                                        (#u
                                                            (sequence ($i)
                                                                (for-each $g ($_ $a)
                                                                    (seq-push $i #u (map $a))
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
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_seq_prop_default_value() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq-default (@d) (iter #u $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1
                (seq (@d) (iter #u $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                ((seq-default $d)
                    (prop $c S:1:1
                        (#u
                            (sequence ($e)
                                (for-each $d ($_ $a)
                                    (seq-push $e #u $a)
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
                    (iter
                        #u
                        (struct ($e)
                            (prop $e S:2:2
                                (#u $a)
                            )
                        )
                    )
                )
            )
            (prop $c S:1:1 (#u $b))
        )
        ",
        "
        (decl-seq (@d)
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
                        (sequence ($g)
                            (for-each $d ($_ $e)
                                (seq-push $g #u
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

mod unify_seq_scope_escape_1 {
    use super::test_unify;
    use indoc::indoc;
    use pretty_assertions::assert_eq;
    use test_log::test;

    const ARMS: (&str, &str) = (
        "(struct ($c)
            (prop $c S:0:0 (#u #u))
            (prop $c S:0:1 (seq (@a) (iter #u $b)))
        )",
        "(struct ($d)
            (prop $d O:0:0
                (#u
                    (struct ($e)
                        (prop $e O:1:0 (#u #u))
                        (prop $e O:1:1 (seq (@a) (iter #u $b)))
                    )
                )
            )
        )",
    );

    #[test]
    fn forward() {
        let output = test_unify(ARMS.0, ARMS.1);
        let expected = indoc! {"
            |$c| (struct ($d)
                (prop $d O:0:0
                    (#u
                        (struct ($e)
                            (match-prop $c S:0:1
                                ((seq $a)
                                    (prop $e O:1:1
                                        (#u
                                            (sequence ($f)
                                                (for-each $a ($_ $b)
                                                    (seq-push $f #u $b)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            (prop $e O:1:0
                                (#u #u)
                            )
                        )
                    )
                )
            )"
        };
        assert_eq!(expected, output);
    }

    #[test]
    // BUG: property scopes has to be refactored.
    // Probably need to flatten cross-struct non-sequence properties before running `dep_tree`,
    // then re-build hierarchy after.
    #[should_panic = "not yet implemented"]
    fn backward() {
        let output = test_unify(ARMS.1, ARMS.0);
        let expected = indoc! {"
            |$d| (struct ($c)
                (match-prop $d O:0:0
                    (($_ $e)
                        (match-prop $e O:1:1
                            ((seq $a)
                                (prop $c S:0:1
                                    (#u
                                        (sequence ($f)
                                            (for-each $a ($_ $b)
                                                (seq-push $f #u $b)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (prop $c S:0:0
                    (#u #u)
                )
            )
        "
        };
        assert_eq!(expected, output);
    }
}

mod unify_seq_scope_escape_2 {
    use super::test_unify;
    use indoc::indoc;
    use pretty_assertions::assert_eq;
    use test_log::test;

    const ARMS: (&str, &str) = (
        "(struct ($e)
            (prop $e S:0:0
                (#u
                    (struct ($f)
                        (prop $f S:1:0
                            (seq (@a) (iter #u $b))
                        )
                    )
                )
            )
            (prop $e S:0:1
                (seq (@c) (iter #u $d))
            )
        )",
        // Note: The expr prop O:0:0 itself does not depend on anything in scope.
        // So it's constant in this sense, but each _child_ need to _clone_ the original scope.
        "(struct ($g)
            (prop $g O:0:0
                (#u
                    (struct ($h)
                        (prop $h O:1:0
                            (#u
                                (struct ($i)
                                    (prop $i O:2:0
                                        (seq (@a) (iter #u $b))
                                    )
                                )
                            )
                        )
                        (prop $h O:1:1
                            (seq (@c) (iter #u $d))
                        )
                    )
                )
            )
        )",
    );

    #[test]
    fn forward() {
        let output = test_unify(ARMS.0, ARMS.1);
        let expected = indoc! {"
            |$e| (struct ($g)
                (prop $g O:0:0
                    (#u
                        (struct ($h)
                            (match-prop $e S:0:1
                                ((seq $c)
                                    (prop $h O:1:1
                                        (#u
                                            (sequence ($j)
                                                (for-each $c ($_ $d)
                                                    (seq-push $j #u $d)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            (match-prop $e S:0:0
                                (($_ $f)
                                    (prop $h O:1:0
                                        (#u
                                            (struct ($i)
                                                (match-prop $f S:1:0
                                                    ((seq $a)
                                                        (prop $i O:2:0
                                                            (#u
                                                                (sequence ($k)
                                                                    (for-each $a ($_ $b)
                                                                        (seq-push $k #u $b)
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
                    )
                )
            )"
        };
        assert_eq!(expected, output);
    }
}

#[test]
fn test_unify_regex_capture1() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:0:0
                (#u
                    (regex def@0:0 ((1 $a)))
                )
            )
        )
        ",
        "
        (struct ($c)
            (prop $c O:0:0
                (#u $a)
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:0:0
                (($_ $d)
                    (match-regex $d def@0:0
                        (((1 $a))
                            (prop $c O:0:0
                                (#u $a)
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
fn test_unify_regex_capture2() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0
                (#u
                    (regex def@0:0 ((1 $a) (2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:0:0 (#u $a))
            (prop $d O:1:0 (#u $b))
        )
        ",
    );

    // BUG/FIXME: This generated code sucks!!
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (match-regex $e def@0:0
                        (((1 $a) (2 $b))
                            (prop $d O:0:0
                                (#u $a)
                            )
                        )
                    )
                    (match-regex $e def@0:0
                        (((1 $a) (2 $b))
                            (prop $d O:1:0
                                (#u $b)
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
#[should_panic = "regex-seq"]
fn test_unify_regex_loop1() {
    let output = test_unify(
        // Contains a looping regex with two variations
        "
        (struct ($c)
            (prop $c S:0:0
                (#u
                    (regex-seq (@a) def@0:0 ((1 $a)) ((2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $e O:0:0 (seq (@a) (iter #u $a)))
            (prop $e O:0:1 (seq (@a) (iter #u $b)))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:0:0
                (($_ $e)
                    (let ($f (sequence))
                        (let ($g (sequence))
                            (regex-for-each $e def@0:0
                                (((1 $a))
                                    (seq-push $f #u $a)
                                )
                                (((2 $b))
                                    (seq-push $g #u $b)
                                )
                            )

                            (prop $d O:0:0 (#u $f))
                            (prop $d O:0:1 (#u $g))
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

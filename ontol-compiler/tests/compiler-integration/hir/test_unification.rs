use indoc::indoc;
use ontol_compiler::hir_unify::test_api::test_unify;
use pretty_assertions::assert_eq;
use test_log::test;

#[test]
fn test_unify_no_op() {
    let output = test_unify("$a", "$a");
    assert_eq!("|$a| $a", output);
}

#[test]
fn test_unify_expr1() {
    let output = test_unify("(- $a 10)", "$a");
    let expected = indoc! {"
        |$b| (let ($a (map (+ $b 10)))
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
        |$b| (let ($a (map (+ $b 10)))
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
            (prop $b S:1:0 (#u $a))
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                (($_ $a)
                    (prop $c S:1:1
                        (#u $a)
                    )
                )
            )
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_deep_structural_map() {
    let output = test_unify(
        "
        (struct ($f)
            (prop $f S:1:9
                (#u $a)
            )
            (prop $f S:1:10
                (#u $b)
            )
            (prop $f S:1:11
                (#u
                    (struct ($g)
                        (prop $g S:1:12
                            (#u $c)
                        )
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:2
                (#u $a)
            )
            (prop $d O:1:6
                (#u
                    (struct ($e)
                        (prop $e O:1:4
                            (#u $b)
                        )
                        (prop $e O:1:5
                            (#u $c)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$f| (struct ($d)
            (match-prop $f S:1:9
                (($_ $a)
                    (prop $d O:1:2
                        (#u $a)
                    )
                )
            )
            (match-prop $f S:1:10
                (($_ $b)
                    (match-prop $f S:1:11
                        (($_ $g)
                            (match-prop $g S:1:12
                                (($_ $c)
                                    (prop $d O:1:6
                                        (#u
                                            (struct ($e)
                                                (prop $e O:1:4
                                                    (#u $b)
                                                )
                                                (prop $e O:1:5
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
            )
            (move-rest-attrs $d $f)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_two_prop_struct() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0 (#u $a))
            (prop $c S:2:1 (#u $b))
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (#u $a))
            (prop $d O:2:1 (#u $b))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $a)
                    (prop $d O:1:0
                        (#u $a)
                    )
                )
            )
            (match-prop $c S:2:1
                (($_ $b)
                    (prop $d O:2:1
                        (#u $b)
                    )
                )
            )
            (move-rest-attrs $d $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_struct_in_struct_scope() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0
                (#u
                    (struct ($d)
                        (prop $d S:2:0 (#u $a))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $d)
                    (match-prop $d S:2:0
                        (($_ $a)
                            (prop $d O:1:0
                                (#u $a)
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $d $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_struct_map_prop() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:1:0 (#u (map $a)))
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1 (#u (map $a)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                (($_ $a)
                    (prop $c S:1:1
                        (#u (map $a))
                    )
                )
            )
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_struct_simple_arithmetic() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:1:0 (#u (- $a 10)))
        )
        ",
        "
        (struct ($c)
            (prop $c S:1:1 (#u (+ $a 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                (($_ $d)
                    (let ($a (map (+ $d 10)))
                        (prop $c S:1:1
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
            (prop $c S:1:0 (#u (- $a 10)))
            (prop $c S:1:1 (#u (- $b 10)))
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (#u (+ $a $b)))
            (prop $d O:1:1 (#u (+ $a 20)))
            (prop $d O:1:2 (#u (+ $b 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $e)
                    (let ($a (map (+ $e 10)))
                        (prop $d O:1:1
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $f)
                                (let ($b (map (+ $f 10)))
                                    (prop $d O:1:0
                                        (#u (+ $a $b))
                                    )
                                    (prop $d O:1:2
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
            (prop $c S:1:0 (#u (- $a 10)))
            (prop $c S:1:1
                (#u
                    (struct ($e)
                        (prop $e S:2:0 (#u (- $b 10)))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (#u (+ $a $b)))
            (prop $d O:1:1 (#u (+ $a 20)))
            (prop $d O:1:2 (#u (+ $b 20)))
        )
        ",
    );
    // FIXME: prop S:1:0 is extracted twice.
    // This will be fixed when scope properties get flattened and rebuilt.
    let _expected_when_fixed = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $f)
                    (let ($a (map (+ $f 10)))
                        (prop $d O:1:1
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $e)
                                (match-prop $e S:2:0
                                    (($_ $g)
                                        (let ($b (map (+ $g 10)))
                                            (prop $d O:1:0
                                                (#u (+ $a $b))
                                            )
                                            (prop $d O:1:2
                                                (#u (+ $b 20))
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

    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $f)
                    (let ($a (map (+ $f 10)))
                        (prop $d O:1:1
                            (#u (+ $a 20))
                        )
                        (match-prop $c S:1:1
                            (($_ $e)
                                (prop $d O:1:0
                                    (#u
                                        (match-prop $e S:2:0
                                            (($_ $g)
                                                (let ($b (map (+ $g 10)))
                                                    (+ $a $b)
                                                )
                                            )
                                        )
                                    )
                                )
                                (prop $d O:1:2
                                    (#u
                                        (match-prop $e S:2:0
                                            (($_ $g)
                                                (let ($b (map (+ $g 10)))
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
            (prop $b S:1:0
                (seq (@d) (iter #u $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:2:1
                (seq (@d) (iter #u $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                ((seq $d)
                    (prop $c S:2:1
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
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_seq_prop_element_iter_mix() {
    let output = test_unify(
        "
        (struct ($e)
            (prop $e S:1:0 (#u $a))
            (prop $e S:1:1 (seq (@d) (iter #u $b)))
            (prop $e S:1:2 (#u $c))
        )
        ",
        "
        (struct ($f)
            (prop $f S:2:1
                (seq (@d) (#u $a) (iter #u $b) (#u $c))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$e| (struct ($f)
            (match-prop $e S:1:1
                ((seq $d)
                    (match-prop $e S:1:0
                        (($_ $a)
                            (match-prop $e S:1:2
                                (($_ $c)
                                    (prop $f S:2:1
                                        (#u
                                            (sequence ($i)
                                                (seq-push $i #u $a)
                                                (for-each $d ($_ $b)
                                                    (seq-push $i #u $b)
                                                )
                                                (seq-push $i #u $c)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $f $e)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_seq_prop_deep() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:1:0
                (seq (@f)
                    (iter
                        #u
                        (struct ($c)
                            (prop $c S:2:1
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
            (prop $d O:1:0
                (seq (@f)
                    (iter
                        #u
                        (struct ($e)
                            (prop $e O:2:1
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
            (match-prop $b S:1:0
                ((seq $f)
                    (prop $d O:1:0
                        (#u
                            (sequence ($h)
                                (for-each $f ($_ $c)
                                    (seq-push $h #u
                                        (struct ($e)
                                            (match-prop $c S:2:1
                                                ((seq $g)
                                                    (prop $e O:2:1
                                                        (#u
                                                            (sequence ($k)
                                                                (for-each $g ($_ $a)
                                                                    (seq-push $k #u (map $a))
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
            (move-rest-attrs $d $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_seq_prop_merge_rel_and_val_zwizzle() {
    let output = test_unify(
        "
        (struct ($e)
            (prop $e S:1:0
                (seq (@f)
                    (iter
                        (struct ($g)
                            (prop $g S:2:0 (#u $a))
                            (prop $g S:2:1 (#u $b))
                        )
                        (struct ($h)
                            (prop $h S:3:0 (#u $c))
                            (prop $h S:3:1 (#u $d))
                        )
                    )
                )
            )
        )",
        "
        (struct ($i)
            (prop $i O:1:0
                (seq (@f)
                    (iter
                        (struct ($j)
                            (prop $j O:2:0 (#u $a))
                            (prop $j O:2:1 (#u $c))
                        )
                        (struct ($k)
                            (prop $k O:3:0 (#u $b))
                            (prop $k O:3:1 (#u $d))
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {"
        |$e| (struct ($i)
            (match-prop $e S:1:0
                ((seq $f)
                    (prop $i O:1:0
                        (#u
                            (sequence ($l)
                                (for-each $f ($g $h)
                                    (match-prop $g S:2:0
                                        (($_ $a)
                                            (match-prop $g S:2:1
                                                (($_ $b)
                                                    (match-prop $h S:3:0
                                                        (($_ $c)
                                                            (match-prop $h S:3:1
                                                                (($_ $d)
                                                                    (seq-push $l
                                                                        (struct ($j)
                                                                            (prop $j O:2:0
                                                                                (#u $a)
                                                                            )
                                                                            (prop $j O:2:1
                                                                                (#u $c)
                                                                            )
                                                                        )
                                                                        (struct ($k)
                                                                            (prop $k O:3:0
                                                                                (#u $b)
                                                                            )
                                                                            (prop $k O:3:1
                                                                                (#u $d)
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
                    )
                )
            )
            (move-rest-attrs $i $e)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_seq_prop_default_value() {
    let output = test_unify(
        "
        (struct ($b)
            (prop $b S:1:0
                (seq-default (@d) (iter #u $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:2:1
                (seq (@d) (iter #u $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                ((seq-default $d)
                    (prop $c S:2:1
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
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_flat_map1() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0
                (seq (@d)
                    (iter
                        #u
                        (struct ($e)
                            (prop $e S:3:2
                                (#u $a)
                            )
                        )
                    )
                )
            )
            (prop $c S:2:1 (#u $b))
        )
        ",
        "
        (decl-seq (@d)
            #u
            (struct ($f)
                (prop $f O:1:0
                    (#u $a)
                )
                (prop $f O:2:1
                    (#u $b)
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (sequence ($g)
            (match-prop $c S:1:0
                ((seq $d)
                    (match-prop $c S:2:1
                        (($_ $b)
                            (for-each $d ($_ $e)
                                (match-prop $e S:3:2
                                    (($_ $a)
                                        (seq-push $g #u
                                            (struct ($f)
                                                (prop $f O:2:1
                                                    (#u $b)
                                                )
                                                (prop $f O:1:0
                                                    (#u $a)
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
fn test_unify_flat_map2() {
    let output = test_unify(
        // The difference with test_unify_flat_map1 is that the non-seq-prop appears _first_ here:
        "
        (struct ($d)
            (prop $d S:1:2 (#u $a))
            (prop $d S:1:5
                (seq (@c)
                    (iter #u
                        (struct ($e)
                            (prop $e S:1:4
                                (#u $b)
                            )
                        )
                    )
                )
            )
        )
        ",
        "
        (decl-seq (@c) #u
            (struct ($f)
                (prop $f S:1:7
                    (#u $a)
                )
                (prop $f S:1:8
                    (#u $b)
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$d| (sequence ($i)
            (match-prop $d S:1:5
                ((seq $c)
                    (match-prop $d S:1:2
                        (($_ $a)
                            (for-each $c ($_ $e)
                                (match-prop $e S:1:4
                                    (($_ $b)
                                        (seq-push $i #u
                                            (struct ($f)
                                                (prop $f S:1:7
                                                    (#u $a)
                                                )
                                                (prop $f S:1:8
                                                    (#u $b)
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
fn test_unify_opt_props1() {
    let output = test_unify(
        "
        (struct ($b)
            (prop? $b S:1:0
                (#u (map $a))
            )
        )
        ",
        "
        (struct ($c)
            (prop? $c O:2:1
                (#u (map $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                (($_ $a)
                    (prop $c O:2:1
                        (#u (map $a))
                    )
                )
                (())
            )
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_props2() {
    let output = test_unify(
        "
        (struct ($b)
            (prop? $b S:1:0
                (#u
                    (struct ($c)
                        (prop? $c S:2:1
                            (#u $a)
                        )
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop? $d O:1:0
                (#u
                    (struct ($e)
                        (prop? $e O:2:1
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
            (match-prop $b S:1:0
                (($_ $c)
                    (prop $d O:1:0
                        (#u
                            (struct ($e)
                                (match-prop $c S:2:1
                                    (($_ $a)
                                        (prop $e O:2:1
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
            (move-rest-attrs $d $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_props3() {
    let output = test_unify(
        "
        (struct ($c)
            (prop? $c S:1:0
                (#u
                    (struct ($d)
                        (prop? $d S:2:0
                            (#u $a)
                        )
                    )
                )
            )
            (prop? $c S:1:1
                (#u
                    (struct ($e)
                        (prop? $e S:2:1
                            (#u $b)
                        )
                    )
                )
            )
        )
        ",
        // We should get "O:1:0" if either of "S:1:0" or "S:1:1" are defined(?)
        // Then its optional child props should be defined respectively based on free vars
        "
        (struct ($f)
            (prop? $f O:1:0
                (#u
                    (struct ($g)
                        (prop? $g O:2:0
                            (#u $a)
                        )
                        (prop? $g O:2:1
                            (#u $b)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($f)
            (prop $f O:1:0
                (#u
                    (struct ($g)
                        (match-prop $c S:1:0
                            (($_ $d)
                                (match-prop $d S:2:0
                                    (($_ $a)
                                        (prop $g O:2:0
                                            (#u $a)
                                        )
                                    )
                                    (())
                                )
                            )
                            (())
                        )
                        (match-prop $c S:1:1
                            (($_ $e)
                                (match-prop $e S:2:1
                                    (($_ $b)
                                        (prop $g O:2:1
                                            (#u $b)
                                        )
                                    )
                                    (())
                                )
                            )
                            (())
                        )
                    )
                )
            )
            (move-rest-attrs $f $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_props4() {
    let output = test_unify(
        "
        (struct ($c)
            (prop? $c S:1:0
                (#u
                    (struct ($d)
                        (prop? $d S:2:0
                            (#u $a)
                        )
                    )
                )
            )
            (prop? $c S:1:1
                (#u
                    (struct ($e)
                        (prop? $e S:2:1
                            (#u $b)
                        )
                    )
                )
            )
            (prop $c S:1:2 (#u $h))
        )
        ",
        "
        (struct ($f)
            (prop? $f O:1:0
                (#u
                    (struct ($g)
                        (prop? $g O:2:0
                            (#u $a)
                        )
                        (prop? $g O:2:1
                            (#u $b)
                        )
                        (prop $g O:2:2
                            (#u $h)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($f)
            (prop $f O:1:0
                (#u
                    (struct ($g)
                        (match-prop $c S:1:0
                            (($_ $d)
                                (match-prop $d S:2:0
                                    (($_ $a)
                                        (prop $g O:2:0
                                            (#u $a)
                                        )
                                    )
                                    (())
                                )
                            )
                            (())
                        )
                        (match-prop $c S:1:1
                            (($_ $e)
                                (match-prop $e S:2:1
                                    (($_ $b)
                                        (prop $g O:2:1
                                            (#u $b)
                                        )
                                    )
                                    (())
                                )
                            )
                            (())
                        )
                        (match-prop $c S:1:2
                            (($_ $h)
                                (prop $g O:2:2
                                    (#u $h)
                                )
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $f $c)
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
            (move-rest-attrs $d $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_opt_rel_and_val_struct_merge1() {
    let output = test_unify(
        "
        (struct ($c)
            (prop? $c S:1:0
                (
                    (struct ($d) (prop $d S:2:0 (#u $a)))
                    (struct ($e) (prop $e S:2:1 (#u $b)))
                )
            )
        )
        ",
        "
        (struct ($f)
            (prop? $f O:1:0
                (#u (+ $a $b))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (struct ($f)
            (match-prop $c S:1:0
                (($d $e)
                    (match-prop $d S:2:0
                        (($_ $a)
                            (match-prop $e S:2:1
                                (($_ $b)
                                    (prop $f O:1:0
                                        (#u (+ $a $b))
                                    )
                                )
                            )
                        )
                    )
                )
                (())
            )
            (move-rest-attrs $f $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_prop_variants() {
    let output = test_unify(
        "
        (struct ($e)
            (prop  $e S:1:0 (#u $a) (#u $b))
            (prop? $e S:2:1 (#u $c) (#u $d))
        )
        ",
        "
        (struct ($f)
            (prop  $f O:1:0 (#u $a) (#u $b))
            (prop? $f O:2:1 (#u $c) (#u $d))
        )
        ",
    );
    let expected = indoc! {"
        |$e| (struct ($f)
            (match-prop $e S:1:0
                (($_ $a)
                    (prop $f O:1:0
                        (#u $a)
                    )
                )
                (($_ $b)
                    (prop $f O:1:0
                        (#u $b)
                    )
                )
            )
            (match-prop $e S:2:1
                (($_ $c)
                    (prop $f O:2:1
                        (#u $c)
                    )
                )
                (($_ $d)
                    (prop $f O:2:1
                        (#u $d)
                    )
                )
                (())
            )
            (move-rest-attrs $f $e)
        )"
    };
    assert_eq!(expected, output);
}

mod dependent_scoping {
    use super::test_unify;
    use indoc::indoc;
    use pretty_assertions::assert_eq;
    use test_log::test;

    pub const EXPR_1: &str = "
    (struct ($c)
        (prop $c S:1:0 (#u (+ $b $c)))
        (prop $c S:2:1 (#u (+ $a $b)))
        (prop $c S:3:2 (#u $a))
    )";

    pub const EXPR_2: &str = "
    (struct ($f)
        (prop $f O:1:0 (#u $a))
        (prop $f O:2:1 (#u $b))
        (prop $f O:3:2 (#u $c))
    )
    ";

    #[test]
    fn forwards() {
        let output = test_unify(EXPR_1, EXPR_2);
        let expected = indoc! {"
            |$c| (struct ($f)
                (match-prop $c S:3:2
                    (($_ $a)
                        (prop $f O:1:0
                            (#u $a)
                        )
                        (match-prop $c S:2:1
                            (($_ $h)
                                (let ($b (map (- $a $h)))
                                    (prop $f O:2:1
                                        (#u $b)
                                    )
                                    (match-prop $c S:1:0
                                        (($_ $g)
                                            (let ($c (map (- $b $g)))
                                                (prop $f O:3:2
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
    fn backwards() {
        let output = test_unify(EXPR_2, EXPR_1);
        // FIXME: The classic unifier did this:
        let _optimal = indoc! {"
            |$f| (struct ($c)
                (match-prop $f O:1:0
                    (($_ $a)
                        (match-prop $f O:2:1
                            (($_ $b)
                                (prop $c S:2:1
                                    (#u (+ $a $b))
                                )
                                (match-prop $f O:3:2
                                    (($_ $c)
                                        (prop $c S:1:0
                                            (#u (+ $b $c))
                                        )
                                    )
                                )
                            )
                        )
                        (prop $c S:2:2
                            (#u $a)
                        )
                    )
                )
                (move-rest-attrs $c $f)
            )"
        };
        // FIXME: The backwards code does not trigger fallback to the classic unifier,
        // and the flat unifier does this (match-prop duplication):
        let expected = indoc! {"
            |$f| (struct ($c)
                (match-prop $f O:1:0
                    (($_ $a)
                        (match-prop $f O:2:1
                            (($_ $b)
                                (prop $c S:2:1
                                    (#u (+ $a $b))
                                )
                            )
                        )
                        (prop $c S:3:2
                            (#u $a)
                        )
                    )
                )
                (match-prop $f O:2:1
                    (($_ $b)
                        (match-prop $f O:3:2
                            (($_ $c)
                                (prop $c S:1:0
                                    (#u (+ $b $c))
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $c $f)
            )"
        };
        assert_eq!(expected, output);
    }
}

mod unify_seq_scope_escape_1 {
    use super::test_unify;
    use indoc::indoc;
    use pretty_assertions::assert_eq;
    use test_log::test;

    const ARMS: (&str, &str) = (
        "(struct ($c)
            (prop $c S:1:0 (#u #u))
            (prop $c S:1:1 (seq (@a) (iter #u $b)))
        )",
        "(struct ($d)
            (prop $d O:1:0
                (#u
                    (struct ($e)
                        (prop $e O:2:0 (#u #u))
                        (prop $e O:2:1 (seq (@a) (iter #u $b)))
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
                (prop $d O:1:0
                    (#u
                        (struct ($e)
                            (match-prop $c S:1:1
                                ((seq $a)
                                    (prop $e O:2:1
                                        (#u
                                            (sequence ($g)
                                                (for-each $a ($_ $b)
                                                    (seq-push $g #u $b)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            (prop $e O:2:0
                                (#u #u)
                            )
                        )
                    )
                )
                (move-rest-attrs $d $c)
            )"
        };
        assert_eq!(expected, output);
    }

    #[test]
    fn backward() {
        let output = test_unify(ARMS.1, ARMS.0);
        let expected = indoc! {"
            |$d| (struct ($c)
                (match-prop $d O:1:0
                    (($_ $e)
                        (match-prop $e O:2:1
                            ((seq $a)
                                (prop $c S:1:1
                                    (#u
                                        (sequence ($i)
                                            (for-each $a ($_ $b)
                                                (seq-push $i #u $b)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (prop $c S:1:0
                    (#u #u)
                )
                (move-rest-attrs $c $d)
            )"
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
            (prop $e S:1:0
                (#u
                    (struct ($f)
                        (prop $f S:2:0
                            (seq (@a) (iter #u $b))
                        )
                    )
                )
            )
            (prop $e S:1:1
                (seq (@c) (iter #u $d))
            )
        )",
        // Note: The expr prop O:1:0 itself does not depend on anything in scope.
        // So it's constant in this sense, but each _child_ need to _clone_ the original scope.
        "(struct ($g)
            (prop $g O:1:0
                (#u
                    (struct ($h)
                        (prop $h O:2:0
                            (#u
                                (struct ($i)
                                    (prop $i O:3:0
                                        (seq (@a) (iter #u $b))
                                    )
                                )
                            )
                        )
                        (prop $h O:2:1
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
                (prop $g O:1:0
                    (#u
                        (struct ($h)
                            (match-prop $e S:1:1
                                ((seq $c)
                                    (prop $h O:2:1
                                        (#u
                                            (sequence ($l)
                                                (for-each $c ($_ $d)
                                                    (seq-push $l #u $d)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            (prop $h O:2:0
                                (#u
                                    (struct ($i)
                                        (match-prop $e S:1:0
                                            (($_ $f)
                                                (match-prop $f S:2:0
                                                    ((seq $a)
                                                        (prop $i O:3:0
                                                            (#u
                                                                (sequence ($o)
                                                                    (for-each $a ($_ $b)
                                                                        (seq-push $o #u $b)
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
                (move-rest-attrs $g $e)
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
            (prop $b S:1:0
                (#u
                    (regex def@0:0 ((1 $a)))
                )
            )
        )
        ",
        "
        (struct ($c)
            (prop $c O:1:0
                (#u $a)
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (struct ($c)
            (match-prop $b S:1:0
                (($_ $f)
                    (match-regex $f def@0:0
                        (((1 $a))
                            (prop $c O:1:0
                                (#u $a)
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $c $b)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_capture2() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0
                (#u
                    (regex def@0:0 ((1 $a)) ((2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (#u $a))
            (prop $d O:2:0 (#u $b))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $g)
                    (match-regex $g def@0:0
                        (((1 $a))
                            (prop $d O:1:0
                                (#u $a)
                            )
                        )
                        (((2 $b))
                            (prop $d O:2:0
                                (#u $b)
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $d $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_loop1() {
    let output = test_unify(
        // Contains a looping regex with two variations
        "
        (struct ($c)
            (prop $c S:1:0
                (#u
                    (regex-seq (@e) def@0:0 ((1 $a)) ((2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop $d O:1:0 (seq (@e) (iter #u $a)))
            (prop $d O:1:1 (seq (@e) (iter #u $b)))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (struct ($d)
            (match-prop $c S:1:0
                (($_ $e)
                    (let ($m (sequence ($k)))
                        (let ($l (sequence ($j)))
                            (match-regex-iter $e def@0:0
                                (((1 $a))
                                    (seq-push $l #u $a)
                                )
                                (((2 $b))
                                    (seq-push $m #u $b)
                                )
                            )
                            (prop $d O:1:0
                                (#u $l)
                            )
                            (prop $d O:1:1
                                (#u $m)
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $d $c)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_loop2() {
    let output = test_unify(
        "
        (struct ($d)
            (prop $d S:1:0
                (#u (regex-seq (@c) def@0:0 ((1 $a) (2 $b))))
            )
        )
        ",
        "
        (struct ($e)
            (prop $e S:1:0
                (seq (@c)
                    (iter #u
                        (struct ($f)
                            (prop $f S:2:0
                                (#u $a)
                            )
                            (prop $f S:2:1
                                (#u $b)
                            )
                        )
                    )
                )
            )
        )
        ",
    );

    let expected = indoc! {"
        |$d| (struct ($e)
            (match-prop $d S:1:0
                (($_ $c)
                    (let ($k (sequence ($j)))
                        (match-regex-iter $c def@0:0
                            (((1 $a) (2 $b))
                                (seq-push $k #u
                                    (struct ($f)
                                        (prop $f S:2:0
                                            (#u $a)
                                        )
                                        (prop $f S:2:1
                                            (#u $b)
                                        )
                                    )
                                )
                            )
                        )
                        (prop $e S:1:0
                            (#u $k)
                        )
                    )
                )
            )
            (move-rest-attrs $e $d)
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_loop3() {
    let output = test_unify(
        "
        (match-struct ($d)
            (prop $d S:1:2
                (#u (regex-seq (@c) def@0:43 ((1 $a)) ((2 $b))))
            )
        )
        ",
        "
        (struct ($e)
            (prop $e S:1:6
                (seq (@c)
                    (iter #u
                        (struct ($f)
                            (prop $f S:1:4
                                (#u $a)
                            )
                        )
                    )
                )
            )
            (prop $e S:1:7
                (seq (@c)
                    (iter #u
                        (struct ($g)
                            (prop $g S:1:4
                                (#u $b)
                            )
                        )
                    )
                )
            )
        )
        ",
    );

    let expected = indoc! {"
        |$d| (struct ($e)
            (match-prop $d S:1:2
                (($_ $c)
                    (let ($o (sequence ($m)))
                        (let ($n (sequence ($l)))
                            (match-regex-iter $c def@0:43
                                (((1 $a))
                                    (seq-push $n #u
                                        (struct ($f)
                                            (prop $f S:1:4
                                                (#u $a)
                                            )
                                        )
                                    )
                                )
                                (((2 $b))
                                    (seq-push $o #u
                                        (struct ($g)
                                            (prop $g S:1:4
                                                (#u $b)
                                            )
                                        )
                                    )
                                )
                            )
                            (prop $e S:1:6
                                (#u $n)
                            )
                            (prop $e S:1:7
                                (#u $o)
                            )
                        )
                    )
                )
            )
            (move-rest-attrs $e $d)
        )"
    };
    assert_eq!(expected, output);
}

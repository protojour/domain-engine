use indoc::indoc;
use ontol_compiler::hir_unify::test_api::test_unify;
use pretty_assertions::assert_eq;
use test_log::test;

#[test]
fn test_unify_no_op() {
    let output = test_unify("$a", "$a");
    assert_eq!("|$a| (block $a)", output);
}

#[test]
fn test_unify_expr1() {
    let output = test_unify("(- $a 10)", "$a");
    let expected = indoc! {"
        |$b| (block
            (let $a (+ $b 10)) $a)"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_expr2() {
    let output = test_unify("$a", "(+ $a 20)");
    assert_eq!("|$a| (block (+ $a 20))", output);
}

#[test]
fn test_unify_symmetric_exprs() {
    let output = test_unify("(- $a 10)", "(+ $a 20)");
    let expected = indoc! {"
        |$b| (block
            (let $a (+ $b 10)) (+ $a 20))"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_struct() {
    let output = test_unify(
        "
        (struct ($b)
            (prop! $b S:1:0 (#u $a))
        )
        ",
        "
        (struct ($c)
            (prop! $c S:1:1 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $a ($b S:1:0))
            (struct ($c)
                (prop! $c S:1:1
                    (#u $a)
                )
                (move-rest-attrs $c $b)
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_deep_structural_map() {
    let output = test_unify(
        "
        (struct ($f)
            (prop! $f S:1:9
                (#u $a)
            )
            (prop! $f S:1:10
                (#u $b)
            )
            (prop! $f S:1:11
                (#u
                    (struct ($g)
                        (prop! $g S:1:12
                            (#u $c)
                        )
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:2
                (#u $a)
            )
            (prop! $d O:1:6
                (#u
                    (struct ($e)
                        (prop! $e O:1:4
                            (#u $b)
                        )
                        (prop! $e O:1:5
                            (#u $c)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$f| (block
            (let-prop $_ $a ($f S:1:9))
            (let-prop $_ $b ($f S:1:10))
            (let-prop $_ $g ($f S:1:11))
            (let-prop $_ $c ($g S:1:12))
            (struct ($d)
                (prop! $d O:1:2
                    (#u $a)
                )
                (prop! $d O:1:6
                    (#u
                        (struct ($e)
                            (prop! $e O:1:4
                                (#u $b)
                            )
                            (prop! $e O:1:5
                                (#u $c)
                            )
                        )
                    )
                )
                (move-rest-attrs $d $f)
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
            (prop! $c S:1:0
                (#u
                    (struct ($d)
                        (prop! $d S:2:0 (#u $a))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:0 (#u $a))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $d ($c S:1:0))
            (let-prop $_ $a ($d S:2:0))
            (struct ($d)
                (prop! $d O:1:0
                    (#u $a)
                )
                (move-rest-attrs $d $c)
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
            (prop! $b S:1:0 (#u (map $a)))
        )
        ",
        "
        (struct ($c)
            (prop! $c S:1:1 (#u (map $a)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $a ($b S:1:0))
            (struct ($c)
                (prop! $c S:1:1
                    (#u (map $a))
                )
                (move-rest-attrs $c $b)
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
            (prop! $b S:1:0 (#u (- $a 10)))
        )
        ",
        "
        (struct ($c)
            (prop! $c S:1:1 (#u (+ $a 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $d ($b S:1:0))
            (let $a (+ $d 10))
            (struct ($c)
                (prop! $c S:1:1
                    (#u (+ $a 20))
                )
                (move-rest-attrs $c $b)
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
            (prop! $c S:1:0 (#u (- $a 10)))
            (prop! $c S:1:1 (#u (- $b 10)))
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:0 (#u (+ $a $b)))
            (prop! $d O:1:1 (#u (+ $a 20)))
            (prop! $d O:1:2 (#u (+ $b 20)))
        )
        ",
    );
    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $e ($c S:1:0))
            (let-prop $_ $f ($c S:1:1))
            (let $a (+ $e 10))
            (let $b (+ $f 10))
            (struct ($d)
                (prop! $d O:1:0
                    (#u (+ $a $b))
                )
                (prop! $d O:1:1
                    (#u (+ $a 20))
                )
                (prop! $d O:1:2
                    (#u (+ $b 20))
                )
                (move-rest-attrs $d $c)
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
            (prop! $c S:1:0 (#u (- $a 10)))
            (prop! $c S:1:1
                (#u
                    (struct ($e)
                        (prop! $e S:2:0 (#u (- $b 10)))
                    )
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:0 (#u (+ $a $b)))
            (prop! $d O:1:1 (#u (+ $a 20)))
            (prop! $d O:1:2 (#u (+ $b 20)))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $f ($c S:1:0))
            (let-prop $_ $e ($c S:1:1))
            (let-prop $_ $g ($e S:2:0))
            (let $a (+ $f 10))
            (let $b (+ $g 10))
            (struct ($d)
                (prop! $d O:1:0
                    (#u (+ $a $b))
                )
                (prop! $d O:1:1
                    (#u (+ $a 20))
                )
                (prop! $d O:1:2
                    (#u (+ $b 20))
                )
                (move-rest-attrs $d $c)
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
            (prop! $b S:1:0
                (#u (set (.. @d #u $a)))
            )
        )
        ",
        "
        (struct ($c)
            (prop! $c S:2:1
                (#u (set (.. @d #u $a)))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $d ($b S:1:0))
            (struct ($c)
                (prop! $c S:2:1
                    (#u
                        (make-seq ($e)
                            (for-each $d ($_ $a)
                                (insert $e #u $a)
                            )
                        )
                    )
                )
                (move-rest-attrs $c $b)
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_basic_seq_prop_element_iter_mix() {
    let output = test_unify(
        "
        (struct ($e)
            (prop! $e S:1:0 (#u $a))
            (prop! $e S:1:1 (#u (set (.. @d #u $b))))
            (prop! $e S:1:2 (#u $c))
        )
        ",
        "
        (struct ($f)
            (prop! $f S:2:1
                (#u
                    (set
                        (#u $a)
                        (.. @d #u $b)
                        (#u $c)
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$e| (block
            (let-prop $_ $a ($e S:1:0))
            (let-prop $_ $d ($e S:1:1))
            (let-prop $_ $c ($e S:1:2))
            (struct ($f)
                (prop! $f S:2:1
                    (#u
                        (make-seq ($g)
                            (insert $g #u $a)
                            (for-each $d ($_ $b)
                                (insert $g #u $b)
                            )
                            (insert $g #u $c)
                        )
                    )
                )
                (move-rest-attrs $f $e)
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
            (prop! $b S:1:0
                (#u
                    (set
                        (.. @f
                            #u
                            (struct ($c)
                                (prop! $c S:2:1
                                    (#u (set (.. @g #u (map $a))))
                                )
                            )
                        )
                    )
                )
            )
        )",
        "
        (struct ($d)
            (prop! $d O:1:0
                (#u
                    (set
                        (.. @f
                            #u
                            (struct ($e)
                                (prop! $e O:2:1
                                    (#u (set (.. @g #u (map $a))))
                                )
                            )
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $f ($b S:1:0))
            (struct ($d)
                (prop! $d O:1:0
                    (#u
                        (make-seq ($h)
                            (for-each $f ($_ $c)
                                (let-prop $_ $g ($c S:2:1))
                                (insert $h #u
                                    (struct ($e)
                                        (prop! $e O:2:1
                                            (#u
                                                (make-seq ($i)
                                                    (for-each $g ($_ $a)
                                                        (insert $i #u (map $a))
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
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_seq_prop_merge_rel_and_val_zwizzle() {
    let output = test_unify(
        "
        (struct ($e)
            (prop! $e S:1:0
                (#u
                    (set
                        (.. @f
                            (struct ($g)
                                (prop! $g S:2:0 (#u $a))
                                (prop! $g S:2:1 (#u $b))
                            )
                            (struct ($h)
                                (prop! $h S:3:0 (#u $c))
                                (prop! $h S:3:1 (#u $d))
                            )
                        )
                    )
                )
            )
        )",
        "
        (struct ($i)
            (prop! $i O:1:0
                (#u
                    (set
                        (.. @f
                            (struct ($j)
                                (prop! $j O:2:0 (#u $a))
                                (prop! $j O:2:1 (#u $c))
                            )
                            (struct ($k)
                                (prop! $k O:3:0 (#u $b))
                                (prop! $k O:3:1 (#u $d))
                            )
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {"
        |$e| (block
            (let-prop $_ $f ($e S:1:0))
            (struct ($i)
                (prop! $i O:1:0
                    (#u
                        (make-seq ($l)
                            (for-each $f ($g $h)
                                (let-prop $_ $a ($g S:2:0))
                                (let-prop $_ $b ($g S:2:1))
                                (let-prop $_ $c ($h S:3:0))
                                (let-prop $_ $d ($h S:3:1))
                                (insert $l
                                    (struct ($j)
                                        (prop! $j O:2:0
                                            (#u $a)
                                        )
                                        (prop! $j O:2:1
                                            (#u $c)
                                        )
                                    )
                                    (struct ($k)
                                        (prop! $k O:3:0
                                            (#u $b)
                                        )
                                        (prop! $k O:3:1
                                            (#u $d)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $i $e)
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
            (prop $b S:1:0
                (#u (set (.. @d #u $a)))
            )
        )
        ",
        "
        (struct ($c)
            (prop $c S:2:1
                (#u (set (.. @d #u $a)))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop-default $_ $d ($b S:1:0) #u #u)
            (struct ($c)
                (prop $c S:2:1
                    (#u
                        (make-seq ($e)
                            (for-each $d ($_ $a)
                                (insert $e #u $a)
                            )
                        )
                    )
                )
                (move-rest-attrs $c $b)
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
            (prop! $c S:1:0
                (#u
                    (set
                        (.. @d
                            #u
                            (struct ($e)
                                (prop! $e S:3:2
                                    (#u $a)
                                )
                            )
                        )
                    )
                )
            )
            (prop! $c S:2:1 (#u $b))
        )
        ",
        "
        (set
            (.. @d
                #u
                (struct ($f)
                    (prop! $f O:1:0
                        (#u $a)
                    )
                    (prop! $f O:2:1
                        (#u $b)
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $d ($c S:1:0))
            (let-prop $_ $b ($c S:2:1))
            (make-seq ($g)
                (for-each $d ($_ $e)
                    (let-prop $_ $a ($e S:3:2))
                    (insert $g #u
                        (struct ($f)
                            (prop! $f O:1:0
                                (#u $a)
                            )
                            (prop! $f O:2:1
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
fn test_unify_flat_map2() {
    let output = test_unify(
        // The difference with test_unify_flat_map1 is that the non-seq-prop appears _first_ here:
        "
        (struct ($d)
            (prop! $d S:1:2 (#u $a))
            (prop! $d S:1:5
                (#u
                    (set
                        (.. @c
                            #u
                            (struct ($e)
                                (prop! $e S:1:4
                                    (#u $b)
                                )
                            )
                        )
                    )
                )
            )
        )
        ",
        "
        (set
            (.. @c
                #u
                (struct ($f)
                    (prop! $f S:1:7
                        (#u $a)
                    )
                    (prop! $f S:1:8
                        (#u $b)
                    )
                )
            )
        )
        ",
    );

    let expected = indoc! {"
        |$d| (block
            (let-prop $_ $a ($d S:1:2))
            (let-prop $_ $c ($d S:1:5))
            (make-seq ($g)
                (for-each $c ($_ $e)
                    (let-prop $_ $b ($e S:1:4))
                    (insert $g #u
                        (struct ($f)
                            (prop! $f S:1:7
                                (#u $a)
                            )
                            (prop! $f S:1:8
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
        |$b| (block
            (let-prop $_ $a ($b S:1:0))
            (struct ($c)
                (catch (@d)
                    (try? @d $a)
                    (prop? $c O:2:1
                        (#u (map $a))
                    )
                )
                (move-rest-attrs $c $b)
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
        |$b| (block
            (let-prop $_ $c ($b S:1:0))
            (struct ($d)
                (prop? $d O:1:0
                    (#u
                        (struct ($e)
                            (catch (@f)
                                (try? @f $c)
                                (let-prop? @f $_ $a ($c S:2:1))
                                (prop? $e O:2:1
                                    (#u $a)
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $d $b)
            )
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
        |$c| (block
            (let-prop $_ $d ($c S:1:0))
            (let-prop $_ $e ($c S:1:1))
            (struct ($f)
                (prop? $f O:1:0
                    (#u
                        (struct ($g)
                            (catch (@h)
                                (try? @h $d)
                                (let-prop? @h $_ $a ($d S:2:0))
                                (prop? $g O:2:0
                                    (#u $a)
                                )
                            )
                            (catch (@i)
                                (try? @i $e)
                                (let-prop? @i $_ $b ($e S:2:1))
                                (prop? $g O:2:1
                                    (#u $b)
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $f $c)
            )
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
            (prop! $c S:1:2 (#u $h))
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
                        (prop! $g O:2:2
                            (#u $h)
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $d ($c S:1:0))
            (let-prop $_ $e ($c S:1:1))
            (let-prop $_ $h ($c S:1:2))
            (struct ($f)
                (prop? $f O:1:0
                    (#u
                        (struct ($g)
                            (catch (@i)
                                (try? @i $d)
                                (let-prop? @i $_ $a ($d S:2:0))
                                (prop? $g O:2:0
                                    (#u $a)
                                )
                            )
                            (catch (@j)
                                (try? @j $e)
                                (let-prop? @j $_ $b ($e S:2:1))
                                (prop? $g O:2:1
                                    (#u $b)
                                )
                            )
                            (prop! $g O:2:2
                                (#u $h)
                            )
                        )
                    )
                )
                (move-rest-attrs $f $c)
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
        |$c| (block
            (let-prop $a $b ($c S:1:7))
            (struct ($d)
                (catch (@e)
                    (try? @e $a)
                    (try? @e $b)
                    (prop? $d S:1:7
                        (#u (+ $a $b))
                    )
                )
                (move-rest-attrs $d $c)
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
            (prop? $c S:1:0
                (
                    (struct ($d) (prop! $d S:2:0 (#u $a)))
                    (struct ($e) (prop! $e S:2:1 (#u $b)))
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
        |$c| (block
            (let-prop $d $e ($c S:1:0))
            (struct ($f)
                (catch (@g)
                    (try? @g $d)
                    (let-prop? @g $_ $a ($d S:2:0))
                    (try? @g $e)
                    (let-prop? @g $_ $b ($e S:2:1))
                    (prop? $f O:1:0
                        (#u (+ $a $b))
                    )
                )
                (move-rest-attrs $f $c)
            )
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
        (prop! $c S:1:0 (#u (+ $b $c)))
        (prop! $c S:2:1 (#u (+ $a $b)))
        (prop! $c S:3:2 (#u $a))
    )";

    pub const EXPR_2: &str = "
    (struct ($f)
        (prop! $f O:1:0 (#u $a))
        (prop! $f O:2:1 (#u $b))
        (prop! $f O:3:2 (#u $c))
    )
    ";

    #[test]
    fn forwards() {
        let output = test_unify(EXPR_1, EXPR_2);
        let expected = indoc! {"
            |$c| (block
                (let-prop $_ $g ($c S:1:0))
                (let-prop $_ $h ($c S:2:1))
                (let-prop $_ $a ($c S:3:2))
                (let $b (- $a $h))
                (let $c (- $b $g))
                (struct ($f)
                    (prop! $f O:1:0
                        (#u $a)
                    )
                    (prop! $f O:2:1
                        (#u $b)
                    )
                    (prop! $f O:3:2
                        (#u $c)
                    )
                    (move-rest-attrs $f $c)
                )
            )"
        };
        assert_eq!(expected, output);
    }

    #[test]
    fn backwards() {
        let output = test_unify(EXPR_2, EXPR_1);
        let expected = indoc! {"
            |$f| (block
                (let-prop $_ $a ($f O:1:0))
                (let-prop $_ $b ($f O:2:1))
                (let-prop $_ $c ($f O:3:2))
                (struct ($c)
                    (prop! $c S:1:0
                        (#u (+ $b $c))
                    )
                    (prop! $c S:2:1
                        (#u (+ $a $b))
                    )
                    (prop! $c S:3:2
                        (#u $a)
                    )
                    (move-rest-attrs $c $f)
                )
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
            (prop! $c S:1:0 (#u #u))
            (prop! $c S:1:1 (#u (set (.. @a #u $b))))
        )",
        "(struct ($d)
            (prop! $d O:1:0
                (#u
                    (struct ($e)
                        (prop! $e O:2:0 (#u #u))
                        (prop! $e O:2:1 (#u (set (.. @a #u $b))))
                    )
                )
            )
        )",
    );

    #[test]
    fn forward() {
        let output = test_unify(ARMS.0, ARMS.1);
        let expected = indoc! {"
            |$c| (block
                (let-prop $_ $_ ($c S:1:0))
                (let-prop $_ $a ($c S:1:1))
                (struct ($d)
                    (prop! $d O:1:0
                        (#u
                            (struct ($e)
                                (prop! $e O:2:0
                                    (#u #u)
                                )
                                (prop! $e O:2:1
                                    (#u
                                        (make-seq ($f)
                                            (for-each $a ($_ $b)
                                                (insert $f #u $b)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                    (move-rest-attrs $d $c)
                )
            )"
        };
        assert_eq!(expected, output);
    }

    #[test]
    fn backward() {
        let output = test_unify(ARMS.1, ARMS.0);
        let expected = indoc! {"
            |$d| (block
                (let-prop $_ $e ($d O:1:0))
                (let-prop $_ $_ ($e O:2:0))
                (let-prop $_ $a ($e O:2:1))
                (struct ($c)
                    (prop! $c S:1:0
                        (#u #u)
                    )
                    (prop! $c S:1:1
                        (#u
                            (make-seq ($f)
                                (for-each $a ($_ $b)
                                    (insert $f #u $b)
                                )
                            )
                        )
                    )
                    (move-rest-attrs $c $d)
                )
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
            (prop! $e S:1:0
                (#u
                    (struct ($f)
                        (prop! $f S:2:0
                            (#u (set (.. @a #u $b)))
                        )
                    )
                )
            )
            (prop! $e S:1:1
                (#u (set (.. @c #u $d)))
            )
        )",
        // Note: The expr prop O:1:0 itself does not depend on anything in scope.
        // So it's constant in this sense, but each _child_ need to _clone_ the original scope.
        "(struct ($g)
            (prop! $g O:1:0
                (#u
                    (struct ($h)
                        (prop! $h O:2:0
                            (#u
                                (struct ($i)
                                    (prop! $i O:3:0
                                        (#u (set (.. @a #u $b)))
                                    )
                                )
                            )
                        )
                        (prop! $h O:2:1
                            (#u (set (.. @c #u $d)))
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
            |$e| (block
                (let-prop $_ $f ($e S:1:0))
                (let-prop $_ $a ($f S:2:0))
                (let-prop $_ $c ($e S:1:1))
                (struct ($g)
                    (prop! $g O:1:0
                        (#u
                            (struct ($h)
                                (prop! $h O:2:0
                                    (#u
                                        (struct ($i)
                                            (prop! $i O:3:0
                                                (#u
                                                    (make-seq ($j)
                                                        (for-each $a ($_ $b)
                                                            (insert $j #u $b)
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                                (prop! $h O:2:1
                                    (#u
                                        (make-seq ($k)
                                            (for-each $c ($_ $d)
                                                (insert $k #u $d)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                    (move-rest-attrs $g $e)
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
            (prop! $b S:1:0
                (#u
                    (regex def@0:0 ((1 $a)))
                )
            )
        )
        ",
        "
        (struct ($c)
            (prop! $c O:1:0
                (#u $a)
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $d ($b S:1:0))
            (let-regex ((1 $a)) def@0:0 $d)
            (struct ($c)
                (catch (@e)
                    (try? @e $a)
                    (prop! $c O:1:0
                        (#u $a)
                    )
                )
                (move-rest-attrs $c $b)
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
            (prop! $c S:1:0
                (#u
                    (regex def@0:0 ((1 $a)) ((2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:0 (#u $a))
            (prop! $d O:2:0 (#u $b))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $e ($c S:1:0))
            (let-regex ((1 $a)) ((2 $b)) def@0:0 $e)
            (struct ($d)
                (catch (@f)
                    (try? @f $a)
                    (prop! $d O:1:0
                        (#u $a)
                    )
                )
                (catch (@g)
                    (try? @g $b)
                    (prop! $d O:2:0
                        (#u $b)
                    )
                )
                (move-rest-attrs $d $c)
            )
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
            (prop! $c S:1:0
                (#u
                    (regex-seq (@e) def@0:0 ((1 $a)) ((2 $b)))
                )
            )
        )
        ",
        "
        (struct ($d)
            (prop! $d O:1:0 (#u (set (.. @e #u $a))))
            (prop! $d O:1:1 (#u (set (.. @e #u $b))))
        )
        ",
    );

    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $f ($c S:1:0))
            (let-regex-iter $e ((1 $a)) ((2 $b)) def@0:0 $f)
            (struct ($d)
                (prop! $d O:1:0
                    (#u
                        (make-seq ($g)
                            (for-each $e ($_ $h)
                                (catch (@i)
                                    (let-tup? @i ($a $_) $h)
                                    (insert $g #u $a)
                                )
                            )
                        )
                    )
                )
                (prop! $d O:1:1
                    (#u
                        (make-seq ($j)
                            (for-each $e ($_ $k)
                                (catch (@l)
                                    (let-tup? @l ($_ $b) $k)
                                    (insert $j #u $b)
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $d $c)
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_loop2() {
    let output = test_unify(
        "
        (struct ($d)
            (prop! $d S:1:0
                (#u (regex-seq (@c) def@0:0 ((1 $a) (2 $b))))
            )
        )
        ",
        "
        (struct ($e)
            (prop! $e S:1:0
                (#u
                    (set
                        (.. @c
                            #u
                            (struct ($f)
                                (prop! $f S:2:0
                                    (#u $a)
                                )
                                (prop! $f S:2:1
                                    (#u $b)
                                )
                            )
                        )
                    )
                )
            )
        )
        ",
    );

    let expected = indoc! {"
        |$d| (block
            (let-prop $_ $g ($d S:1:0))
            (let-regex-iter $c ((1 $a) (2 $b)) def@0:0 $g)
            (struct ($e)
                (prop! $e S:1:0
                    (#u
                        (make-seq ($h)
                            (for-each $c ($_ $i)
                                (catch (@j)
                                    (let-tup? @j ($a $b) $i)
                                    (insert $h #u
                                        (struct ($f)
                                            (prop! $f S:2:0
                                                (#u $a)
                                            )
                                            (prop! $f S:2:1
                                                (#u $b)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $e $d)
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_regex_loop3() {
    let output = test_unify(
        "
        (match-struct ($d)
            (prop! $d S:1:2
                (#u (regex-seq (@c) def@0:43 ((1 $a)) ((2 $b))))
            )
        )
        ",
        "
        (struct ($e)
            (prop! $e S:1:6
                (#u
                    (set
                        (.. @c
                            #u
                            (struct ($f)
                                (prop! $f S:1:4
                                    (#u $a)
                                )
                            )
                        )
                    )
                )
            )
            (prop! $e S:1:7
                (#u
                    (set
                        (.. @c
                            #u
                            (struct ($g)
                                (prop! $g S:1:4
                                    (#u $b)
                                )
                            )
                        )
                    )
                )
            )
        )",
    );

    let expected = indoc! {"
        |$d| (block
            (let-prop $_ $h ($d S:1:2))
            (let-regex-iter $c ((1 $a)) ((2 $b)) def@0:43 $h)
            (struct ($e)
                (prop! $e S:1:6
                    (#u
                        (make-seq ($i)
                            (for-each $c ($_ $j)
                                (catch (@k)
                                    (let-tup? @k ($a $_) $j)
                                    (insert $i #u
                                        (struct ($f)
                                            (prop! $f S:1:4
                                                (#u $a)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (prop! $e S:1:7
                    (#u
                        (make-seq ($l)
                            (for-each $c ($_ $m)
                                (catch (@n)
                                    (let-tup? @n ($_ $b) $m)
                                    (insert $l #u
                                        (struct ($g)
                                            (prop! $g S:1:4
                                                (#u $b)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (move-rest-attrs $e $d)
            )
        )"
    };
    assert_eq!(expected, output);
}

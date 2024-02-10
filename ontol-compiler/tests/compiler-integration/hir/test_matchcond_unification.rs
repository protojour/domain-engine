use indoc::indoc;
use ontol_compiler::hir_unify::test_api::test_unify;
use pretty_assertions::assert_eq;
use test_log::test;

#[test]
fn test_unify_matchcond_empty() {
    let output = test_unify(
        "
        (struct ($b))
        ",
        "
        (set
            (.. @a
                #u
                (match-struct ($c))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (match-struct ($c)
                (push-cond-clause $c
                    (root '$c)
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_matchcond_single_prop() {
    let output = test_unify(
        "
        (struct ($b)
            (prop! $b S:1:0 (#u $a))
        )
        ",
        "
        (set
            (.. @d
                #u
                (match-struct ($c)
                    (prop! $c O:1:0 (#u $a))
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (block
            (let-prop $_ $a ($b S:1:0))
            (match-struct ($c)
                (push-cond-clause $c
                    (root '$c)
                )
                (push-cond-clause $c
                    (attr '$c O:1:0 (_ $a))
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_matchcond_struct_in_struct() {
    let output = test_unify(
        "
        (struct ($c)
            (prop! $c S:1:0 (#u $a))
            (prop! $c S:1:1 (#u $b))
        )
        ",
        "
        (set
            (.. @f
                #u
                (match-struct ($d)
                    (prop! $d O:1:0
                        (#u
                            (struct ($e)
                                (prop! $e O:2:0 ($a $b))
                            )
                        )
                    )
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$c| (block
            (let-prop $_ $a ($c S:1:0))
            (let-prop $_ $b ($c S:1:1))
            (match-struct ($d)
                (push-cond-clause $d
                    (root '$d)
                )
                (push-cond-clause $d
                    (attr '$d O:1:0 (_ '$e))
                )
                (push-cond-clause $d
                    (attr '$e O:2:0 ($a $b))
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
// BUG: Unfinished design work
// #[should_panic = "not yet implemented"]
fn test_unify_matchcond_cartesian_set() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0
                (#u (set (.. @e #u $a)))
            )
            (prop $c S:1:1
                (#u (set (.. @f #u $b)))
            )
        )
        ",
        "
        (set
            (.. @g
                #u
                (match-struct ($d)
                    (prop $d O:1:0
                        (element-in
                            (set
                                (.. @e
                                    #u
                                    (struct ($q)
                                        (prop $q O:2:0 (#u $a))
                                    )
                                )
                                (.. @f
                                    #u
                                    (struct ($r)
                                        (prop $r O:2:0 (#u $b))
                                    )
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
        |$c| (match-struct ($d)
            (push-cond-clause $d
                (root '$d)
            )
            (match-prop $c S:1:0
                ((.. $e)
                    (match-prop $c S:1:1
                        ((.. $f)
                            (push-cond-clause $d
                                (attr '$d O:1:0 (_ '$e))
                            )
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

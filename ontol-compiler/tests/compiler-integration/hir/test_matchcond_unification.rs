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
            (.. (@a)
                #u
                (match-struct ($c))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (match-struct ($c)
            (push-cond-clause $c
                (root '$c)
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
            (prop $b S:1:0 (#u $a))
        )
        ",
        "
        (set
            (.. (@d)
                #u
                (match-struct ($c)
                    (prop $c O:1:0 (#u $a))
                )
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (match-struct ($c)
            (push-cond-clause $c
                (root '$c)
            )
            (match-prop $b S:1:0
                (($_ $a)
                    (push-cond-clause $c
                        (attr '$c O:1:0 (_ $a))
                    )
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
            (prop $c S:1:0 (#u $a))
            (prop $c S:1:1 (#u $b))
        )
        ",
        "
        (set
            (.. (@f)
                #u
                (match-struct ($d)
                    (prop $d O:1:0
                        (#u
                            (struct ($e)
                                (prop $e O:2:0 ($a $b))
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
                (($_ $a)
                    (match-prop $c S:1:1
                        (($_ $b)
                            (begin
                                (push-cond-clause $d
                                    (attr '$d O:1:0 (_ '$e))
                                )
                                (push-cond-clause $d
                                    (attr '$e O:2:0 ($a $b))
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
// BUG: Unfinished design work
#[should_panic]
fn test_unify_matchcond_cartesian_set() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:1:0
                (.. (@e) (iter #u $a))
            )
            (prop $c S:1:1
                (.. (@f) (iter #u $b))
            )
        )
        ",
        "
        (set
            (.. (@g)
                #u
                (match-struct ($d)
                    (prop $d O:1:0
                        (element-in
                            (set-of
                                (iter (@f)
                                    #u
                                    (struct ($q)
                                        (prop $q O:2:0 #u $a)
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

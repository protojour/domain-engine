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
        (decl-set (@a) #u
            (match-struct ($c))
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
        (decl-set (@d) #u
            (match-struct ($c)
                (prop $c O:1:0 (#u $a))
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
        (decl-set (@f) #u
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
                (seq (@e) (iter #u $a))
            )
            (prop $c S:1:1
                (seq (@f) (iter #u $b))
            )
        )
        ",
        "
        (decl-set (@g) #u
            (match-struct ($d)
                (prop $d O:1:0
                    (#u /* what goes here? */)
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
                ((seq $e)
                    (match-prop $c S:1:1
                        ((seq $f)
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

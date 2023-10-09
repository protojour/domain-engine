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
        (decl-seq (@a) #u
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
            (prop $b S:0:0 (#u $a))
        )
        ",
        "
        (decl-seq (@d) #u
            (match-struct ($c)
                (prop $c O:0:0 (#u $a))
            )
        )
        ",
    );
    let expected = indoc! {"
        |$b| (match-struct ($c)
            (push-cond-clause $c
                (root '$c)
            )
            (match-prop $b S:0:0
                (($_ $a)
                    (push-cond-clause $c
                        (attr '$c O:0:0 (_ $a))
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_matchcond_complex() {
    let output = test_unify(
        "
        (struct ($c)
            (prop $c S:0:0 (#u $a))
            (prop $c S:0:1 (#u $b))
        )
        ",
        "
        (decl-seq (@f) #u
            (match-struct ($d)
                (prop $d O:0:0
                    (#u
                        (struct ($e)
                            (prop $e O:1:0 ($a $b))
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
        )"
    };
    assert_eq!(expected, output);
}

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
            (match-struct ($d)
                (let-cond-var $c $d)
                (push-cond-clause $d
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
            (match-struct ($e)
                (let-cond-var $c $e)
                (push-cond-clause $e
                    (root '$c)
                )
                (push-cond-clause $e
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
            (match-struct ($g)
                (let-cond-var $d $g)
                (push-cond-clause $g
                    (root '$d)
                )
                (let-cond-var $e $g)
                (push-cond-clause $g
                    (attr '$d O:1:0 (_ '$e))
                )
                (push-cond-clause $g
                    (attr '$e O:2:0 ($a $b))
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

#[test]
fn test_unify_matchcond_element_in() {
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
        |$c| (block
            (let-prop-default $_ $e ($c S:1:0) #u #u)
            (let-prop-default $_ $f ($c S:1:1) #u #u)
            (match-struct ($s)
                (let-cond-var $d $s)
                (push-cond-clause $s
                    (root '$d)
                )
                (let-cond-var $t $s)
                (push-cond-clause $s
                    (match-prop '$d O:1:0 element-in '$t))
                (for-each $e ($_ $a)
                    (let-cond-var $q $s)
                    (push-cond-clause $s
                        (member '$t (_ '$q))
                    )
                    (catch (@u)
                        (let? @u $v $a)
                        (push-cond-clause $s
                            (attr '$q O:2:0 (_ $v))
                        )
                    )
                )
                (for-each $f ($_ $b)
                    (let-cond-var $r $s)
                    (push-cond-clause $s
                        (member '$t (_ '$r))
                    )
                    (catch (@w)
                        (let? @w $x $b)
                        (push-cond-clause $s
                            (attr '$r O:2:0 (_ $x))
                        )
                    )
                )
            )
        )"
    };
    assert_eq!(expected, output);
}

use crate::TestCompile;
use test_log::test;

#[test]
fn lex_error() {
    // BUG: chumsky bug
    "( ;; ERROR parse error".compile_ok(|_| {});
}

#[test]
fn parse_error1() {
    "() ;; ERROR parse error: expected keyword".compile_fail()
}

#[test]
fn rel_type_not_found() {
    "
    (type! foo)
    (rel! (foo) bar
        (baz) ;; ERROR type not found
    )
    "
    .compile_fail()
}

#[test]
fn rel_duplicate_anonymous_relation() {
    "
    (type! foo)
    (type! bar)
    (rel! ;; ERROR unit type `bar` cannot be part of a union
        (foo) _ (bar)
    )
    (rel! ;; ERROR duplicate anonymous relation
        (foo) _ (bar)
    )
    "
    .compile_fail()
}

#[test]
fn rel_mix_anonymous_and_named() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) _ (bar))
    (rel! ;; ERROR cannot mix named and anonymous relations on the same type
        (foo) foobar (bar)
    )
    "
    .compile_fail()
}

#[test]
fn map_union_unit_type() {
    "
    (type! foo)
    (type! bar)
    (type! u)
    (rel! (u) _ (foo)) ;; ERROR unit type `foo` cannot be part of a union
    (rel! (u) _ (bar)) ;; ERROR unit type `bar` cannot be part of a union
    "
    .compile_fail()
}

#[test]
fn map_union_missing_discriminator() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) a "constant")
    (rel! (bar) b (string))
    (type! u)
    (rel! (u) _ (foo))
    (rel! (u) _ (bar)) ;; ERROR cannot discriminate type
    "#
    .compile_fail()
}

#[test]
fn map_union_non_uniform_discriminators() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) a "constant")
    (rel! (bar) b "other-constant")
    (type! u) ;; ERROR no uniform discriminator found for union variants
    (rel! (u) _ (foo))
    (rel! (u) _ (bar))
    "#
    .compile_fail()
}

#[test]
fn eq_undeclared_variable() {
    "
    (eq! ()
        :x ;; ERROR undeclared variable
        42
    )
    "
    .compile_fail()
}

#[test]
fn eq_obj_non_domain_type_and_unit_type() {
    "
    (type! foo)
    (eq! (:x)
        (obj!
            number ;; ERROR expected domain type
        )
        (obj! ;; ERROR no properties expected
            foo (prop :x)
        )
    )
    "
    .compile_fail()
}

#[test]
fn eq_attribute_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) prop (bar))
    (rel! (bar) _ (number))
    (eq! (:x)
        (obj! ;; ERROR missing property `prop`
            foo
            (_ :x) ;; ERROR expected named property
        )
        (obj! bar) ;; ERROR expected anonymous property
    )
    "
    .compile_fail()
}

#[test]
fn eq_duplicate_unknown_property() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) a (bar))
    (eq! (:x)
        (obj! foo
            (a :x)
            (a :x) ;; ERROR duplicate property
            (b :x) ;; ERROR unknown property
        )
        (obj! bar)
    )
    "
    .compile_fail()
}

#[test]
fn eq_type_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) prop (string))
    (rel! (bar) prop (number))
    (eq! (:x)
        (obj! foo (prop :x))
        (obj! bar
            (prop
                :x ;; ERROR type mismatch: expected `number`, found `string`
            )
        )
    )
    "
    .compile_fail()
}

#[test]
fn eq_type_mismatch_in_func() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) prop (string))
    (rel! (bar) prop (number))
    (eq! (:x)
        (obj! foo (prop :x))
        (obj! bar
            (prop
                (*
                    :x ;; ERROR type mismatch: expected `number`, found `string`
                    2
                )
            )
        )
    )
    "
    .compile_fail()
}

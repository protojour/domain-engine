use crate::TestCompile;

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
    (rel! (foo) _ (bar))
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
fn eq_obj_non_domain_type_and_unit_type() {
    "
    (type! foo)
    (eq! ()
        (obj!
            number ;; ERROR expected domain type
        )
        (obj! ;; ERROR no attributes expected
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
        (obj! foo
            (_ :x)
        )
        (obj! bar) ;; ERROR expected anonymous attribute
    )
    "
    .compile_fail()
}

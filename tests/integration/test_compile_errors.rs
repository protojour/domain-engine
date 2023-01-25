use crate::TestCompile;

#[test]
fn lex_error() {
    // BUG: chumsky bug
    "( ;; ERROR parse error".compile_ok(|_| {});
}

#[test]
fn parse_error1() {
    "() ;; ERROR parse error".compile_fail()
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
fn duplicate_anonymous_relation() {
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
fn mix_anonymous_and_named() {
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

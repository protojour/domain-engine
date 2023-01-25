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
fn goood() {
    "
    (type! foo)
    (rel! ;; ERROR type not found
        (foo)
        bar
        (baz)
    )
    "
    .compile_fail()
}

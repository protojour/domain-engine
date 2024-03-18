use ontol_test_utils::{expect_eq, TestCompile};

#[test]
fn error_spans_are_correct_projected_from_regex_syntax_errors() {
    r"
    def lol ()
    rel () /abc\/(?P<42>.)/: lol // ERROR invalid regex: invalid capture group character
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "4");
    })
}

#[test]
fn regex_error_span1() {
    r"
    def a (rel .'a': text)
    def b ()
    map(
        a(
            'a': /(?<dupe>\w+) (?<dupe>\w+)!/ // ERROR invalid regex: duplicate capture group name
        ),
        b()
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "dupe");
    })
}

#[test]
fn regex_error_span2() {
    r"
    def a ()
    def b (rel .'b': text)
    map(
        a(),
        b(
            'b': /(?<bad_var>\w+)!/ // ERROR unbound variable
        )
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "bad_var");
    })
}

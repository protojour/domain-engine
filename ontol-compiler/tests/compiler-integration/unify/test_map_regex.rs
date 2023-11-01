use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
fn test_map_regex_duplex1() {
    r#"
    def(pub) foo {
        rel .'input': text
    }
    def(pub) bar {
        rel .'first': text
        rel .'second': text
    }
    map {
        foo {
            'input': /(?<one>\w+) (?<two>\w+)!/
        }
        bar {
            'first': one
            'second': two
        }
    }
    "#
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "input": "Hello world!"}),
            json!({ "first": "Hello", "second": "world"}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "first": "Get", "second": "outtahere"}),
            json!({ "input": "Get outtahere!"}),
        );
    });
}

#[test]
fn test_map_regex_alternation1() {
    r#"
    def(pub) foo {
        rel .'input': text
    }
    def capture {
        rel .'value': text
    }
    def(pub) bar {
        rel .'first'?: text
        rel .'second'?: text
    }
    map {
        foo match {
            'input': /(first=(?<first>\w+))|(second=(?<second>\w+))!/
        }
        bar {
            'first'?: first
            'second'?: second
        }
    }
    "#
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "input": "first=FOO!"}),
            json!({ "first": "FOO" }),
        );
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "input": "second=BAR!"}),
            json!({ "second": "BAR" }),
        );
    });
}

// BUG: Unreachable code in flat_unifier when removing `match`. Should show proper error.
#[test]
fn test_map_regex_loop_pattern() {
    r#"
    def(pub) in {
        rel .'input': text
    }
    def capture {
        rel .'first': text
        rel .'second': text
    }
    def(pub) out {
        rel .'captures': [capture]
    }
    map {
        in match {
            'input': [
                ../(?<one>\w+) (?<two>\w+),?/
            ]
        }
        out {
            'captures': [
                ..capture {
                    'first': one
                    'second': two
                }
            ]
        }
    }
    "#
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("in", "out"),
            json!({ "input": "" }),
            json!({ "captures": [] }),
        );
        test.mapper().assert_map_eq(
            ("in", "out"),
            json!({ "input": "a b, c d"}),
            json!({ "captures": [
                { "first": "a", "second": "b" },
                { "first": "c", "second": "d" },
            ] }),
        );
    });
}

#[test]
fn test_map_regex_loop_alternation() {
    r#"
    def(pub) in {
        rel .'input': text
    }
    def capture {
        rel .'value': text
    }
    def(pub) out {
        rel .'foo': [capture]
        rel .'bar': [capture]
    }
    map {
        in match {
            'input': [
                ../FOO=(?<foo>\w+)|BAR=(?<bar>\w+)/
            ]
        }
        out {
            'foo': [..capture { 'value': foo }]
            'bar': [..capture { 'value': bar }]
        }
    }
    "#
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("in", "out"),
            json!({ "input": "" }),
            json!({ "foo": [], "bar": [] }),
        );
        test.mapper().assert_map_eq(
            ("in", "out"),
            json!({ "input": "junkjunk FOO=1 BAR=2 FOO=3 junkjunkjunk" }),
            json!({
                "foo": [
                    { "value": "1" },
                    { "value": "3" },
                ],
                "bar": [
                    { "value": "2" },
                ]
            }),
        );
    });
}

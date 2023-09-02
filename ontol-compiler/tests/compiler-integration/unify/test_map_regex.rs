use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
fn test_map_regex_duplex1() {
    r#"
    pub def foo {
        rel .'input': string
    }
    pub def bar {
        rel .'first': string
        rel .'second': string
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
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "input": "Hello world!"}),
            json!({ "first": "Hello", "second": "world"}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "first": "Get", "second": "outtahere"}),
            json!({ "input": "Get outtahere!"}),
        );
    });
}

#[test]
// BUG: Have to treat regexes as property sets in dependency tree, and flatten things
#[should_panic = "Regex did not match"]
fn test_map_regex_alternation1() {
    r#"
    pub def foo {
        rel .'input': string
    }
    def capture {
        rel .'value': string
    }
    pub def bar {
        rel .'first'?: string
        rel .'second'?: string
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
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "input": "first=FOO!"}),
            json!({ "first": "FOO" }),
        );
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "input": "second=FOO!"}),
            json!({ "second": "BAR" }),
        );
    });
}

// BUG: This should make an iteration group consisting of (one, two)
#[test]
#[should_panic = "looping regex"]
fn test_map_regex_loop_pattern() {
    r#"
    pub def in {
        rel .'input': string
    }
    def capture {
        rel .'first': string
        rel .'second': string
    }
    pub def out {
        rel .'captures': [capture]
    }
    map {
        in {
            'input': [ // ERROR type mismatch: expected `[string]`, found `string`
                ../(?<one>\w+) (?<two>\w+),/ // ERROR Incompatible literal
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
    .compile_fail();
}

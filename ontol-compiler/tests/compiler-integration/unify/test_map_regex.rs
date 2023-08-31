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

// BUG: This should make an iteration group consisting of (one, two)
#[test]
fn test_map_regex_repetition1() {
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
            'input': /((?<one>\w+) (?<two>\w+),*)!/
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

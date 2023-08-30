use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
fn test_map_regex_capture1() {
    r#"
    pub def foo {
        rel .'input': string
    }
    pub def bar {
        rel .'first': string
        rel .'second': string
    }
    map {
        // TODO: Backwards mapping
        foo match {
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
    });
}

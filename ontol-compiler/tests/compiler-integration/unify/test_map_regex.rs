use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
fn map_regex_capture1() {
    r#"
    pub def foo {
        rel .'i': string
    }
    pub def bar {
        rel .'o': string
    }
    map {
        // TODO: Backwards mapping
        foo match {
            'i': /Hello (?P<what>\w+)!/
        }
        bar {
            'o': what
        }
    }
    "#
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "i": "Hello world!"}),
            json!({ "o": "world"}),
        );
    });
}

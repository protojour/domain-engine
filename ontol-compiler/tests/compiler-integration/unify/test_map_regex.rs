use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
fn map_regex_capture1() {
    "
    pub def foo {
        rel .'f': string
    }
    pub def bar {
        rel .'b': string
    }
    map {
        foo { 'f': // }
        bar { 'b': x }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    });
}

use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

#[test]
#[should_panic = "not yet implemented"]
fn map_regex_capture1() {
    r#"
    pub def foo {
        rel .'i': string
    }
    pub def bar {
        rel .'o': string
    }
    map {
        foo {
            'i': /Hello (?P<name>\w+)!/
        }
        bar {
            'o': name
        }
    }
    "#
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "f": "Hello world!"}),
            json!({ "b": "world"}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "b": "world"}),
            json!({ "f": "Hello world!"}),
        );
    });
}

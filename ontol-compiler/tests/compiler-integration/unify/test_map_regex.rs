use ontol_test_utils::TestCompile;
use test_log::test;

// BUG: Make it work
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
        foo {
            'i': /Hello (?P<name>\w+)!/
        }
        bar {
            'o': name // ERROR unbound variable
        }
    }
    "#
    .compile_fail();
    // .compile_ok(|test| {
    //     test.assert_domain_map(
    //         ("foo", "bar"),
    //         json!({ "f": "Hello world!"}),
    //         json!({ "b": "world"}),
    //     );
    //     test.assert_domain_map(
    //         ("bar", "foo"),
    //         json!({ "b": "world"}),
    //         json!({ "f": "Hello world!"}),
    //     );
    // });
}

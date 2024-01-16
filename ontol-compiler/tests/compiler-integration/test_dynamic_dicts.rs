use ontol_test_utils::{assert_error_msg, serde_helper::serde_create, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn dynamic_dict() {
    "
    def foo (
        rel .'id'|id: (rel .is: text)
        rel .'dynamic_dict': ()
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_value(json!({
                "id": "foo",
                "dynamic_dict": {
                    "key": "value"
                }
            })),
            r#"invalid type: map, expected unit at line 1 column 16"#
        );
    });
}

#[test]
fn semidynamic_dict() {
    "
    def foo (
        rel .'id'|id: (rel .is: text)
        rel .'dynamic_dict': (
            rel .'key': text
        )
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_value(json!({
                "id": "foo",
                "dynamic_dict": {
                    "key": "value",
                    "foo": "bar"
                }
            })),
            r#"unknown property `foo` at line 1 column 22"#
        );
    });
}

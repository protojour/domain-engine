use crate::{assert_error_msg, assert_json_io_matches, util::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn test_serde_empty_type() {
    "(type! foo)".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({}));
    });
}

#[test]
fn test_serde_value_type() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!("string"));
    });
}

#[test]
fn test_serde_map_type() {
    "
    (type! foo)
    (rel! (foo) a (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "string" }));
    });
}

#[test]
fn test_serde_complex_type() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) a (string))
    (rel! (foo) b (bar))
    (rel! (bar) c (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "A", "b": { "c": "C" }}));
    });
}

#[test]
fn test_serde_sequence() {
    "
    (type! t)
    (rel! (t) 0 (string))
    (rel! (t) 1 (int))
    "
    .compile_ok(|env| {
        let t = TypeBinding::new(env, "t");
        assert_json_io_matches!(t, json!(["a", 1]));
    });
}

#[test]
fn test_serde_value_union1() {
    r#"
    (type! u)
    (rel! (u) _ "a")
    (rel! (u) _ "b")
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(u, json!("a"));
    });
}

#[test]
fn test_serde_string_or_null() {
    r#"
    (type! string-or-null)
    (rel! (string-or-null) _ (string))
    (rel! (string-or-null) _ (null))

    (type! foo)
    (rel! (foo) a (string-or-null))
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "string" }));
        assert_json_io_matches!(foo, json!({ "a": null }));
    });
}

#[test]
fn test_serde_map_union() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) type "foo")
    (rel! (foo) c (int))
    (rel! (bar) type "bar")
    (rel! (bar) d (int))

    (type! u)
    (rel! (u) _ (foo))
    (rel! (u) _ (bar))
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(u, json!({ "type": "foo", "c": 7}));
    });
}

#[test]
fn test_serde_many_cardinality() {
    "
    (type! foo)
    (rel! (foo) s[] (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "s": []}));
        assert_json_io_matches!(foo, json!({ "s": ["a", "b"]}));
    });
}

#[test]
fn test_serde_infinite_sequence() {
    r#"
    (type! foo)
    (rel! (foo) ..2 (int))
    (rel! (foo) 2..4 (string))
    (rel! (foo) 5..6 (int))
    (rel! (foo) 6.. (int))
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!([42, 43, "a", "b", null, 44]));
        assert_json_io_matches!(foo, json!([42, 43, "a", "b", null, 44, 45, 46]));
        assert_error_msg!(
            foo.deserialize_data(json!([77])),
            "invalid length 1, expected sequence with minimum length 6 at line 1 column 4"
        );
    });
}

#[test]
fn test_jsonml() {
    r#"
    (type! element)
    (type! tag)
    (type! tag_name)
    (type! attributes)

    (rel! (element) _ (tag))
    (rel! (element) _ (string))

    (rel! (tag) 0 (tag_name))

    ; BUG: should have default `{}` and serde would skip this if not a map
    ; (also in serialization if this equals the default value!)
    (rel! (tag) 1 (attributes))

    (rel! (tag) 2.. (element))

    (rel! (tag_name) _ "div")
    (rel! (tag_name) _ "em")
    (rel! (tag_name) _ "strong")

    ; BUG: should accept any string as key
    (rel! (attributes) class? (string))
    "#
    .compile_ok(|env| {
        let element = TypeBinding::new(env, "element");

        assert_json_io_matches!(element, json!("text"));
        assert_json_io_matches!(element, json!(["div", {}]));
        assert_json_io_matches!(element, json!(["div", {}, "text"]));
        assert_json_io_matches!(
            element,
            json!(["div", {}, ["em", {}, "text1"], ["strong", { "class": "foo" }]])
        );
    });
}

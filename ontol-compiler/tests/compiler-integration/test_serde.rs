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
fn test_serde_object_property_not_sugared() {
    "
    (type! foo)
    ; this is sugar for `(rel! (foo) a (string))`:
    (rel! (foo) a @(unit) a[] (string))
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
fn test_serde_tuple() {
    "
    (type! t)
    (rel! (t) _ (tuple! (string) (int)))
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

use crate::{util::serialize_json, util::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

macro_rules! assert_json_io_matches {
    ($env:expr, $binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize_value($env, input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = serialize_json($env, &value);

        pretty_assertions::assert_eq!(input, output);
    };
}

#[test]
fn test_serde_empty_type() {
    "(type! foo)".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({}));
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
        assert_json_io_matches!(env, foo, json!("string"));
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
        assert_json_io_matches!(env, foo, json!({ "a": "string" }));
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
        assert_json_io_matches!(env, foo, json!({ "a": "A", "b": { "c": "C" }}));
    });
}

#[test]
fn test_serde_tuple() {
    "
    (type! t)
    (rel! (t) _ (tuple! (string) (number)))
    "
    .compile_ok(|env| {
        let t = TypeBinding::new(env, "t");
        assert_json_io_matches!(env, t, json!(["a", 1]));
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
        assert_json_io_matches!(env, u, json!("a"));
    });
}

#[test]
#[ignore = "need to implement on-demand discriminator per DefId"]
fn test_serde_value_union2() {
    r#"
    (type! u)
    (rel! (u) _ (tuple! "a"))
    (rel! (u) _ "b")
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(env, u, json!(["a"]));
    });
}

#[test]
fn test_serde_map_union() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) type "foo")
    (rel! (foo) c (number))
    (rel! (bar) type "bar")
    (rel! (bar) d (number))

    (type! u)
    (rel! (u) _ (foo))
    (rel! (u) _ (bar))
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(env, u, json!({ "type": "foo", "c": 7}));
    });
}

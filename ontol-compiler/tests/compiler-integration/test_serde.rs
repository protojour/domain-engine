use crate::{util::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

macro_rules! assert_json_io_matches {
    ($env:expr, $binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize($env, input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = $binding.serialize_json($env, &value);

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

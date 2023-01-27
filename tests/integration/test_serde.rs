use crate::{util::TypeBinding, TestCompile};
use serde_json::json;

macro_rules! assert_json_io_matches {
    ($compiler:expr, $binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize($compiler, input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = $binding.serialize_json($compiler, &value);

        pretty_assertions::assert_eq!(input, output);
    };
}

#[test]
fn test_serde_empty_type() {
    "(type! foo)".compile_ok(|mut compiler| {
        let foo = TypeBinding::new(&mut compiler, "foo");
        assert_json_io_matches!(&compiler, foo, json!({}));
    });
}

#[test]
fn test_serde_value_type() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    "
    .compile_ok(|mut compiler| {
        let foo = TypeBinding::new(&mut compiler, "foo");
        assert_json_io_matches!(&compiler, foo, json!("string"));
    });
}

#[test]
fn test_serde_map_type() {
    "
    (type! foo)
    (rel! (foo) a (string))
    "
    .compile_ok(|mut compiler| {
        let foo = TypeBinding::new(&mut compiler, "foo");
        assert_json_io_matches!(&compiler, foo, json!({ "a": "string" }));
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
    .compile_ok(|mut compiler| {
        let foo = TypeBinding::new(&mut compiler, "foo");
        assert_json_io_matches!(&compiler, foo, json!({ "a": "A", "b": { "c": "C" }}));
    });
}

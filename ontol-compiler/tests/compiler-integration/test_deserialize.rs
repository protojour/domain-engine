//! Tests for deserialization, including errors

use assert_matches::assert_matches;
use ontol_runtime::value::Value;
use serde_json::json;
use test_log::test;

use crate::{assert_error_msg, util::TypeBinding, TestCompile};

#[test]
fn deserialize_empty_type() {
    "(type! foo)".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_error_msg!(
            foo.deserialize(env, json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            foo.deserialize(env, json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            foo.deserialize(env, json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_number() {
    "
    (type! foo)
    (rel! (foo) _ (number))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(foo.deserialize(env, json!(42)), Ok(Value::Number(42)));
        assert_matches!(foo.deserialize(env, json!(-42)), Ok(Value::Number(-42)));

        assert_error_msg!(
            foo.deserialize(env, json!({})),
            "invalid type: map, expected number at line 1 column 0"
        );
        assert_error_msg!(
            foo.deserialize(env, json!("boom")),
            "invalid type: string \"boom\", expected number at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_string() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize(env, json!("hei")),
            Ok(Value::String(s)) if s == "hei"
        );

        assert_error_msg!(
            foo.deserialize(env, json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    (type! obj)
    (rel! (obj) a (string))
    (rel! (obj) b (number))
    "
    .compile_ok(|env| {
        let obj = TypeBinding::new(env, "obj");
        assert_matches!(
            obj.deserialize(env, json!({ "a": "hei", "b": 42 })),
            Ok(Value::Compound(_))
        );

        assert_error_msg!(
            obj.deserialize(env, json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            obj.deserialize(env, json!({})),
            "missing properties, expected `a` and `b` at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    (type! one)
    (type! two)
    (type! three)
    (rel! (one) two (two))
    (rel! (one) three (three))
    (rel! (two) three (three))
    (rel! (three) _ (string))
    "
    .compile_ok(|env| {
        let one = TypeBinding::new(env, "one");
        assert_matches!(
            one.deserialize(env, json!({
                "two": {
                    "three": "a"
                },
                "three": "b"
            })),
            Ok(Value::Compound(a)) if a.len() == 2
        );
    });
}

#[test]
fn deserialize_recursive() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) b (bar))
    (rel! (bar) f (foo))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_error_msg!(
            foo.deserialize(
                env,
                json!({
                    "b": {
                        "f": {
                            "b": 42
                        }
                    },
                })
            ),
            "invalid type: integer `42`, expected type `bar` at line 1 column 17"
        );
    });
}

#[test]
fn deserialize_union_of_primitives() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    (rel! (foo) _ (number))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize_variant(env, json!(42)),
            Ok(Value::Number(42))
        );
        assert_matches!(foo.deserialize_variant(env, json!("qux")), Ok(Value::String(s)) if s == "qux");
        assert_error_msg!(
            foo.deserialize(env, json!({})),
            "invalid type: map, expected `number` or `string` at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_string_constant() {
    r#"
    (type! foo)
    (rel! (foo) _ "my_value")
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize(env, json!("my_value")),
            Ok(Value::String(s)) if s == "my_value"
        );
        assert_error_msg!(
            foo.deserialize(env, json!("other value")),
            r#"invalid type: string "other value", expected "my_value" at line 1 column 13"#
        );
        assert_error_msg!(
            foo.deserialize(env, json!(42)),
            r#"invalid type: integer `42`, expected "my_value" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_string_union() {
    r#"
    (type! foo)
    (rel! (foo) _ "a")
    (rel! (foo) _ "b")
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize_variant(env, json!("a")),
            Ok(Value::String(a)) if a == "a"
        );
        assert_error_msg!(
            foo.deserialize_variant(env, json!("junk")),
            r#"invalid type: string "junk", expected `"a"` or `"b"` at line 1 column 6"#
        );
    });
}

#[test_log::test]
fn deserialize_map_union() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) variant "foo")
    (rel! (bar) variant "bar")
    (rel! (bar) prop (number))

    (type! union)
    (rel! (union) _ (foo))
    (rel! (union) _ (bar))
    "#
    .compile_ok(|env| {
        let union = TypeBinding::new(env, "union");
        assert_matches!(
            union.deserialize_variant(env, json!({ "variant": "foo" })),
            Ok(Value::Compound(map)) if map.len() == 1
        );
        assert_matches!(
            union.deserialize_variant(env, json!({ "variant": "bar", "prop": 42 })),
            Ok(Value::Compound(map)) if map.len() == 2
        );
        assert_error_msg!(
            union.deserialize_variant(env, json!("junk")),
            r#"invalid type: string "junk", expected `foo` or `bar` at line 1 column 6"#
        );
        assert_error_msg!(
            union.deserialize_variant(env, json!({ "variant": "bar" })),
            r#"missing properties, expected `prop` at line 1 column 17"#
        );
    });
}

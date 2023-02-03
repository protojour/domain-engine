//! Tests for deserialization, including errors

use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

use crate::{assert_error_msg, util::TypeBinding, TestCompile};

#[test]
fn deserialize_empty_type() {
    "(type! foo)".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_error_msg!(
            foo.deserialize_data(env, json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            foo.deserialize_data(env, json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            foo.deserialize_data(env, json!({})),
            Ok(Data::Map(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_int() {
    "
    (type! foo)
    (rel! (foo) _ (int))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize_data_variant(env, json!(42)),
            Ok(Data::Int(42))
        );
        assert_matches!(
            foo.deserialize_data_variant(env, json!(-42)),
            Ok(Data::Int(-42))
        );

        assert_error_msg!(
            foo.deserialize_data(env, json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            foo.deserialize_data(env, json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
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
            foo.deserialize_data_variant(env, json!("hei")),
            Ok(Data::String(s)) if s == "hei"
        );

        assert_error_msg!(
            foo.deserialize_data(env, json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    (type! obj)
    (rel! (obj) a (string))
    (rel! (obj) b (int))
    "
    .compile_ok(|env| {
        let obj = TypeBinding::new(env, "obj");
        assert_matches!(
            obj.deserialize_data(env, json!({ "a": "hei", "b": 42 })),
            Ok(Data::Map(_))
        );

        assert_error_msg!(
            obj.deserialize_data(env, json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            obj.deserialize_data(env, json!({})),
            r#"missing properties, expected "a" and "b" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    (type! one)
    (type! two)
    (type! three)
    (rel! (one) x (two))
    (rel! (one) y (three))
    (rel! (two) y (three))
    (rel! (three) _ (string))
    "
    .compile_ok(|env| {
        let one = TypeBinding::new(env, "one");
        assert_matches!(
            one.deserialize_data(env, json!({
                "x": {
                    "y": "a"
                },
                "y": "b"
            })),
            Ok(Data::Map(a)) if a.len() == 2
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
            foo.deserialize_data(
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
    (rel! (foo) _ (int))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize_data_variant(env, json!(42)),
            Ok(Data::Int(42))
        );
        assert_matches!(foo.deserialize_data_variant(env, json!("qux")), Ok(Data::String(s)) if s == "qux");
        assert_error_msg!(
            foo.deserialize_data(env, json!({})),
            "invalid type: map, expected `foo` (`int` or `string`) at line 1 column 2"
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
            foo.deserialize_data_variant(env, json!("my_value")),
            Ok(Data::String(s)) if s == "my_value"
        );
        assert_error_msg!(
            foo.deserialize_data(env, json!("other value")),
            r#"invalid type: string "other value", expected "my_value" at line 1 column 13"#
        );
        assert_error_msg!(
            foo.deserialize_data(env, json!(42)),
            r#"invalid type: integer `42`, expected "my_value" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_tuple() {
    r#"
    (type! foo)
    (rel! (foo) _ (tuple! (int) "a"))
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_matches!(
            foo.deserialize_data_variant(env, json!([42, "a"])),
            Ok(Data::Vec(vector)) if vector.len() == 2
        );
        assert_error_msg!(
            foo.deserialize_data_variant(env, json!([77])),
            "invalid length 1, expected tuple with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            foo.deserialize_data_variant(env, json!([11, "a", "boom"])),
            "trailing characters at line 1 column 9"
        );
        assert_error_msg!(
            foo.deserialize_data_variant(env, json!([14, "b"])),
            r#"invalid type: string "b", expected "a" at line 1 column 7"#
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
            foo.deserialize_data_variant(env, json!("a")),
            Ok(Data::String(a)) if a == "a"
        );
        assert_error_msg!(
            foo.deserialize_data_variant(env, json!("junk")),
            r#"invalid type: string "junk", expected `foo` ("a" or "b") at line 1 column 6"#
        );
    });
}

#[test]
fn deserialize_map_union() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) variant "foo")
    (rel! (bar) variant "bar")
    (rel! (bar) prop (int))

    (type! union)
    (rel! (union) _ (foo))
    (rel! (union) _ (bar))
    "#
    .compile_ok(|env| {
        let union = TypeBinding::new(env, "union");
        assert_matches!(
            union.deserialize_data_variant(env, json!({ "variant": "foo" })),
            Ok(Data::Map(map)) if map.len() == 1
        );
        assert_matches!(
            union.deserialize_data_variant(env, json!({ "variant": "bar", "prop": 42 })),
            Ok(Data::Map(map)) if map.len() == 2
        );
        assert_matches!(
            union.deserialize_data_variant(env, json!({ "prop": 42, "variant": "bar" })),
            Ok(Data::Map(map)) if map.len() == 2
        );
        assert_error_msg!(
            union.deserialize_data_variant(env, json!("junk")),
            r#"invalid type: string "junk", expected `union` (`foo` or `bar`) at line 1 column 6"#
        );
        assert_error_msg!(
            union.deserialize_data_variant(env, json!({ "variant": "bar" })),
            r#"missing properties, expected "prop" at line 1 column 17"#
        );
    });
}

#[test]
#[ignore = "must implement"]
fn deserialize_monads() {
    r#"
    (type! foo)
    (rel! (foo) a (string))
    (default! (foo) a "default")

    (type! bar)
    ; a is either a string or not present
    (rel! (bar) maybe? (string))

    ; bar and string may be related via b many times
    (rel! (bar) array[] (string))

    ; bar and string may be related via c many times, minimum 1
    (rel! (bar) maybe-array[1..]? (string))

    ; a is either a string or null
    (rel! (bar) nullable (string))
    (rel! (bar) nullable (null))
    "#
    .compile_ok(|env| {})
}

//! Tests for deserialization, including errors

use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use ontol_test_utils::{assert_error_msg, serde_utils::*, type_binding::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn deserialize_empty_type() {
    "pub type foo".compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_error_msg!(
            create_de(&foo).data(json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            create_de(&foo).data(json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            create_de(&foo).data(json!({})),
            Ok(Data::Struct(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_int() {
    "
    pub type foo
    rel foo is?: int
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(create_de(&foo).data_variant(json!(42)), Ok(Data::Int(42)));
        assert_matches!(create_de(&foo).data_variant(json!(-42)), Ok(Data::Int(-42)));

        assert_error_msg!(
            create_de(&foo).data(json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            create_de(&foo).data(json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_string() {
    "
    pub type foo
    rel foo is?: string
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data_variant(json!("hei")),
            Ok(Data::String(s)) if s == "hei"
        );

        assert_error_msg!(
            create_de(&foo).data(json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    pub type obj
    rel obj 'a': string
    rel obj 'b': int
    "
    .compile_ok(|test| {
        let obj = TypeBinding::new(&test, "obj");
        assert_matches!(
            create_de(&obj).data(json!({ "a": "hei", "b": 42 })),
            Ok(Data::Struct(_))
        );

        assert_error_msg!(
            create_de(&obj).data(json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            create_de(&obj).data(json!({ "a": "hei", "b": 42, "_edge": { "param": 42 } })),
            "`_edge` property not accepted here at line 1 column 8"
        );
        assert_error_msg!(
            create_de(&obj).data(json!({})),
            r#"missing properties, expected "a" and "b" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_read_only_property_error() {
    "
    pub type obj
    rel obj 'created'(rel .gen: create_time): datetime
    "
    .compile_ok(|test| {
        let obj = TypeBinding::new(&test, "obj");
        assert_error_msg!(
            create_de(&obj).data(json!({ "created": "something" })),
            "property `created` is read-only at line 1 column 10"
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    pub type one
    pub type two
    type three
    rel one 'x': two
    rel one 'y': three
    rel two 'y': three
    rel three is?: string
    "
    .compile_ok(|test| {
        let one = TypeBinding::new(&test, "one");
        assert_matches!(
            create_de(&one).data(json!({
                "x": {
                    "y": "a"
                },
                "y": "b"
            })),
            Ok(Data::Struct(a)) if a.len() == 2
        );
    });
}

#[test]
fn deserialize_recursive() {
    "
    pub type foo
    pub type bar
    rel foo 'b': bar
    rel bar 'f': foo
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_error_msg!(
            create_de(&foo).data(json!({
                "b": {
                    "f": {
                        "b": 42
                    }
                },
            })),
            "invalid type: integer `42`, expected type `bar` at line 1 column 17"
        );
    });
}

#[test]
fn deserialize_union_of_primitives() {
    "
    pub type foo
    rel foo is?: string
    rel foo is?: int
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data_variant(json!(42)),
            Ok(Data::Int(42))
        );
        assert_matches!(create_de(&foo).data_variant(json!("qux")), Ok(Data::String(s)) if s == "qux");
        assert_error_msg!(
            create_de(&foo).data(json!({})),
            "invalid type: map, expected `foo` (`int` or `string`) at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_string_constant() {
    "
    pub type foo
    rel foo is?: 'my_value'
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data_variant(json!("my_value")),
            Ok(Data::String(s)) if s == "my_value"
        );
        assert_error_msg!(
            create_de(&foo).data(json!("other value")),
            r#"invalid type: string "other value", expected "my_value" at line 1 column 13"#
        );
        assert_error_msg!(
            create_de(&foo).data(json!(42)),
            r#"invalid type: integer `42`, expected "my_value" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_finite_non_uniform_sequence() {
    "
    pub type foo
    rel foo 0: int
    rel foo 1: 'a'
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data(json!([42, "a"])),
            Ok(Data::Sequence(vec)) if vec.len() == 2
        );
        assert_error_msg!(
            create_de(&foo).data(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            create_de(&foo).data(json!([11, "a", "boom"])),
            "trailing characters at line 1 column 9"
        );
        assert_error_msg!(
            create_de(&foo).data(json!([14, "b"])),
            r#"invalid type: string "b", expected "a" at line 1 column 7"#
        );
    });
}

#[test]
fn deserialize_finite_uniform_sequence() {
    "
    pub type foo
    rel foo ..2: int
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data(json!([42, 42])),
            Ok(Data::Sequence(vector)) if vector.len() == 2
        );
        assert_error_msg!(
            create_de(&foo).data(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            create_de(&foo).data(json!([11, "a"])),
            r#"invalid type: string "a", expected integer at line 1 column 7"#
        );
        assert_error_msg!(
            create_de(&foo).data(json!([14, 15, 16])),
            r#"trailing characters at line 1 column 8"#
        );
    });
}

#[test]
fn deserialize_string_union() {
    "
    pub type foo
    rel foo is?: 'a'
    rel foo is?: 'b'
    "
    .compile_ok(|test| {
        let foo = TypeBinding::new(&test, "foo");
        assert_matches!(
            create_de(&foo).data_variant(json!("a")),
            Ok(Data::String(a)) if a == "a"
        );
        assert_error_msg!(
            create_de(&foo).data_variant(json!("junk")),
            r#"invalid type: string "junk", expected `foo` ("a" or "b") at line 1 column 6"#
        );
    });
}

#[test]
fn deserialize_map_union() {
    "
    pub type foo
    pub type bar
    rel foo 'variant': 'foo'
    rel bar 'variant': 'bar'
    rel bar 'prop': int

    pub type union
    rel union is?: foo
    rel union is?: bar
    "
    .compile_ok(|test| {
        let union = TypeBinding::new(&test, "union");
        assert_matches!(
            create_de(&union).data_variant(json!({ "variant": "foo" })),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_matches!(
            create_de(&union).data_variant(json!({ "variant": "bar", "prop": 42 })),
            Ok(Data::Struct(attrs)) if attrs.len() == 2
        );
        assert_matches!(
            create_de(&union).data_variant(json!({ "prop": 42, "variant": "bar" })),
            Ok(Data::Struct(attrs)) if attrs.len() == 2
        );
        assert_error_msg!(
            create_de(&union).data_variant(json!("junk")),
            r#"invalid type: string "junk", expected `union` (`foo` or `bar`) at line 1 column 6"#
        );
        assert_error_msg!(
            create_de(&union).data_variant(json!({ "variant": "bar" })),
            r#"missing properties, expected "prop" at line 1 column 17"#
        );
    });
}

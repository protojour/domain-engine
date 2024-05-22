//! Tests for deserialization, including errors

use assert_matches::assert_matches;
use ontol_macros::test;
use ontol_runtime::value::Value;
use ontol_test_utils::{assert_error_msg, serde_helper::*, TestCompile};
use serde_json::json;

#[test]
fn deserialize_empty_type() {
    "def foo ()".compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_value(json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            serde_create(&foo).to_value(json!({})),
            Ok(Value::Struct(attrs, _)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_is_i64() {
    "
    def foo ()
    rel foo is: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!(42)),
            Ok(Value::I64(42, _))
        );
        assert_matches!(
            serde_create(&foo).to_value_variant(json!(-42)),
            Ok(Value::I64(-42, _))
        );

        assert_error_msg!(
            serde_create(&foo).to_value(json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_is_maybe_i64() {
    "
    def foo ()
    rel foo is?: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!(42)),
            Ok(Value::I64(42, _))
        );
        assert_matches!(
            serde_create(&foo).to_value_variant(json!(-42)),
            Ok(Value::I64(-42, _))
        );

        assert_error_msg!(
            serde_create(&foo).to_value(json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_string() {
    "
    def foo ()
    rel foo is?: text
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!("hei")),
            Ok(Value::Text(s, _)) if s == "hei"
        );

        assert_error_msg!(
            serde_create(&foo).to_value(json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    def obj ()
    rel obj 'a': text
    rel obj 'b': i64
    "
    .compile_then(|test| {
        let [obj] = test.bind(["obj"]);
        assert_matches!(
            serde_create(&obj).to_value(json!({ "a": "hei", "b": 42 })),
            Ok(Value::Struct(..))
        );

        assert_error_msg!(
            serde_create(&obj).to_value(json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            serde_create(&obj).to_value(json!({ "a": "hei", "b": 42, "_edge": { "param": 42 } })),
            "`_edge` property not accepted here at line 1 column 8"
        );
        assert_error_msg!(
            serde_create(&obj).to_value(json!({})),
            r#"missing properties, expected "a" and "b" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_read_only_property_error() {
    "
    def obj ()
    rel obj 'created'[rel .gen: create_time]: datetime
    "
    .compile_then(|test| {
        let [obj] = test.bind(["obj"]);
        assert_error_msg!(
            serde_create(&obj).to_value(json!({ "created": "something" })),
            "property `created` is read-only at line 1 column 10"
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    def one ()
    def two ()
    def three ()
    rel one 'x': two
    rel one 'y': three
    rel two 'y': three
    rel three is?: text
    "
    .compile_then(|test| {
        let [one] = test.bind(["one"]);
        assert_matches!(
            serde_create(&one).to_value(json!({
                "x": {
                    "y": "a"
                },
                "y": "b"
            })),
            Ok(Value::Struct(a, _)) if a.len() == 2
        );
    });
}

#[test]
fn deserialize_recursive() {
    "
    def foo ()
    def bar ()
    rel foo 'b': bar
    rel bar 'f': foo
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_value(json!({
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
    def foo ()
    rel foo is?: text
    rel foo is?: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!(42)),
            Ok(Value::I64(42, _))
        );
        assert_matches!(serde_create(&foo).to_value_variant(json!("qux")), Ok(Value::Text(s, _)) if s == "qux");
        assert_error_msg!(
            serde_create(&foo).to_value(json!({})),
            "invalid type: map, expected `foo` (`int` or `string`) at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_string_constant() {
    "
    def foo ()
    rel foo is?: 'my_value'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!("my_value")),
            Ok(Value::Text(s, _)) if s == "my_value"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!("other value")),
            r#"invalid type: string "other value", expected "my_value" at line 1 column 13"#
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!(42)),
            r#"invalid type: integer `42`, expected "my_value" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_finite_non_uniform_sequence() {
    "
    def foo ()
    rel foo 0: i64
    rel foo 1: 'a'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value(json!([42, "a"])),
            Ok(Value::Sequence(seq, _)) if seq.attrs().len() == 2
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([11, "a", "boom"])),
            "trailing characters at line 1 column 9"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([14, "b"])),
            r#"invalid type: string "b", expected "a" at line 1 column 7"#
        );
    });
}

#[test]
fn deserialize_finite_uniform_sequence() {
    "
    def foo ()
    rel foo ..2: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value(json!([42, 42])),
            Ok(Value::Sequence(seq, _)) if seq.attrs().len() == 2
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([11, "a"])),
            r#"invalid type: string "a", expected integer at line 1 column 7"#
        );
        assert_error_msg!(
            serde_create(&foo).to_value(json!([14, 15, 16])),
            r#"trailing characters at line 1 column 8"#
        );
    });
}

#[test]
fn deserialize_string_union() {
    "
    def foo ()
    rel foo is?: 'a'
    rel foo is?: 'b'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_value_variant(json!("a")),
            Ok(Value::Text(a, _)) if a == "a"
        );
        assert_error_msg!(
            serde_create(&foo).to_value_variant(json!("junk")),
            r#"invalid type: string "junk", expected `foo` ("a" or "b") at line 1 column 6"#
        );
    });
}

#[test]
fn deserialize_map_union() {
    "
    def foo ()
    def bar ()
    rel foo 'variant': 'foo'
    rel bar 'variant': 'bar'
    rel bar 'prop': i64

    def union (
        rel .is?: foo
        rel .is?: bar
    )
    "
    .compile_then(|test| {
        let [union] = test.bind(["union"]);
        assert_matches!(
            serde_create(&union).to_value_variant(json!({ "variant": "foo" })),
            Ok(Value::Struct(attrs, _)) if attrs.len() == 1
        );
        assert_matches!(
            serde_create(&union).to_value_variant(json!({ "variant": "bar", "prop": 42 })),
            Ok(Value::Struct(attrs, _)) if attrs.len() == 2
        );
        assert_matches!(
            serde_create(&union).to_value_variant(json!({ "prop": 42, "variant": "bar" })),
            Ok(Value::Struct(attrs, _)) if attrs.len() == 2
        );
        assert_error_msg!(
            serde_create(&union).to_value_variant(json!("junk")),
            r#"invalid type: string "junk", expected `union` (`foo` or `bar`) at line 1 column 6"#
        );
        assert_error_msg!(
            serde_create(&union).to_value_variant(json!({ "variant": "bar" })),
            r#"missing properties, expected "prop" at line 1 column 17"#
        );
    });
}

#[test]
fn union_tree() {
    "
    def u1 (
        rel .is?: '1a'
        rel .is?: '1b'
    )
    def u2 (
        rel .is?: '2a'
        rel .is?: '2b'
    )
    def u3 (
        rel .is?: u1
        rel .is?: u2
    )
    "
    .compile_then(|test| {
        let [u3] = test.bind(["u3"]);
        assert_matches!(
            serde_create(&u3).to_value_variant(json!("1a")),
            Ok(Value::Text(s, _)) if s == "1a"
        );
        assert_error_msg!(
            serde_create(&u3).to_value_variant(json!("ugh")),
            r#"invalid type: string "ugh", expected `u3` (one of "1a", "1b", "2a", "2b") at line 1 column 5"#
        );
    });
}

#[test]
fn test_deserialize_open_data() {
    "
    def @open foo (
        rel .'closed': i64
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);

        let Value::Struct(map, _) = serde_create(&foo)
            .enable_open_data()
            .to_value(json!({
                "closed": 1,
                "int": 2,
                "float": 6.66,
                "bool": true,
                "null": null,
                "dict": {
                    "key": "value"
                },
                "array": ["value"]
            }))
            .unwrap()
        else {
            panic!();
        };

        let open_data = &map
            .get(&test.ontology().ontol_domain_meta().open_data_property_id())
            .unwrap()
            .val;
        let Value::Dict(open_data, _) = &open_data else {
            panic!();
        };

        assert!(!open_data.contains_key("closed"));
        assert!(open_data.contains_key("int"));
        assert!(open_data.contains_key("float"));
        assert!(open_data.contains_key("bool"));
        assert!(open_data.contains_key("null"));
        assert!(open_data.contains_key("dict"));
        assert!(open_data.contains_key("array"));
    });
}
